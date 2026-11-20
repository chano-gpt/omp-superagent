use serde_json::{Value, json};

use crate::adapter::EffectError;

use super::super::Inner;
use super::super::media::{message_payload, plain};
use super::super::wire::{same_optional, short_hash, slack_id};
use super::{Closure, Dialog, Modal, Pending};

impl Inner {
    pub(super) async fn assert_card<'message>(
        &self,
        message: &'message Value,
        pending: &Pending,
    ) -> Result<&'message [Value], EffectError> {
        let identity = self.identity().await?;
        let mismatch =
            || EffectError::rejected("Slack message is not the matching owned approval card");
        let Some(ts) = pending.message.as_ref() else {
            return Err(mismatch());
        };
        let thread = match &pending.approval.route.thread_id {
            Some(root) => message["thread_ts"] == root.as_str(),
            None => same_optional(message, "thread_ts", ts.as_str()),
        };
        let blocks = message["blocks"].as_array().ok_or_else(mismatch)?;
        if message["ts"] != ts.as_str()
            || message["user"] != identity.bot_user
            || message["bot_id"] != identity.bot_id
            || !same_optional(message, "team", &self.team)
            || !thread
            || !same_optional(message, "channel", &pending.approval.route.channel_id)
            || !blocks
                .iter()
                .any(|block| block["block_id"] == pending.control("card"))
        {
            return Err(mismatch());
        }
        let prefix = pending.control("");
        for block in blocks {
            if block["block_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("omp:") && !id.starts_with(&prefix))
                || block["elements"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|element| {
                        element["action_id"]
                            .as_str()
                            .is_some_and(|id| !id.starts_with(&prefix))
                    })
            {
                return Err(EffectError::rejected(
                    "Slack card correlation does not match the approval generation",
                ));
            }
        }
        Ok(blocks)
    }

    pub(super) async fn update_card(
        &self,
        pending: &Pending,
        closure: Option<Closure>,
    ) -> Result<bool, EffectError> {
        let Some(ts) = pending.message.as_ref() else {
            return Ok(false);
        };
        let route = &pending.approval.route;
        self.destination(route).await?;
        let message = self.history_message(route, ts).await?;
        let blocks = self.assert_card(&message, pending).await?;
        let status = [Closure::Closed, Closure::Submitted, Closure::Cancelled]
            .into_iter()
            .find(|closure| {
                blocks
                    .iter()
                    .any(|block| block["block_id"] == pending.control(closure.control()))
            });
        let has_controls = blocks.iter().any(|block| block["type"] == "actions");
        match status {
            Some(Closure::Submitted | Closure::Cancelled) => return Ok(false),
            Some(Closure::Closed) if matches!(closure, None | Some(Closure::Closed)) => {
                return Ok(false);
            }
            None if !has_controls => return Ok(false),
            Some(Closure::Closed) | None => {}
        }
        let card = pending.view(closure)?;
        let mut args = message_payload(&card.text, card.blocks)?;
        args["channel"] = json!(route.channel_id);
        args["ts"] = json!(ts);
        args["as_user"] = json!(true);
        let receipt: Value = match self.api.call("chat.update", args).await {
            Ok(receipt) => receipt,
            Err(error)
                if closure.is_some()
                    && (error.is_code("message_not_found")
                        || error.is_code("channel_not_found")) =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        };
        if receipt["channel"] != route.channel_id || receipt["ts"] != ts.as_str() {
            return Err(EffectError::uncertain(
                "Slack card update has no authoritative destination receipt",
            ));
        }
        Ok(true)
    }

    pub(super) async fn close_card(
        &self,
        pending: &mut Pending,
        closure: Closure,
    ) -> Result<(), EffectError> {
        pending.active = false;
        pending.modal = None;
        pending.custom_input = None;
        pending.answers.clear();
        pending.selected.clear();
        match self.update_card(pending, Some(closure)).await {
            Ok(_) => Ok(()),
            Err(error)
                if error.certainty == "rejected"
                    && matches!(
                        error.message.as_str(),
                        "Slack API rejected request: message_not_found"
                            | "Slack API rejected request: channel_not_found"
                    ) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn open_modal(
        &self,
        pending: &mut Pending,
        action: &str,
        trigger: &Value,
    ) -> Result<(), EffectError> {
        let trigger = trigger
            .as_str()
            .filter(|trigger| !trigger.is_empty())
            .ok_or_else(|| EffectError::rejected("Slack modal has no valid trigger"))?;
        if pending.modal.is_some() {
            return Ok(());
        }
        let metadata = short_hash(&json!([
            pending.token,
            pending.message,
            pending.approval.route.channel_id,
            action
        ]));
        let (title, multiline, prefill, placeholder) = match &pending.dialog {
            Dialog::Ask { .. } => ("Other answer", true, pending.custom_input.as_deref(), None),
            Dialog::Editor(editor) => (
                "OMP response",
                true,
                editor.prefill.as_deref(),
                editor.placeholder.as_deref(),
            ),
            Dialog::Input(editor) => (
                "OMP response",
                false,
                editor.prefill.as_deref(),
                editor.placeholder.as_deref(),
            ),
            Dialog::Confirm { .. } | Dialog::Select(_) => {
                return Err(EffectError::rejected("Slack dialog cannot open a modal"));
            }
        };
        let mut element = json!({
            "type": "plain_text_input", "action_id": "value", "multiline": multiline, "max_length": 3000,
        });
        if let Some(prefill) = prefill {
            element["initial_value"] = json!(prefill);
        }
        if let Some(placeholder) = placeholder.filter(|text| !text.is_empty()) {
            element["placeholder"] = plain(placeholder);
        }
        let receipt: Value = self.api.call("views.open", json!({
            "trigger_id": trigger,
            "view": {
                "type": "modal", "callback_id": pending.control(action), "private_metadata": metadata,
                "notify_on_close": true, "title": plain(title), "submit": plain("Submit"), "close": plain("Cancel"),
                "blocks": [{
                    "type": "input", "block_id": "response", "optional": true,
                    "label": plain("Response (not secret)"), "element": element,
                }],
            },
        })).await?;
        let id = receipt["view"]["id"]
            .as_str()
            .filter(|id| slack_id(id))
            .ok_or_else(|| {
                EffectError::uncertain("Slack modal has no authoritative view receipt")
            })?;
        pending.modal = Some(Modal {
            id: id.to_owned(),
            metadata,
            action: action.to_owned(),
        });
        Ok(())
    }
}
