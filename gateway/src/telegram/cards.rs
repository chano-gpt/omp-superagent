use serde_json::json;
use uuid::Uuid;

use crate::adapter::EffectError;

use super::{
    Core,
    approval::{Approval, Pending, Progress, card_key},
    dialog::Dialog,
    effects::message_id,
    model::Identity,
    view::{Keyboard, Renderer, UNSUPPORTED, View},
};

impl Core {
    pub(super) async fn show_approval(
        &self,
        me: &Identity,
        approval: Approval,
    ) -> Result<String, EffectError> {
        self.policy.authorize(me, &approval.route)?;
        if approval.state != "pending" {
            return Err(EffectError::rejected(
                "Telegram approval is not pending on an active messenger",
            ));
        }
        let token = approval.token();
        // One transaction at a time per adapter: publication, dismissal and
        // callback handling must not race a remote card write or its receipt.
        let mut cards = self.cards.lock().await;
        if let Some(card) = cards.get(&token).filter(|card| card.progress.active) {
            if card.approval.request != approval.request {
                return Err(EffectError::rejected("Telegram approval request changed"));
            }
            check_receipt(&approval, &card.progress)?;
            if let Some(receipt) = &card.progress.message_id {
                return Ok(receipt.clone());
            }
        }

        let bot = &approval.route.account_id;
        let mut progress = self.progress(bot, &token)?.unwrap_or_default();
        check_receipt(&approval, &progress)?;
        if progress.presenting {
            return Err(EffectError::uncertain(
                "Telegram approval presentation has an unresolved outcome",
            ));
        }
        if progress.notice && !progress.active {
            if let Some(receipt) = progress.message_id.clone() {
                if progress.answer_request.as_deref() != Some(&approval.request_hash()) {
                    return Err(EffectError::uncertain(
                        "Telegram approval notice request changed",
                    ));
                }
                if !progress.answer_admitted {
                    self.complete_notice(&approval, &token, &mut progress)?;
                } else if !self.has_pending_answer(&approval.id, &approval.route)? {
                    return Err(EffectError::uncertain(
                        "Telegram approval notice no longer has a queued answer",
                    ));
                }
                return Ok(receipt);
            }
        }
        if !progress.active && (progress.attempted || progress.message_id.is_some()) {
            if let Some(receipt) = &progress.message_id
                && progress.answer_request.as_deref() == Some(&approval.request_hash())
                && self.has_pending_answer(&approval.id, &approval.route)?
            {
                // Existing Bun receipts predate the response digest. New
                // attempts must match the original answer, not just its ID.
                match &progress.binding {
                    None => return Ok(receipt.clone()),
                    Some(binding)
                        if binding.approval_id == approval.id
                            && binding.route == approval.route
                            && self.has_bound_answer(binding)? =>
                    {
                        return Ok(receipt.clone());
                    }
                    Some(_) => {}
                }
            }
            return Err(EffectError::uncertain(
                "Telegram approval has already been answered or has an unresolved outcome",
            ));
        }
        progress.message_id = approval.remote_id.clone().or(progress.message_id);
        let id = progress.message_id.as_deref().map(message_id).transpose()?;
        if let Some(receipt) = &progress.message_id
            && self.get(&card_key(&approval.route, receipt))?.as_deref() == Some("closed")
        {
            return Err(EffectError::uncertain(
                "Telegram approval receipt is closed",
            ));
        }
        if progress.attempted && id.is_none() {
            return Err(EffectError::uncertain(
                "Telegram approval send outcome is unknown; refusing to resend",
            ));
        }

        let stamp = Uuid::new_v4().simple().to_string()[..12].to_owned();
        let renderer = Renderer {
            token: &token,
            stamp: &stamp,
            owner_context: approval.owner_context.as_deref(),
        };
        let supported = Dialog::parse(&approval.request)
            .filter(|dialog| approval.decision.is_none() || matches!(dialog, Dialog::Ask { .. }));
        let rendered = match supported {
            Some(dialog) if !progress.valid_for(&dialog) => {
                return Err(EffectError::uncertain(
                    "Telegram approval progress does not match the original dialog",
                ));
            }
            Some(dialog) if renderer.future_questions_fit(&dialog, &progress) => {
                renderer.draw(&dialog, &progress).map(|view| (dialog, view))
            }
            Some(_) | None => None,
        };
        let Some((dialog, view)) = rendered else {
            return self.show_notice(me, &approval, &token, progress).await;
        };
        self.destination(me, &approval.route).await?;
        let mut pending = Pending {
            approval,
            token,
            dialog,
            stamp,
            progress,
        };
        pending.progress.attempted = true;
        pending.progress.presenting = true;
        pending.save(self)?;

        let message = match self.write_card(&pending.approval.route, id, &view).await {
            Ok(message) => message,
            Err(error) => {
                pending.progress.presenting = false;
                if id.is_none() && error.is_rejected() {
                    pending.progress.attempted = false;
                } else {
                    pending.progress.close();
                }
                pending.save(self)?;
                return Err(error.into());
            }
        };
        let receipt = self.receipt(me, &pending.approval.route, &message)?;
        if !pending.matches_presentation(&message)
            || pending
                .progress
                .message_id
                .as_ref()
                .is_some_and(|id| id != &receipt)
        {
            return Err(EffectError::uncertain(
                "Telegram card response has unexpected controls or receipt",
            ));
        }
        self.set(&card_key(&pending.approval.route, &receipt), "card")?;
        pending.progress.message_id = Some(receipt.clone());
        pending.progress.presenting = false;
        pending.save(self)?;
        cards.insert(pending.token.clone(), pending);
        Ok(receipt)
    }

    async fn show_notice(
        &self,
        me: &Identity,
        approval: &Approval,
        token: &str,
        mut progress: Progress,
    ) -> Result<String, EffectError> {
        self.destination(me, &approval.route).await?;
        let id = progress.message_id.as_deref().map(message_id).transpose()?;
        progress.attempted = true;
        progress.presenting = true;
        progress.notice = true;
        progress.answer_request = Some(approval.request_hash());
        self.save_progress(&approval.route.account_id, token, &progress)?;
        let view = View {
            text: UNSUPPORTED.to_owned(),
            keyboard: Keyboard::default(),
        };
        let message = match self.write_card(&approval.route, id, &view).await {
            Ok(message) => message,
            Err(error) => {
                if id.is_none() && error.is_rejected() {
                    let reset = Progress::default();
                    self.save_progress(&approval.route.account_id, token, &reset)?;
                }
                return Err(error.into());
            }
        };
        let receipt = self.receipt(me, &approval.route, &message)?;
        if message.text.as_deref() != Some(UNSUPPORTED)
            || progress
                .message_id
                .as_ref()
                .is_some_and(|id| id != &receipt)
            || message
                .reply_markup
                .as_ref()
                .is_some_and(|markup| !markup.inline_keyboard.is_empty())
        {
            return Err(EffectError::uncertain(
                "Telegram returned an unexpected unsupported-dialog notice",
            ));
        }
        self.set(&card_key(&approval.route, &receipt), "closed")?;
        progress.close();
        progress.message_id = Some(receipt.clone());
        progress.presenting = false;
        self.save_progress(&approval.route.account_id, token, &progress)?;
        self.complete_notice(approval, token, &mut progress)?;
        Ok(receipt)
    }

    fn complete_notice(
        &self,
        approval: &Approval,
        token: &str,
        progress: &mut Progress,
    ) -> Result<(), EffectError> {
        self.ctx
            .cancel_unsupported_approval(&approval.id, &json!(approval.route))
            .map_err(|_| super::ledger_error())?;
        progress.answer_admitted = true;
        self.save_progress(&approval.route.account_id, token, progress)
    }

    pub(super) async fn dismiss_approval(
        &self,
        me: &Identity,
        approval: Approval,
    ) -> Result<(), EffectError> {
        self.policy.authorize(me, &approval.route)?;
        let token = approval.token();
        let bot = &approval.route.account_id;
        let mut cards = self.cards.lock().await;
        let mut progress = self.progress(bot, &token)?;
        if let Some(progress) = &progress {
            check_receipt(&approval, progress)?;
        }
        let receipt = approval
            .remote_id
            .or_else(|| progress.as_ref().and_then(|state| state.message_id.clone()));
        if let Some(receipt) = &receipt {
            message_id(receipt)?;
        }
        if let Some(progress) = &mut progress {
            progress.close();
            self.save_progress(bot, &token, progress)?;
        } else {
            self.save_progress(
                bot,
                &token,
                &Progress {
                    message_id: receipt.clone(),
                    attempted: true,
                    active: false,
                    ..Progress::default()
                },
            )?;
        }
        if let Some(receipt) = &receipt {
            self.set(&card_key(&approval.route, receipt), "closed")?;
        }
        cards.remove(&token);
        if let Some(receipt) = receipt {
            self.clear_controls(&approval.route, &receipt).await?;
        }
        Ok(())
    }
}

fn check_receipt(approval: &Approval, progress: &Progress) -> Result<(), EffectError> {
    if approval
        .remote_id
        .as_ref()
        .zip(progress.message_id.as_ref())
        .is_some_and(|(new, saved)| new != saved)
    {
        return Err(EffectError::rejected("Telegram approval receipt conflict"));
    }
    Ok(())
}
