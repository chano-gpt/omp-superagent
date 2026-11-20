use serde_json::{Value, json};

use crate::{adapter::EffectError, store::answer_digest};

use super::{
    Core,
    approval::{AnswerBinding, Pending, card_key},
    dialog::{Answer, Dialog},
    model::Route,
};

pub(super) struct AnswerEvent<'a> {
    pub id: &'a str,
    pub callback: Option<&'a str>,
}

pub(super) struct ClosedAnswer<'a> {
    pub token: &'a str,
    pub route: &'a Route,
    pub receipt: &'a str,
}

pub(super) enum AnswerResponse {
    Cancel,
    Confirm(bool),
    Text(String),
    Ask(Vec<Answer>),
}

impl AnswerResponse {
    fn wire(self, pending: &Pending) -> Result<Value, EffectError> {
        let invalid =
            || EffectError::rejected("Decision response does not match its original questions");
        if pending.approval.decision.is_some() {
            match (&self, &pending.dialog) {
                (Self::Cancel, Dialog::Ask { .. }) => {}
                (Self::Ask(answers), Dialog::Ask { questions })
                    if answers.len() == questions.len()
                        && answers
                            .iter()
                            .zip(questions)
                            .all(|(answer, question)| answer.matches(question)) => {}
                _ => return Err(invalid()),
            }
        }
        Ok(match self {
            Self::Cancel => json!({"cancelled":true}),
            Self::Confirm(confirmed) => json!({"confirmed":confirmed}),
            Self::Text(value) => json!({"value":value}),
            Self::Ask(answers) => json!({"answers":answers}),
        })
    }
}

pub(super) fn answer_key(route: &Route, receipt: &str) -> String {
    format!("{}:answer", card_key(route, receipt))
}

impl Core {
    pub(super) async fn finish_answer(
        &self,
        pending: &mut Pending,
        event: AnswerEvent<'_>,
        response: AnswerResponse,
    ) -> Result<(), EffectError> {
        let payload = response.wire(pending)?;
        let receipt = pending.progress.message_id.clone().ok_or_else(|| {
            EffectError::uncertain("Telegram approval has no verified card receipt")
        })?;
        let mut progress = pending.progress.clone();
        progress.close();
        progress.last_update = Some(event.id.to_owned());
        progress.answer_request = Some(pending.approval.request_hash());
        progress.binding = Some(AnswerBinding {
            approval_id: pending.approval.id.clone(),
            route: pending.approval.route.clone(),
            response_hash: answer_digest(&payload).map_err(|_| super::ledger_error())?,
        });
        let route = &pending.approval.route;
        self.set(&answer_key(route, &receipt), &pending.token)?;
        self.save_progress(&route.account_id, &pending.token, &progress)?;
        pending.progress = progress;
        self.set(&card_key(route, &receipt), "closed")?;

        let answer = json!({
            "kind":"answer","approvalId":pending.approval.id,"response":payload,"route":route
        });
        let result = async {
            self.admit(answer)?;
            pending.progress.answer_admitted = true;
            pending.progress.ack_attempted = event.callback.is_some();
            pending.save(self)?;
            if let Some(callback) = event.callback {
                self.ack_callback(callback, None).await;
            }
            Ok(())
        }
        .await;
        let dismissed = self.clear_controls(&pending.approval.route, &receipt).await;
        result.and(dismissed)
    }

    pub(super) async fn recover_answer(
        &self,
        card: ClosedAnswer<'_>,
        event: AnswerEvent<'_>,
    ) -> Result<bool, EffectError> {
        let bot = &card.route.account_id;
        let Some(mut progress) = self.progress(bot, card.token)? else {
            return Ok(false);
        };
        let Some(binding) = &progress.binding else {
            return Ok(false);
        };
        if progress.active
            || progress.last_update.as_deref() != Some(event.id)
            || progress.message_id.as_deref() != Some(card.receipt)
            || !binding.route.same_physical(card.route)
        {
            return Ok(false);
        }
        // An exact committed event remains proof after the head has taken
        // and acknowledged it. A tombstone or pending-only lookup cannot
        // distinguish that crash window from a failed admission.
        if !self.has_bound_answer(binding)? {
            return Err(EffectError::uncertain(
                "Telegram final answer admission remains unresolved",
            ));
        }
        let callback = event.callback.filter(|_| !progress.ack_attempted);
        if !progress.answer_admitted || callback.is_some() {
            progress.answer_admitted = true;
            progress.ack_attempted |= callback.is_some();
            self.save_progress(bot, card.token, &progress)?;
        }
        if let Some(callback) = callback {
            self.ack_callback(callback, None).await;
        }
        Ok(true)
    }

    pub(super) async fn ack_callback(&self, id: &str, reason: Option<&str>) {
        let mut request = json!({"callback_query_id":id});
        if let Some(reason) = reason {
            request["text"] = json!(reason);
        }
        if self
            .api
            .call::<bool>("answerCallbackQuery", &request)
            .await
            .is_err()
        {
            eprintln!("Telegram callback feedback did not complete");
        }
    }
}
