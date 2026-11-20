use crate::adapter::EffectError;

use super::{
    Core,
    answer_commit::{AnswerEvent, AnswerResponse, ClosedAnswer, answer_key},
    approval::{Pending, Progress, card_key},
    dialog::{Answer, Dialog},
    effects::message_id,
    ingress::{Callback, Inbound, Message},
    model::Identity,
};

const EXPIRED: &str = "This request is expired, unauthorized, or already being handled.";
const NO_CHOICE: &str = "Choose an option or enter an answer before continuing.";

enum Action {
    Cancel,
    Confirm(bool),
    Pick(usize),
    Choose(usize, usize),
    Next(usize),
}

impl Action {
    fn parse(input: &str) -> Option<Self> {
        match input {
            "cancel" => Some(Self::Cancel),
            "yes" => Some(Self::Confirm(true)),
            "no" => Some(Self::Confirm(false)),
            _ => {
                let (action, option) = input.split_once('-')?;
                let index = digits(option)?;
                match action {
                    "pick" => Some(Self::Pick(index)),
                    "next" => Some(Self::Next(index)),
                    _ => Some(Self::Choose(digits(action.strip_prefix('q')?)?, index)),
                }
            }
        }
    }
}

fn digits(value: &str) -> Option<usize> {
    if value.is_empty() || !value.bytes().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn split_data(data: &str) -> Option<(&str, &str)> {
    let (token, action) = data.strip_prefix("omp:")?.split_once(':')?;
    (token.len() == 24
        && token
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')))
    .then_some((token, action))
}

enum Step {
    Finish(AnswerResponse),
    Save(Progress),
    MissingChoice,
    Reject,
}

fn step(pending: &Pending, action: Action) -> Step {
    let progress = &pending.progress;
    match (&pending.dialog, action) {
        (_, Action::Cancel) => Step::Finish(AnswerResponse::Cancel),
        (Dialog::Confirm { .. }, Action::Confirm(value)) => {
            Step::Finish(AnswerResponse::Confirm(value))
        }
        (Dialog::Select(selection), Action::Pick(index)) => {
            selection.options.get(index).map_or(Step::Reject, |label| {
                Step::Finish(AnswerResponse::Text(label.clone()))
            })
        }
        (Dialog::Ask { questions }, Action::Choose(index, choice))
            if progress.question == index =>
        {
            let Some(question) = questions.get(index) else {
                return Step::Reject;
            };
            let Some(option) = question.options.get(choice) else {
                return Step::Reject;
            };
            let mut next = progress.clone();
            if question.multi {
                if next.selected.contains(&option.label) {
                    next.selected.retain(|value| value != &option.label);
                } else {
                    next.selected.push(option.label.clone());
                }
            } else {
                next.selected = vec![option.label.clone()];
                next.custom_input = None;
            }
            Step::Save(next)
        }
        (Dialog::Ask { questions }, Action::Next(index)) if progress.question == index => {
            let Some(question) = questions.get(index) else {
                return Step::Reject;
            };
            let answer = Answer {
                id: question.id.clone(),
                selected_options: progress.selected.clone(),
                custom_input: progress.custom_input.clone(),
            };
            if pending.approval.decision.is_some() && !answer.matches(question) {
                return Step::MissingChoice;
            }
            let mut next = progress.clone();
            next.answers.push(answer);
            next.question += 1;
            if next.question == questions.len() {
                return Step::Finish(AnswerResponse::Ask(next.answers));
            }
            next.selected.clear();
            next.custom_input = None;
            Step::Save(next)
        }
        _ => Step::Reject,
    }
}

impl Core {
    pub(super) async fn handle_callback(
        &self,
        me: &Identity,
        query: &Callback,
    ) -> Result<(), EffectError> {
        let Some((token, action)) = query.data.as_deref().and_then(split_data) else {
            self.ack_callback(&query.id, Some(EXPIRED)).await;
            return Ok(());
        };
        let Some(message) = query
            .message
            .as_ref()
            .filter(|message| message.date != 0 && self.policy.owner(query.from))
        else {
            self.ack_callback(&query.id, Some(EXPIRED)).await;
            return Ok(());
        };
        let id = format!("callback:{}", query.id);
        let event = AnswerEvent {
            id: &id,
            callback: Some(&query.id),
        };
        let mut cards = self.cards.lock().await;
        let Some(pending) = cards.get_mut(token).filter(|card| card.progress.active) else {
            if let Some(route) = self
                .policy
                .route(me, &message.chat, message.message_thread_id)
                && let Ok(receipt) = self.receipt(me, &route, message)
                && self
                    .recover_answer(
                        ClosedAnswer {
                            token,
                            route: &route,
                            receipt: &receipt,
                        },
                        event,
                    )
                    .await?
            {
                return Ok(());
            }
            self.ack_callback(&query.id, Some(EXPIRED)).await;
            return Ok(());
        };
        self.policy.authorize(me, &pending.approval.route)?;
        if !pending.matches_card(self, me, message)
            || pending.progress.last_update.as_deref() == Some(&id)
            || !message.reply_markup.as_ref().is_some_and(|markup| {
                markup
                    .inline_keyboard
                    .iter()
                    .flatten()
                    .any(|button| button.callback_data == query.data)
            })
        {
            self.ack_callback(&query.id, Some(EXPIRED)).await;
            return Ok(());
        }
        match Action::parse(action).map_or(Step::Reject, |action| step(pending, action)) {
            Step::Finish(response) => {
                let result = self.finish_answer(pending, event, response).await;
                if !pending.progress.active {
                    cards.remove(token);
                }
                return result;
            }
            Step::Save(draft) => {
                self.save_draft(me, pending, draft, &id).await?;
                self.ack_callback(&query.id, None).await;
            }
            Step::MissingChoice => self.ack_callback(&query.id, Some(NO_CHOICE)).await,
            Step::Reject => self.ack_callback(&query.id, Some(EXPIRED)).await,
        }
        Ok(())
    }

    pub(super) async fn handle_reply(
        &self,
        me: &Identity,
        message: &Message,
        input: &Inbound,
    ) -> Result<bool, EffectError> {
        let Some(reply) = message.reply_to_message.as_deref() else {
            return Ok(false);
        };
        let receipt = reply.message_id.to_string();
        let mut cards = self.cards.lock().await;
        let candidate = cards.values_mut().find(|card| {
            card.approval.route.same_physical(&input.route)
                && card.progress.message_id.as_deref() == Some(&receipt)
        });
        if let Some(card) = candidate {
            if !card.progress.active
                || !card.matches_card(self, me, reply)
                || message.edit_date.is_some()
                || card.progress.last_update.as_deref() == Some(&input.id)
            {
                return Ok(true);
            }
            let Some(text) = message.text.as_deref().filter(|text| !text.is_empty()) else {
                return Ok(true);
            };
            match &card.dialog {
                Dialog::Input(_) | Dialog::Editor(_) => {
                    let token = card.token.clone();
                    let event = AnswerEvent {
                        id: &input.id,
                        callback: None,
                    };
                    let result = self
                        .finish_answer(card, event, AnswerResponse::Text(text.to_owned()))
                        .await;
                    if !card.progress.active {
                        cards.remove(&token);
                    }
                    result?;
                }
                Dialog::Ask { questions } => {
                    let mut draft = card.progress.clone();
                    if let Some(question) = questions.get(draft.question) {
                        draft.custom_input = Some(text.to_owned());
                        if !question.multi {
                            draft.selected.clear();
                        }
                        self.save_draft(me, card, draft, &input.id).await?;
                    }
                }
                Dialog::Confirm { .. } | Dialog::Select(_) => {}
            }
            return Ok(true);
        }
        if reply
            .from
            .is_some_and(|sender| sender.id == me.id && sender.is_bot)
        {
            if self.receipt(me, &input.route, reply).is_ok() {
                if let Some(token) = self.get(&answer_key(&input.route, &receipt))? {
                    let card = ClosedAnswer {
                        token: &token,
                        route: &input.route,
                        receipt: &receipt,
                    };
                    self.recover_answer(
                        card,
                        AnswerEvent {
                            id: &input.id,
                            callback: None,
                        },
                    )
                    .await?;
                }
                if self.get(&card_key(&input.route, &receipt))?.is_some() {
                    return Ok(true);
                }
            }
            if reply.reply_markup.as_ref().is_some_and(|markup| {
                markup.inline_keyboard.iter().flatten().any(|button| {
                    button
                        .callback_data
                        .as_deref()
                        .and_then(split_data)
                        .is_some()
                })
            }) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn save_draft(
        &self,
        me: &Identity,
        pending: &mut Pending,
        mut draft: Progress,
        event: &str,
    ) -> Result<(), EffectError> {
        draft.last_update = Some(event.to_owned());
        let view = pending
            .renderer()
            .draw(&pending.dialog, &draft)
            .ok_or_else(|| {
                EffectError::rejected(
                    "Dialog exceeds Telegram card limits; use the local OMP interface",
                )
            })?;
        let route = &pending.approval.route;
        self.save_progress(&route.account_id, &pending.token, &draft)?;
        pending.progress = draft;
        let id = pending
            .progress
            .message_id
            .as_deref()
            .map(message_id)
            .transpose()?;
        match self.write_card(route, id, &view).await {
            Ok(message)
                if pending.matches_card(self, me, &message)
                    && pending.matches_presentation(&message) =>
            {
                Ok(())
            }
            Ok(_) => Err(EffectError::uncertain(
                "Telegram returned an unexpected edited card",
            )),
            Err(error) if error.unchanged() => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
