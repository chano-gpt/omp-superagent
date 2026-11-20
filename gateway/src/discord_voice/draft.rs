use anyhow::{Context as _, Result, ensure};
use serde::Deserialize;
use serde_json::json;
use std::{fmt::Write as _, time::Duration};
use time::OffsetDateTime;
use tokio::{sync::oneshot, time::Instant};
use uuid::Uuid;

use super::Rest;
use super::policy::{Id, Message, Route, User, timestamp};

const TTL: Duration = Duration::from_secs(300);
const LIMIT: usize = 2_000;
const HEADER: &str = "Proposed work from the live voice session (not yet submitted).\n";
const CONFIRMED: &str = "Work proposed in a live Discord voice session and confirmed by the owner in writing.\n\
This is an ordinary work request, not a slash command, native control, or approval. \
Apply the usual safety and approval checks.\n\n";

#[derive(Default)]
pub(super) struct Drafts(Option<Slot>);

struct Slot {
    id: Uuid,
    since: Instant,
    status: Status,
}

enum Status {
    InFlight { _cancel: oneshot::Sender<()> },
    Uncertain,
    Posted(Posted),
}

pub(super) struct Offer {
    id: Uuid,
    route: Route,
    text: String,
    content: String,
    cancelled: oneshot::Receiver<()>,
}

struct Posted {
    route: Route,
    id: Id,
    created: OffsetDateTime,
    text: String,
}

#[derive(Deserialize)]
struct Receipt {
    id: Id,
    author: User,
    channel_id: Id,
    guild_id: Option<Id>,
    content: String,
    timestamp: String,
}

impl Drafts {
    pub(super) fn begin(&mut self, route: Route, text: &str) -> Result<Offer> {
        self.expire();
        ensure!(
            self.0.is_none(),
            "A voice work proposal is already outstanding"
        );
        let content = render(text)?;
        let (cancel, cancelled) = oneshot::channel();
        let id = Uuid::new_v4();
        self.0 = Some(Slot {
            id,
            since: Instant::now(),
            status: Status::InFlight { _cancel: cancel },
        });
        Ok(Offer {
            id,
            route,
            text: text.trim().to_owned(),
            content,
            cancelled,
        })
    }

    pub(super) fn finish(&mut self, delivery: Delivery, posting_deadline: Option<Instant>) -> bool {
        self.expire();
        let Some(slot) = self.0.as_mut().filter(|slot| slot.id == delivery.id) else {
            return false;
        };
        if let Some(posted) = delivery.posted
            && posting_deadline.is_none_or(|deadline| Instant::now() < deadline)
        {
            slot.status = Status::Posted(posted);
            slot.since = Instant::now();
            return true;
        }
        slot.status = Status::Uncertain;
        false
    }

    pub(super) fn consume(&mut self, message: &Message) -> Option<String> {
        self.expire();
        let Status::Posted(draft) = &self.0.as_ref()?.status else {
            return None;
        };
        let reference = message.message_reference.as_ref()?;
        if message.content != "/voice confirm"
            || message.kind != 19
            || !message.plain()
            || message.edited_timestamp.is_some()
            || message.webhook_id.is_some()
            || message.author.bot
            || message.author.id != draft.route.owner
            || message.guild_id != Some(draft.route.guild)
            || message.channel_id != draft.route.text
            || message.id == draft.id
            || timestamp(&message.timestamp)? < draft.created
            || reference.kind != 0
            || reference.message_id != Some(draft.id)
            || reference.channel_id != draft.route.text
            || reference
                .guild_id
                .is_some_and(|guild| guild != draft.route.guild)
        {
            return None;
        }
        let text = format!("{CONFIRMED}{}", draft.text);
        self.clear();
        Some(text)
    }

    pub(super) fn clear(&mut self) {
        // Dropping the sender wakes publication's cancellation select, including a
        // pending socket write. A late receipt cannot reactivate the removed slot.
        drop(self.0.take());
    }

    fn expire(&mut self) {
        if self
            .0
            .as_ref()
            .is_some_and(|slot| slot.since.elapsed() >= TTL)
        {
            self.clear();
        }
    }
}

pub(super) struct Delivery {
    id: Uuid,
    posted: Option<Posted>,
}

impl Offer {
    pub(super) async fn publish(mut self, rest: &Rest, deadline: Instant) -> Delivery {
        let result = tokio::select! {
            biased;
            _ = &mut self.cancelled => None,
            result = tokio::time::timeout_at(deadline, async {
                let receipt: Receipt = Rest::response(post_message(rest, self.route, &self.content)).await?;
                receipt.verify(self.route, &self.content, std::mem::take(&mut self.text))
            }) => result.ok().and_then(Result::ok),
        };
        Delivery {
            id: self.id,
            posted: result,
        }
    }
}

impl Receipt {
    fn verify(self, route: Route, content: &str, text: String) -> Result<Posted> {
        ensure!(
            self.author.bot
                && self.author.id == route.account
                && self.channel_id == route.text
                && self.guild_id.is_none_or(|id| id == route.guild)
                && self.content == content,
            "Unverified Discord voice draft receipt"
        );
        let created = timestamp(&self.timestamp).context("Invalid Discord draft timestamp")?;
        Ok(Posted {
            route,
            id: self.id,
            created,
            text,
        })
    }
}

pub(super) fn post_message(rest: &Rest, route: Route, content: &str) -> reqwest::RequestBuilder {
    rest.request(reqwest::Method::POST, &format!("channels/{}/messages", route.text))
        .timeout(Duration::from_secs(10))
        .json(&json!({"content": content, "allowed_mentions": {"parse": [], "replied_user": false}, "flags": 4}))
}

fn render(text: &str) -> Result<String> {
    ensure!(
        text.encode_utf16().count() <= LIMIT,
        "Voice work proposal exceeds the review limit"
    );
    let work = text.trim();
    ensure!(!work.is_empty(), "Voice work proposal is empty");
    let mut card = format!("{HEADER}```\n");
    for c in work.chars() {
        if c != '\n' && (c == '\\' || c == '`' || c.is_control() || invisible(c)) {
            write!(card, "\\u{{{:x}}}", u32::from(c))?;
        } else {
            card.push(c);
        }
    }
    card.push_str("\n```\nTo submit this proposal to HEAD, reply to this message with exactly `/voice confirm` \
while the voice session is active and within five minutes.\n\
Check the transcribed request before confirming. HEAD replies only in Discord text. Risky actions still require separate approval.");
    ensure!(
        card.encode_utf16().count() <= LIMIT,
        "Voice work proposal exceeds the review limit"
    );
    Ok(card)
}

// Cf (format controls) plus the non-Cf Unicode Default_Ignorable_Code_Point ranges.
fn invisible(c: char) -> bool {
    matches!(c, '\u{ad}' | '\u{34f}' | '\u{600}'..='\u{605}' | '\u{61c}' | '\u{6dd}' | '\u{70f}'
        | '\u{890}'..='\u{891}' | '\u{8e2}' | '\u{115f}'..='\u{1160}' | '\u{17b4}'..='\u{17b5}'
        | '\u{180b}'..='\u{180f}' | '\u{200b}'..='\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2060}'..='\u{206f}'
        | '\u{3164}' | '\u{fe00}'..='\u{fe0f}' | '\u{feff}' | '\u{ffa0}' | '\u{fff0}'..='\u{fffb}'
        | '\u{110bd}' | '\u{110cd}' | '\u{13430}'..='\u{13455}' | '\u{1bca0}'..='\u{1bca3}'
        | '\u{1d173}'..='\u{1d17a}' | '\u{e0000}'..='\u{e0fff}')
}

#[cfg(test)]
#[path = "draft_tests.rs"]
mod tests;
