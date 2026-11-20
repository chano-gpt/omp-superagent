use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::rest::broken;
use super::{
    Effect, EffectError, State,
    model::{Channel, Route, Topic, utf16},
};

const KEY: &str = "discord:purposes";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(
    tag = "action",
    rename_all = "lowercase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(super) enum Purpose {
    List {
        source_id: String,
        route: Route,
    },
    Set {
        source_id: String,
        route: Route,
        purpose: String,
    },
    Clear {
        source_id: String,
        route: Route,
    },
}

impl Purpose {
    fn source(&self) -> (&str, &Route) {
        match self {
            Self::List { source_id, route }
            | Self::Set {
                source_id, route, ..
            }
            | Self::Clear { source_id, route } => (source_id, route),
        }
    }

    fn key(&self) -> String {
        let (id, route) = self.source();
        json!([id, route.key()]).to_string()
    }

    fn valid(&self) -> bool {
        let (source, route) = self.source();
        !source.trim().is_empty()
            && utf16(source) <= 1000
            && route.topic_id.is_none()
            && route.platform == "discord"
            && (route.guild_id.is_some() || route.thread_id.is_none())
            && match self {
                Self::Set { purpose, .. } => valid_note(purpose),
                Self::List { .. } | Self::Clear { .. } => true,
            }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Note {
    route: Route,
    purpose: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    request: Purpose,
    result: Vec<Note>,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Purposes {
    notes: HashMap<String, Note>,
    receipts: HashMap<String, Receipt>,
}

impl Purposes {
    fn notes(&self, route: &Route) -> Vec<Note> {
        let parent = self.notes.get(&route.parent().key());
        let thread = route
            .thread_id
            .as_ref()
            .and_then(|_| self.notes.get(&route.physical().key()));
        [parent, thread].into_iter().flatten().cloned().collect()
    }
}

fn valid_note(purpose: &str) -> bool {
    !purpose.trim().is_empty() && utf16(purpose) <= 4000
}

fn received_note(note: &Note) -> bool {
    note.route.platform == "discord"
        && note.route.topic_id.is_none()
        && (note.route.guild_id.is_some() || note.route.thread_id.is_none())
        && valid_note(&note.purpose)
}

impl State {
    fn read_purposes(&self) -> Effect<Purposes> {
        let Some(raw) = self
            .ctx
            .get_meta(KEY)
            .map_err(|_| broken("Discord purpose state is unavailable"))?
        else {
            return Ok(Purposes::default());
        };
        let invalid = || {
            EffectError::rejected("Discord purpose state is damaged; existing notes were preserved")
        };
        let stored: Purposes = serde_json::from_str(&raw).map_err(|_| invalid())?;
        if stored
            .notes
            .iter()
            .any(|(key, note)| key != &note.route.key() || !received_note(note))
            || stored.receipts.iter().any(|(key, receipt)| {
                key != &receipt.request.key()
                    || !receipt.request.valid()
                    || !receipt.result.iter().all(received_note)
            })
        {
            return Err(invalid());
        }
        Ok(stored)
    }

    pub(super) async fn purpose(&self, request: Purpose) -> Effect<Value> {
        if !request.valid() {
            return Err(EffectError::rejected("Invalid Discord purpose request"));
        }
        let (_, route) = request.source();
        self.assert_route(route)?;
        if route.guild_id.is_none() {
            self.channel(route, false).await?;
        }
        let _guard = self.purpose_lock.lock().await;
        let mut stored = self.read_purposes()?;
        if let Purpose::List { .. } = request {
            return Ok(json!(stored.notes(route)));
        }
        let receipt_key = request.key();
        if let Some(saved) = stored.receipts.get(&receipt_key) {
            if saved.request != request {
                return Err(EffectError::rejected(
                    "Discord purpose source identity conflict",
                ));
            }
            return Ok(json!(saved.result));
        }
        match &request {
            Purpose::List { .. } => (),
            Purpose::Clear { .. } => {
                stored.notes.remove(&route.key());
            }
            Purpose::Set { purpose, .. } => {
                stored.notes.insert(
                    route.key(),
                    Note {
                        route: route.clone(),
                        purpose: purpose.clone(),
                    },
                );
            }
        }
        let result = stored.notes(route);
        let value = json!(result);
        stored
            .receipts
            .insert(receipt_key, Receipt { request, result });
        let encoded = serde_json::to_string(&stored)
            .map_err(|_| broken("Discord purpose state could not be encoded"))?;
        self.ctx
            .set_meta(KEY, &encoded)
            .map_err(|_| broken("Discord purpose state could not be saved"))?;
        Ok(value)
    }

    pub(super) async fn context_snapshot(&self, route: &Route, channel: Option<&Channel>) -> Value {
        let mut context = json!({ "kind": if route.guild_id.is_some() { "guild" } else { "dm" },
            "channel": { "id": route.channel_id } });
        if let Some(thread) = &route.thread_id {
            context["thread"] = json!({ "id": thread, "parentId": route.channel_id });
        }
        let cache = self.channels.read().await;
        let observed =
            channel.filter(|c| c.id == route.destination() && c.guild_id == route.guild_id);
        let parent = if route.thread_id.is_some() {
            cache.get(&route.channel_id)
        } else {
            observed
        };
        if let Some(parent) =
            parent.filter(|p| p.id == route.channel_id && p.guild_id == route.guild_id)
        {
            if let Some(name) = &parent.name {
                context["channel"]["name"] = json!(name);
            }
            if let Topic::Observed(topic) = &parent.topic {
                context["channel"]["topic"] = json!(topic);
            }
        }
        if route.thread_id.is_some()
            && let Some(name) = observed.and_then(|c| c.name.as_ref())
        {
            context["thread"]["name"] = json!(name);
        }
        drop(cache);
        match self.read_purposes() {
            Ok(purposes) => {
                for note in purposes.notes(route) {
                    let key = if note.route.thread_id.is_some() {
                        "thread"
                    } else {
                        "channel"
                    };
                    context[key]["purpose"] = json!(note.purpose);
                }
            }
            Err(_) => eprintln!("Discord purpose metadata unavailable; intake preserved"),
        }
        context
    }

    pub(super) async fn inspect_context(&self, route: &Route) -> Effect<Value> {
        self.assert_route(route)?;
        let channel = match self.channel(route, false).await {
            Ok(channel) => Some(channel),
            Err(error) if error.certainty == "rejected" => return Err(error),
            Err(_) => {
                eprintln!("Discord channel inspection unavailable");
                None
            }
        };
        // A newly seen thread may have no cached parent yet; inspection can
        // hydrate it without making optional metadata part of owner intake.
        if route.thread_id.is_some()
            && let Err(error) = self.channel(&route.parent(), false).await
            && error.certainty != "rejected"
        {
            eprintln!("Discord parent channel inspection unavailable");
        }
        Ok(self.context_snapshot(route, channel.as_ref()).await)
    }
}
