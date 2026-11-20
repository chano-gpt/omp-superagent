use anyhow::{Context as _, Result, ensure};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    num::NonZeroU64,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct Id(NonZeroU64);

impl Id {
    pub(super) fn parse(s: &str) -> Option<Self> {
        if s.starts_with('0') || !s.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        s.parse().ok().map(Self)
    }

    pub(super) const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for Id {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(out)
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Self::parse(&raw).ok_or_else(|| serde::de::Error::custom("invalid Discord snowflake"))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Config {
    pub(super) token_env: String,
    owner_id: Id,
    pub(super) allowed_guild_ids: HashSet<Id>,
    allowed_channel_ids: HashSet<Id>,
    pub(super) voice: Settings,
    #[serde(skip)]
    pub(super) auth: Auth,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Settings {
    pub(super) channel_id: Id,
    pub(super) text_channel_id: Id,
    pub(super) voice: String,
    pub(super) max_session_seconds: u64,
    pub(super) model: Option<String>,
    pub(super) backend_model: Option<String>,
    pub(super) api_key_env: Option<String>,
    #[serde(default)]
    auth: Auth,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Auth {
    #[default]
    Api,
    Subscription,
}

impl Config {
    pub(super) fn parse(raw: &Value) -> Result<Self> {
        let mut config: Self =
            serde_json::from_value(raw.clone()).context("Invalid Discord voice configuration")?;
        let voice = &config.voice;
        ensure!(
            config.allowed_channel_ids.contains(&voice.text_channel_id)
                && voice.text_channel_id != voice.channel_id,
            "Voice work requires its own allowlisted text channel"
        );
        ensure!(
            (1..=2_147_483).contains(&voice.max_session_seconds),
            "Invalid voice session limit"
        );
        if matches!(voice.auth, Auth::Api) {
            ensure!(
                voice
                    .api_key_env
                    .as_ref()
                    .is_some_and(|key| key != &config.token_env)
                    && voice.model.is_some()
                    && voice.backend_model.is_some(),
                "API voice requires a separate key and two models"
            );
        }
        config.auth = voice.auth;
        Ok(config)
    }

    pub(super) fn authorize(
        &self,
        message: &Message,
        account: Option<Id>,
        started_at: OffsetDateTime,
    ) -> Option<Route> {
        let account = account?;
        let guild = message.guild_id?;
        let content = message.content.trim();
        if message.author.bot
            || message.author.id != self.owner_id
            || message.author.id == account
            || message.webhook_id.is_some()
            || message.edited_timestamp.is_some()
            || message.channel_id != self.voice.text_channel_id
            || !self.allowed_guild_ids.contains(&guild)
            || timestamp(&message.timestamp)?.unix_timestamp_nanos() / 1_000_000
                < started_at.unix_timestamp_nanos() / 1_000_000
            || !message.plain()
            || if content == "/voice confirm" {
                message.kind != 19
            } else {
                message.kind != 0 || message.message_reference.is_some()
            }
        {
            return None;
        }
        Some(Route {
            account,
            owner: self.owner_id,
            guild,
            text: message.channel_id,
        })
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct Route {
    pub(super) account: Id,
    pub(super) owner: Id,
    pub(super) guild: Id,
    pub(super) text: Id,
}

pub(super) enum Control {
    Help,
    Start(Permission),
    Confirm,
    Stop,
    WorkOff,
}

#[derive(Clone, Copy)]
pub(super) enum Permission {
    Off,
    Call,
    One,
}

impl Control {
    pub(super) fn matches(content: &str) -> bool {
        let content = content.trim();
        content
            .get(..6)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("/voice"))
            && content
                .get(6..)
                .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with(char::is_whitespace))
    }

    pub(super) fn parse(content: &str) -> Self {
        match content {
            "/voice start" => Self::Start(Permission::Off),
            "/voice start work" => Self::Start(Permission::Call),
            "/voice start share" => Self::Start(Permission::One),
            "/voice confirm" => Self::Confirm,
            "/voice stop" => Self::Stop,
            "/voice work off" => Self::WorkOff,
            _ => Self::Help,
        }
    }
}

#[derive(Deserialize)]
pub(super) struct Message {
    pub(super) id: Id,
    pub(super) author: User,
    pub(super) content: String,
    pub(super) guild_id: Option<Id>,
    pub(super) channel_id: Id,
    pub(super) timestamp: String,
    pub(super) edited_timestamp: Option<String>,
    pub(super) webhook_id: Option<Id>,
    #[serde(rename = "type")]
    pub(super) kind: u8,
    pub(super) message_reference: Option<Reference>,
    #[serde(default)]
    attachments: Vec<Value>,
    #[serde(default)]
    sticker_items: Vec<Value>,
    #[serde(default)]
    message_snapshots: Vec<Value>,
}

impl Message {
    pub(super) fn plain(&self) -> bool {
        self.attachments.is_empty()
            && self.sticker_items.is_empty()
            && self.message_snapshots.is_empty()
    }
}

#[derive(Deserialize)]
pub(super) struct User {
    pub(super) id: Id,
    #[serde(default)]
    pub(super) bot: bool,
}

#[derive(Deserialize)]
pub(super) struct Reference {
    pub(super) message_id: Option<Id>,
    pub(super) channel_id: Id,
    pub(super) guild_id: Option<Id>,
    #[serde(rename = "type", default)]
    pub(super) kind: u8,
}

pub(super) fn timestamp(raw: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(raw, &Rfc3339).ok()
}

#[derive(Deserialize)]
pub(super) struct VoiceState {
    pub(super) user_id: Id,
    pub(super) channel_id: Option<Id>,
    pub(super) session_id: String,
}

#[derive(Deserialize)]
pub(super) struct VoiceUpdate {
    pub(super) guild_id: Id,
    #[serde(flatten)]
    pub(super) state: VoiceState,
}

impl std::ops::Deref for VoiceUpdate {
    type Target = VoiceState;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

#[derive(Deserialize)]
pub(super) struct VoiceServer {
    pub(super) guild_id: Id,
    pub(super) endpoint: Option<String>,
    pub(super) token: String,
}

#[derive(Default)]
pub(super) struct Room(HashMap<Id, HashMap<Id, Id>>);

impl Room {
    pub(super) fn remove(&mut self, guild: Id) {
        self.0.remove(&guild);
    }

    pub(super) fn known(&self, guild: Id) -> bool {
        self.0.contains_key(&guild)
    }

    pub(super) fn snapshot(&mut self, guild: Id, states: Option<&[Value]>) {
        let mut users = HashMap::new();
        if let Some(states) = states {
            for raw in states {
                let Ok(state) = serde_json::from_value::<VoiceState>(raw.clone()) else {
                    self.remove(guild);
                    return;
                };
                if let Some(channel) = state.channel_id {
                    users.insert(state.user_id, channel);
                }
            }
            self.0.insert(guild, users);
        } else {
            self.remove(guild);
        }
    }

    pub(super) fn update(&mut self, voice: &VoiceUpdate) -> Option<Id> {
        let users = self.0.get_mut(&voice.guild_id)?;
        match voice.channel_id {
            Some(channel) => users.insert(voice.user_id, channel),
            None => users.remove(&voice.user_id),
        }
    }

    pub(super) fn channel(&self, guild: Id, user: Id) -> Option<Id> {
        self.0.get(&guild)?.get(&user).copied()
    }

    pub(super) fn private_before_join(&self, route: Route, target: Id) -> bool {
        self.private(route, target, false)
    }

    pub(super) fn private_after_join(&self, route: Route, target: Id) -> bool {
        self.private(route, target, true)
    }

    fn private(&self, route: Route, target: Id, joined: bool) -> bool {
        self.0.get(&route.guild).is_some_and(|users| {
            users.get(&route.owner) == Some(&target)
                && users.get(&route.account) == joined.then_some(&target)
                && users.iter().all(|(user, channel)| {
                    *channel != target || *user == route.owner || *user == route.account
                })
        })
    }
}

#[cfg(test)]
#[path = "policy_tests.rs"]
mod tests;
