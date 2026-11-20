//! Optional, explicitly started Discord voice. No voice worker is resident between calls.
#[path = "discord_voice/draft.rs"]
mod draft;
#[path = "discord_voice/link.rs"]
mod link;
#[path = "discord_voice/policy.rs"]
mod policy;
#[path = "discord_voice/provider.rs"]
mod provider;
#[path = "discord_voice/session.rs"]
mod session;

use anyhow::{Context as _, Result};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use self::{
    policy::{Auth, Config, Control, Id, Message, Permission, Room, Route},
    session::{Active, JoinEvent},
};
use super::{
    model::Channel,
    permissions::{VIEW_CHANNEL, guild_permissions, has},
    rest::Rest,
};
use crate::adapter::Context;

pub(super) enum Command {
    Ignore,
    Consumed,
    Confirmed(String),
}

pub(super) struct Voice(Arc<Service>);

struct Service {
    config: Config,
    ctx: Context,
    rest: Arc<Rest>,
    state: Mutex<State>,
    started_at: OffsetDateTime,
    notices: Arc<Semaphore>,
}

#[derive(Default)]
struct State {
    account: Option<Id>,
    outbound: Option<mpsc::Sender<Value>>,
    room: Room,
    call: Option<Active>,
    leave_pending: HashMap<Id, Route>,
    last_command: u64,
    last_work_off: u64,
    last_draft_start: u64,
}

impl Voice {
    pub(super) fn new(config: &Value, ctx: Context) -> Result<Self> {
        let config = Config::parse(config)?;
        let token =
            std::env::var(&config.token_env).context("Discord voice credential unavailable")?;
        let rest = Arc::new(Rest::new(ctx.http.clone(), token)?);
        Ok(Self(Arc::new(Service {
            config,
            ctx,
            rest,
            state: Mutex::default(),
            started_at: OffsetDateTime::now_utc(),
            notices: Arc::new(Semaphore::new(4)),
        })))
    }

    pub(super) fn is_control(&self, data: &Value) -> bool {
        data["channel_id"].as_str().and_then(Id::parse) == Some(self.0.config.voice.text_channel_id)
            && data["content"].as_str().is_some_and(Control::matches)
    }

    pub(super) fn is_priority_control(&self, data: &Value) -> bool {
        self.is_control(data)
            && data["content"].as_str().is_some_and(|content| {
                matches!(
                    Control::parse(content.trim()),
                    Control::Stop
                        | Control::WorkOff
                        | Control::Start(Permission::Call | Permission::One)
                )
            })
    }

    pub(super) async fn command(
        &self,
        data: &Value,
        outbound: &mpsc::Sender<Value>,
    ) -> Result<Command> {
        if !self.is_control(data) {
            return Ok(Command::Ignore);
        }
        let service = &self.0;
        let Ok(message) = serde_json::from_value::<Message>(data.clone()) else {
            return Ok(Command::Consumed);
        };
        let control = Control::parse(message.content.trim());
        let mut state = service.state.lock().await;
        let Some(route) = service
            .config
            .authorize(&message, state.account, service.started_at)
        else {
            return Ok(Command::Consumed);
        };
        if !state.connected_to(outbound) {
            return Ok(Command::Consumed);
        }
        let seq = message.id.get();
        if seq <= state.last_command
            || (!matches!(control, Control::Start(Permission::Off)) && seq <= state.last_work_off)
        {
            return Ok(Command::Consumed);
        }
        if matches!(control, Control::Start(Permission::Call | Permission::One)) {
            if seq <= state.last_draft_start {
                return Ok(Command::Consumed);
            }
            state.last_draft_start = seq;
        }
        if matches!(control, Control::Start(_)) && state.call.is_some() {
            if matches!(control, Control::Start(Permission::Off)) {
                state.last_command = seq;
            }
            service.notice(route, "A voice session is already starting or active.");
            return Ok(Command::Consumed);
        }
        if matches!(control, Control::Start(Permission::One))
            && !matches!(service.config.auth, Auth::Subscription)
        {
            service.notice(route, "/voice start share is only available in subscription mode. Use /voice start work for API voice drafts.");
            return Ok(Command::Consumed);
        }
        if matches!(control, Control::WorkOff) {
            state.last_work_off = seq;
        } else {
            state.last_command = seq;
        }
        match control {
            Control::Confirm => {
                if let Some(text) = state
                    .call
                    .as_mut()
                    .filter(|c| c.route == route && c.open())
                    .and_then(|c| c.drafts.consume(&message))
                {
                    return Ok(Command::Confirmed(text));
                }
                service.notice(route, "No matching active voice draft. Reply to the current draft, or write the request directly.");
            }
            Control::Stop => {
                if let Some(call) = state.call.as_mut() {
                    call.cancel();
                }
                service.notice(route, "Voice is off.");
            }
            Control::WorkOff => {
                if let Some(call) = state.call.as_mut() {
                    call.revoke_work();
                }
                service.notice(
                    route,
                    "Voice work drafts are off. Unconfirmed drafts can no longer be submitted.",
                );
            }
            Control::Start(permission) => {
                // Work-off (which can bypass queued ordinary inputs) never grants a queued start work rights.
                if !state
                    .room
                    .private_before_join(route, service.config.voice.channel_id)
                    || state.leave_pending.contains_key(&route.guild)
                {
                    service.notice(route, "Join the configured voice channel alone before starting voice. The bot must not already be connected.");
                    return Ok(Command::Consumed);
                }
                let cancel = CancellationToken::new();
                let (join, joining) = mpsc::channel(4);
                let call = session::Call {
                    route,
                    generation: seq,
                    outbound: outbound.clone(),
                };
                let (active, finished) = Active::new(&call, permission, join, cancel.clone());
                state.call = Some(active);
                let service = Arc::clone(service);
                tokio::spawn(async move {
                    service.run_call(call, joining, cancel).await;
                    drop(finished);
                });
            }
            Control::Help => {
                let help = match service.config.auth {
                    Auth::Subscription => {
                        "Use /voice start for conversation, /voice start work for work drafts, or /voice start share for one draft. Use /voice work off or /voice stop to revoke."
                    }
                    Auth::Api => {
                        "Use /voice start for conversation or /voice start work for work drafts. Use /voice work off or /voice stop to revoke. Voice uses paid OpenAI Live audio."
                    }
                };
                service.notice(route, help);
            }
        }
        Ok(Command::Consumed)
    }

    pub(super) async fn on_dispatch(
        &self,
        event: &str,
        data: &Value,
        outbound: &mpsc::Sender<Value>,
    ) {
        let mut state = self.0.state.lock().await;
        let target = self.0.config.voice.channel_id;
        match event {
            "READY" => {
                state.account = data["user"]["id"].as_str().and_then(Id::parse);
                state.outbound = Some(outbound.clone());
                state.room = Room::default();
                if let Some(active) = state.call.as_mut() {
                    active.disconnect();
                }
            }
            "RESUMED" => {
                state.outbound = Some(outbound.clone());
                let guilds: Vec<Id> = state.leave_pending.keys().copied().collect();
                for guild in guilds {
                    state.reconcile_leave(guild, target, outbound);
                }
            }
            "GUILD_CREATE" => {
                let Some(guild) = data["id"].as_str().and_then(Id::parse) else {
                    return;
                };
                if self.0.config.allowed_guild_ids.contains(&guild) {
                    state
                        .room
                        .snapshot(guild, data["voice_states"].as_array().map(Vec::as_slice));
                    if state.connected_to(outbound) {
                        state.reconcile_leave(guild, target, outbound);
                    }
                    if let Some(active) = state.call.as_mut().filter(|c| c.route.guild == guild) {
                        active.cancel(); // An authoritative guild replacement invalidates the active room.
                    }
                }
            }
            "GUILD_DELETE" => {
                let Some(guild) = data["id"].as_str().and_then(Id::parse) else {
                    return;
                };
                state.room.remove(guild);
                if let Some(active) = state.call.as_mut().filter(|c| c.route.guild == guild) {
                    active.cancel();
                }
            }
            "CHANNEL_DELETE"
                if data["id"].as_str().and_then(Id::parse).is_some_and(|id| {
                    id == target || id == self.0.config.voice.text_channel_id
                }) =>
            {
                if let Some(active) = state.call.as_mut() {
                    active.cancel();
                }
            }
            "VOICE_STATE_UPDATE" => {
                let Ok(update) = serde_json::from_value::<policy::VoiceUpdate>(data.clone()) else {
                    if let Some(guild) = data["guild_id"].as_str().and_then(Id::parse) {
                        state.room.remove(guild);
                        if let Some(active) = state.call.as_mut().filter(|c| c.route.guild == guild)
                        {
                            active.cancel();
                        }
                    }
                    return;
                };
                if !self.0.config.allowed_guild_ids.contains(&update.guild_id) {
                    return;
                }
                let previous = state.room.update(&update);
                if state.account == Some(update.user_id) && update.state.channel_id.is_none() {
                    state.leave_pending.remove(&update.guild_id);
                }
                if let Some(active) = state
                    .call
                    .as_mut()
                    .filter(|c| c.route.guild == update.guild_id)
                {
                    active.voice_state(update, previous, target);
                }
            }
            "VOICE_SERVER_UPDATE" => {
                let Ok(server) = serde_json::from_value::<policy::VoiceServer>(data.clone()) else {
                    return;
                };
                if let Some(active) = state
                    .call
                    .as_mut()
                    .filter(|c| c.route.guild == server.guild_id && c.join_sent)
                {
                    active.relay(JoinEvent::Server(server));
                }
            }
            _ => {}
        }
    }

    pub(super) async fn on_disconnect(&self) {
        let mut state = self.0.state.lock().await;
        state.outbound = None;
        if let Some(active) = state.call.as_mut() {
            active.disconnect();
            if active.join_sent {
                let route = active.route;
                state.leave_pending.insert(route.guild, route);
            }
        }
    }

    pub(super) async fn stop(&self) {
        let done = {
            let mut state = self.0.state.lock().await;
            state.call.as_mut().map(|active| {
                active.cancel();
                active.done.clone()
            })
        };
        if let Some(mut done) = done {
            // Sender is dropped by the call after closing media/provisioning.
            if let Err(error) = done.changed().await {
                drop(error);
            }
        }
    }
}

impl State {
    fn connected_to(&self, outbound: &mpsc::Sender<Value>) -> bool {
        self.outbound
            .as_ref()
            .is_some_and(|current| !current.is_closed() && current.same_channel(outbound))
    }

    fn reconcile_leave(&mut self, guild: Id, target: Id, outbound: &mpsc::Sender<Value>) {
        let Some(route) = self.leave_pending.get(&guild).copied() else {
            return;
        };
        if self.account != Some(route.account) || !self.room.known(guild) {
            return;
        }
        if self.room.channel(guild, route.account) != Some(target) {
            self.leave_pending.remove(&guild);
        } else if outbound.try_send(voice_state(route, None)).is_err() {
            eprintln!("Discord voice reconnect leave was not queued");
        }
    }
}

impl Service {
    fn notice(&self, route: Route, content: &'static str) {
        let Ok(slot) = self.notices.clone().try_acquire_owned() else {
            return;
        };
        let rest = self.rest.clone();
        tokio::spawn(async move {
            let _slot = slot;
            if Rest::empty(draft::post_message(&rest, route, content))
                .await
                .is_err()
            {
                eprintln!("Discord voice notice was not confirmed");
            }
        });
    }

    async fn verify_channels(&self, route: Route) -> Result<()> {
        const CONNECT: u64 = 1 << 20;
        const SPEAK: u64 = 1 << 21;
        let text = route.text.to_string();
        let voice = self.config.voice.channel_id.to_string();
        let guild = route.guild.to_string();
        let text_path = format!("channels/{text}");
        let voice_path = format!("channels/{voice}");
        let (text_channel, voice_channel) = tokio::try_join!(
            self.rest.get::<Channel>(&text_path),
            self.rest.get::<Channel>(&voice_path)
        )?;
        anyhow::ensure!(
            text_channel.id == text
                && text_channel.kind == 0
                && voice_channel.id == voice
                && voice_channel.kind == 2
                && text_channel.guild_id.as_deref() == Some(guild.as_str())
                && voice_channel.guild_id.as_deref() == Some(guild.as_str()),
            "Discord voice channel identity changed"
        );
        let permissions =
            guild_permissions(&self.rest, &voice_channel, &route.account.to_string()).await?;
        anyhow::ensure!(
            has(permissions, VIEW_CHANNEL | CONNECT | SPEAK),
            "Discord bot lacks permission to view, join, or speak in its configured voice channel"
        );
        Ok(())
    }
}

fn voice_state(route: Route, channel: Option<Id>) -> Value {
    json!({"op":4,"d":{"guild_id":route.guild.to_string(),"channel_id":channel.map(|id|id.to_string()),
        "self_mute":false,"self_deaf":false}})
}
