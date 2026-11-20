#[path = "media.rs"]
mod media;

use anyhow::{Context as _, Result, ensure};
use serde_json::Value;
use std::time::Duration;
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;

use super::{
    Service,
    draft::Drafts,
    link::{Join, Link},
    policy::{Auth, Id, Permission, Route, VoiceServer, VoiceUpdate},
    provider::{Provider, Setup},
    voice_state,
};

const SETUP_WINDOW: Duration = Duration::from_secs(15);
const SHARE_WINDOW: Duration = Duration::from_secs(60);

pub(super) enum JoinEvent {
    State(String),
    Server(VoiceServer),
}

pub(super) struct Call {
    pub(super) route: Route,
    pub(super) generation: u64,
    pub(super) outbound: mpsc::Sender<Value>,
}

pub(super) struct Active {
    pub(super) route: Route,
    pub(self) generation: u64,
    pub(self) permission: Permission,
    pub(self) share_until: Option<Instant>,
    pub(super) drafts: Drafts,
    pub(super) join_sent: bool,
    pub(super) done: tokio::sync::watch::Receiver<()>,
    cancel: CancellationToken,
    joining: Option<mpsc::Sender<JoinEvent>>,
    joined: Option<String>,
    leaving: Option<oneshot::Sender<()>>,
    ready: bool,
    disconnected: bool,
}

impl Active {
    pub(super) fn new(
        call: &Call,
        permission: Permission,
        joining: mpsc::Sender<JoinEvent>,
        cancel: CancellationToken,
    ) -> (Self, tokio::sync::watch::Sender<()>) {
        let (finished, done) = tokio::sync::watch::channel(());
        (
            Self {
                route: call.route,
                generation: call.generation,
                permission,
                share_until: None,
                drafts: Drafts::default(),
                join_sent: false,
                done,
                cancel,
                joining: Some(joining),
                joined: None,
                leaving: None,
                ready: false,
                disconnected: false,
            },
            finished,
        )
    }

    pub(super) fn cancel(&mut self) {
        self.revoke_work();
        self.cancel.cancel();
    }

    pub(super) fn disconnect(&mut self) {
        self.disconnected = true;
        drop(self.leaving.take());
        self.cancel();
    }

    pub(super) fn revoke_work(&mut self) {
        self.permission = Permission::Off;
        self.share_until = None;
        self.drafts.clear();
    }

    pub(super) fn open(&self) -> bool {
        self.ready && !self.cancel.is_cancelled()
    }

    pub(super) fn voice_state(&mut self, update: VoiceUpdate, previous: Option<Id>, channel: Id) {
        if update.user_id == self.route.account {
            if update.channel_id != Some(channel)
                && let Some(signal) = self.leaving.take()
            {
                match signal.send(()) {
                    Ok(()) | Err(()) => {}
                }
            }
            if self.join_sent
                && self.joined.is_none()
                && update.channel_id == Some(channel)
                && !update.session_id.is_empty()
            {
                self.joined = Some(update.session_id.clone());
                self.relay(JoinEvent::State(update.state.session_id));
            } else if update.channel_id != Some(channel)
                || update.channel_id != previous
                || self.joined.as_deref() != Some(update.session_id.as_str())
            {
                self.cancel();
            }
        } else if (update.user_id == self.route.owner && update.channel_id != Some(channel))
            || (update.user_id != self.route.owner && update.channel_id == Some(channel))
        {
            self.cancel();
        }
    }

    pub(super) fn relay(&mut self, event: JoinEvent) {
        if self
            .joining
            .as_ref()
            .is_none_or(|joining| joining.try_send(event).is_err())
        {
            self.cancel();
        }
    }
}

impl Service {
    pub(super) async fn run_call(
        &self,
        call: Call,
        joining: mpsc::Receiver<JoinEvent>,
        cancel: CancellationToken,
    ) {
        let deadline = Instant::now() + Duration::from_secs(self.config.voice.max_session_seconds);
        let Some(provider) = Setup::authorize(self) else {
            self.release(
                &call,
                Some("A separate voice provider key is required. No Live session was opened."),
            )
            .await;
            return;
        };
        let setup = self.connect_link(&call, joining);
        let mut link = match tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            ready = timeout_at(deadline.min(Instant::now() + SETUP_WINDOW), setup) => Some(ready),
        } {
            Some(Ok(Ok(link))) => link,
            Some(_) => {
                self.fail_start(&call).await;
                return;
            }
            None => {
                self.finish(&call, None).await;
                return;
            }
        };

        let mut provider = match tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            ready = timeout_at(deadline, Self::connect_provider(&mut link, provider)) => Some(ready),
        } {
            Some(Ok(Ok(provider))) => provider,
            Some(_) => {
                drop(link);
                self.fail_start(&call).await;
                return;
            }
            None => {
                drop(link);
                self.finish(&call, None).await;
                return;
            }
        };

        let ready = self.mark_ready(&call).await;
        let reason = if ready {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => None,
                _ = tokio::time::sleep_until(deadline) => Some("Voice stopped at the configured session time limit."),
                result = self.media(&call, &mut link, &mut provider) => {
                    if result.is_err() { eprintln!("Discord voice session ended after media, delivery, or provider failure"); }
                    Some("Voice session ended. Use /voice start to open a new session.")
                }
            }
        } else {
            None
        };
        drop(link);
        self.leave(&call).await;
        provider.close().await;
        self.release(&call, reason).await;
    }

    async fn connect_link(
        &self,
        call: &Call,
        mut joining: mpsc::Receiver<JoinEvent>,
    ) -> Result<Link> {
        self.verify_channels(call.route).await?;
        let channel = self.config.voice.channel_id;
        // Reserve queue capacity before committing the operation under the room lock. That
        // guarantees stop/revocation can never race a backlogged, not-yet-sent join.
        let send = call
            .outbound
            .reserve()
            .await
            .context("Discord gateway disconnect during voice join")?;
        {
            let mut state = self.state.lock().await;
            ensure!(
                state.connected_to(&call.outbound)
                    && state.room.private_before_join(call.route, channel),
                "Discord voice room changed before joining"
            );
            let active = state
                .call
                .as_mut()
                .filter(|active| {
                    active.generation == call.generation && !active.cancel.is_cancelled()
                })
                .context("Discord voice join was cancelled")?;
            active.join_sent = true;
            send.send(voice_state(call.route, Some(channel)));
        }
        let mut session_id = None;
        let mut server = None;
        let join = loop {
            match joining
                .recv()
                .await
                .context("Discord voice Gateway updates ended")?
            {
                JoinEvent::State(value) => {
                    session_id = Some(value);
                }
                JoinEvent::Server(value) => {
                    ensure!(
                        server.is_none(),
                        "Discord voice changed servers during handshake"
                    );
                    server = Some(value);
                }
            }
            if session_id.is_some()
                && let Some(VoiceServer {
                    endpoint, token, ..
                }) = server.take()
            {
                ensure!(!token.is_empty(), "Discord voice token unavailable");
                break Join {
                    endpoint: endpoint.context("Discord voice server unavailable")?,
                    token,
                    session_id: session_id
                        .take()
                        .context("Discord voice session unavailable")?,
                };
            }
        };
        {
            let mut state = self.state.lock().await;
            ensure!(
                state.connected_to(&call.outbound)
                    && state.room.private_after_join(call.route, channel),
                "Discord voice room changed during join"
            );
            let active = state
                .call
                .as_mut()
                .filter(|active| {
                    active.generation == call.generation && !active.cancel.is_cancelled()
                })
                .context("Discord voice join was cancelled")?;
            active.joining = None;
            ensure!(
                joining.is_empty(),
                "Discord voice has an unexpected queued state change"
            );
        }
        Link::open(call.route, channel, join).await
    }

    async fn connect_provider(link: &mut Link, setup: Setup<'_>) -> Result<Provider> {
        let provider = setup.open();
        tokio::pin!(provider);
        loop {
            tokio::select! {
                biased;
                transfer = link.poll() => {
                    link.handle(transfer?).await?;
                    ensure!(link.encrypted(), "Owner-only Discord media verification was lost");
                }
                result = &mut provider => return result,
            }
        }
    }

    async fn mark_ready(&self, call: &Call) -> bool {
        let mut state = self.state.lock().await;
        if !state.connected_to(&call.outbound)
            || !state
                .room
                .private_after_join(call.route, self.config.voice.channel_id)
        {
            return false;
        }
        let Some(active) = state
            .call
            .as_mut()
            .filter(|active| active.generation == call.generation && !active.cancel.is_cancelled())
        else {
            return false;
        };
        active.ready = true;
        let notice = match (self.config.auth, active.permission) {
            (Auth::Api, Permission::Call) => {
                "Voice ready. Audio goes to OpenAI Live. Explicit work requests can be posted here as drafts for this call. Use /voice stop to disconnect."
            }
            (Auth::Subscription, Permission::Call) => {
                "Voice ready. Audio goes to OpenAI via your Codex subscription. Explicit work requests can be posted here as drafts for this call. Use /voice stop to disconnect."
            }
            (Auth::Subscription, Permission::One) => {
                active.share_until = Some(Instant::now() + SHARE_WINDOW);
                "Voice ready. Audio goes to OpenAI via your Codex subscription. One work draft can be posted here in the next minute. Use /voice stop to disconnect."
            }
            (Auth::Api, Permission::Off | Permission::One) => {
                "Voice ready. Audio goes to OpenAI Live. Draft posting is off. To enable it, stop this call and use /voice start work."
            }
            (Auth::Subscription, Permission::Off) => {
                "Voice ready. Audio goes to OpenAI via your Codex subscription. Draft posting is off. To enable it, stop this call and use /voice start work."
            }
        };
        self.notice(call.route, notice);
        true
    }

    async fn fail_start(&self, call: &Call) {
        let message = match self.config.auth {
            Auth::Subscription => {
                "Could not open Codex subscription voice. Check the voice bridge, subscription login, voice access and Discord channel. No API fallback."
            }
            Auth::Api => {
                "Could not open voice. Check channel membership, permissions, voice dependencies and provider settings. No automatic retry."
            }
        };
        self.finish(call, Some(message)).await;
    }

    async fn finish(&self, call: &Call, reason: Option<&'static str>) {
        self.leave(call).await;
        self.release(call, reason).await;
    }

    async fn leave(&self, call: &Call) {
        let left = {
            let mut state = self.state.lock().await;
            let bot_channel = state.room.channel(call.route.guild, call.route.account);
            let Some(active) = state
                .call
                .as_mut()
                .filter(|active| active.generation == call.generation)
            else {
                return;
            };
            active.revoke_work();
            active.ready = false;
            active.joining = None;
            if !active.join_sent
                || active.disconnected
                || bot_channel.is_some_and(|channel| channel != self.config.voice.channel_id)
            {
                return;
            }
            let (signal, left) = oneshot::channel();
            active.leaving = Some(signal);
            state.leave_pending.insert(call.route.guild, call.route);
            left
        };
        let result = timeout(Duration::from_secs(2), async {
            call.outbound
                .send(voice_state(call.route, None))
                .await
                .context("Discord voice socket closed")?;
            // The bot's voice state, not a local send, clears the rejoin fence.
            left.await.context("Discord voice leave was not observed")
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            eprintln!("Discord voice leave was not confirmed");
        }
    }

    async fn release(&self, call: &Call, reason: Option<&'static str>) {
        let mut state = self.state.lock().await;
        if state
            .call
            .as_ref()
            .is_some_and(|active| active.generation == call.generation)
        {
            state.call = None;
            if let Some(reason) = reason {
                self.notice(call.route, reason);
            }
        }
    }
}
