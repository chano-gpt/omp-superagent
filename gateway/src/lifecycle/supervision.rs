use anyhow::{Context, Result, ensure};
use std::process;
use uuid::Uuid;

use crate::adapter::EffectError;

use super::{
    Lifecycle, Phase,
    fs::{alive, remove_dead_socket},
    host_launch::PreparedHost,
    host_rpc::{Head, PROBE_TIMEOUT, Probe, ShutdownRefused, confirmed_exit},
    restart_record::{self, ACTIVE, FENCE, Fence, HEAD, Record, STOP, Stage},
};

impl Lifecycle {
    pub(super) async fn probe(&self) -> Result<Probe> {
        let config = &self.0.config;
        Probe::open(&config.head_socket, &config.head_session, PROBE_TIMEOUT).await
    }

    pub(super) async fn authorize(&self, pid: u32) -> Result<()> {
        let actual = self.probe().await?.head;
        let state = self.state()?;
        let owns = match &state.head {
            Some(known) => actual.same(known),
            None => state.launching_owner == Some(actual.owner_pid),
        };
        ensure!(
            actual.pid == pid && owns,
            EffectError::rejected("Gateway client is not the supervised native head")
        );
        Ok(())
    }

    pub(super) async fn ensure_head(&self) -> Result<()> {
        let _supervising = self.0.supervision.lock().await;
        let head = {
            let state = self.state()?;
            if state.phase != Phase::Running || *self.0.closing.borrow() {
                return Ok(());
            }
            state.head.clone()
        };
        if let Some(known) = &head
            && alive(known.pid)?
        {
            let identified = self.probe().await.and_then(|probe| {
                ensure!(probe.head.same(known), "Known native head identity changed");
                Ok(())
            });
            // A vanished process is ordinary recovery; a live PID with an
            // unverifiable owner/session/inode cannot authorize a second HEAD.
            if identified.is_ok() {
                self.state()?.head_error = None;
                return Ok(());
            }
            if alive(known.pid)? {
                return identified;
            }
        }
        let config = &self.0.config;
        remove_dead_socket(
            &config.head_socket,
            head.as_ref().map(Head::endpoint),
            head.as_ref().map(|head| head.owner_pid),
        )
        .await?;
        {
            let mut state = self.state()?;
            self.0.store.set_meta(HEAD, "null")?;
            state.head = None;
            if state.phase != Phase::Running || *self.0.closing.borrow() {
                return Ok(());
            }
        }
        let ulw = self
            .0
            .ulw
            .get()
            .context("Gateway lifecycle has not been prepared")?;
        let prepared = PreparedHost::prepare(config, &self.0.store, ulw).await?;
        let mut owner = {
            let mut state = self.state()?;
            if state.phase != Phase::Running || *self.0.closing.borrow() {
                return Ok(());
            }
            let owner = prepared.spawn(config)?;
            state.launching_owner = Some(owner.pid);
            owner
        };
        let result = owner.identify(config).await;
        match result {
            Ok(identity) => {
                // Start the pipe-owner wait before the store commit so even a
                // failed write retains its manifest until that exact process exits.
                owner.reap();
                let mut state = self.state()?;
                state.launching_owner = None;
                state.head = Some(identity);
                self.0
                    .store
                    .set_meta(HEAD, &serde_json::to_string(&state.head)?)?;
                state.head_error = None;
                Ok(())
            }
            Err(error) => {
                self.state()?.launching_owner = None;
                if owner.abandon().await.is_err() {
                    eprintln!("Failed HEAD startup owner could not be disposed");
                }
                Err(error)
            }
        }
    }

    pub async fn stop_head(&self) -> Result<()> {
        if *self.0.closing.borrow() {
            return Ok(());
        }
        let generation = {
            let mut state = self.state()?;
            let store = &self.0.store;
            store.set_meta(STOP, &Uuid::new_v4().to_string())?;
            state.stop_generation = state
                .stop_generation
                .checked_add(1)
                .context("Operator stop generation exhausted")?;
            state.phase = Phase::OperatorStopping;
            if let Some(mut record) = restart_record::active_record(store)?
                && (record.gateway_pid == process::id()
                    || record.successor_pid == Some(process::id()))
                && record.state.pending()
            {
                record.state = Stage::Refused;
                record.reason = Some(
                    "Explicit operator stop superseded the restart; no successor is authorized."
                        .into(),
                );
                restart_record::save(store, &record)?;
                store.set_meta(ACTIVE, "")?;
            }
            state.stop_generation
        };
        let result = self.shutdown_owned_head(None).await;
        if result.as_ref().is_err_and(is_refusal) {
            let mut state = self.state()?;
            if state.stop_generation == generation && state.phase == Phase::OperatorStopping {
                state.phase = if restart_record::active_record(&self.0.store)?
                    .is_some_and(|record| record.state == Stage::Uncertain)
                {
                    Phase::Held
                } else {
                    Phase::Running
                };
            }
        }
        result
    }

    pub(super) async fn shutdown_owned_head(
        &self,
        restart: Option<(&Head, &Record)>,
    ) -> Result<()> {
        let (attempt, pending) = {
            let state = self.state()?;
            (
                state.shutdown_attempt,
                state.shutdown_attempt > 0 && state.shutdown_result.is_none(),
            )
        };
        let _shutdown = self.0.shutdown_head.lock().await;
        let _supervising = self.0.supervision.lock().await;
        if *self.0.closing.borrow() {
            return Ok(());
        }
        let head = {
            let state = self.state()?;
            // A simultaneous operator-stop shares the exact native shutdown's
            // outcome, including uncertainty. It cannot send the request again.
            if pending || state.shutdown_attempt > attempt {
                return state
                    .shutdown_result
                    .as_ref()
                    .context("Owned shutdown outcome is unresolved")?
                    .result();
            }
            state.head.clone()
        };
        if let Some((expected, record)) = restart {
            if !self.restart_still_authorized(expected, record)? {
                return Ok(());
            }
            ensure!(
                head.as_ref().is_some_and(|head| head.same(expected)),
                EffectError::rejected(
                    "Native HEAD ownership changed; restart was refused without a shutdown"
                )
            );
        }
        if let Some(head) = head {
            let socket = &self.0.config.head_socket;
            if alive(head.pid)? {
                let mut probe = self.probe().await?;
                ensure!(
                    probe.head.same(&head),
                    EffectError::rejected("Refusing to stop an unowned native host")
                );
                if let Some((expected, record)) = restart
                    && !self.restart_still_authorized(expected, record)?
                {
                    return Ok(());
                }
                {
                    let mut state = self.state()?;
                    state.shutdown_attempt = state
                        .shutdown_attempt
                        .checked_add(1)
                        .context("Shutdown attempt exhausted")?;
                    state.shutdown_result = None;
                }
                let received = async {
                    if let Err(error) = probe.shutdown().await {
                        if let Some(refused) = error.downcast_ref::<ShutdownRefused>() {
                            return Err(EffectError::rejected(&refused.0).into());
                        }
                        // A lost shutdown response is not permission to kill, retry
                        // or resume supervision. The Gateway remains held.
                        return Err(error);
                    }
                    drop(probe);
                    confirmed_exit(socket, &head).await?;
                    self.clear_head()
                }
                .await;
                self.state()?.shutdown_result = Some(ShutdownOutcome::from_result(&received));
                return received;
            } else if restart.is_some() {
                // Restart cannot treat an unrelated crash as a clean shutdown.
                confirmed_exit(socket, &head).await?;
            } else {
                // A normal operator stop may retire a known already-dead HEAD,
                // but must first eliminate its exact socket and wait out the owner.
                remove_dead_socket(socket, Some(head.endpoint()), Some(head.owner_pid)).await?;
            }
        }
        self.clear_head()
    }

    fn clear_head(&self) -> Result<()> {
        let mut state = self.state()?;
        self.0.store.set_meta(HEAD, "null")?;
        state.head = None;
        Ok(())
    }

    fn restart_still_authorized(&self, head: &Head, record: &Record) -> Result<bool> {
        let state = self.state()?;
        if state.phase != Phase::Accepted || !self.is_accepted(&record.request.request_id)? {
            return Ok(false);
        }
        let store = &self.0.store;
        let fence: Option<Fence> = restart_record::meta(store.get_meta(FENCE)?)?;
        let stop = store.get_meta(STOP)?;
        ensure!(
            fence.is_some_and(
                |fence| fence.matches(&record.request, head) && fence.stop_boundary == stop
            ),
            EffectError::rejected("HEAD restart fence changed before native shutdown")
        );
        let hash = self.0.config.config_hash()?;
        ensure!(
            self.0.config_hash.get() == Some(&hash),
            EffectError::rejected("Gateway configuration changed before native shutdown")
        );
        Ok(true)
    }
}

pub(super) fn is_refusal(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<EffectError>()
        .is_some_and(|error| error.certainty == "rejected")
}

pub(super) enum ShutdownOutcome {
    Confirmed,
    Refused(String),
    Uncertain,
}

impl ShutdownOutcome {
    fn from_result(result: &Result<()>) -> Self {
        match result {
            Ok(()) => Self::Confirmed,
            Err(error) if is_refusal(error) => Self::Refused(error.to_string()),
            Err(_) => Self::Uncertain,
        }
    }

    fn result(&self) -> Result<()> {
        match self {
            Self::Confirmed => Ok(()),
            Self::Refused(error) => Err(EffectError::rejected(error).into()),
            Self::Uncertain => {
                Err(EffectError::uncertain("Owned shutdown outcome is unresolved").into())
            }
        }
    }
}
