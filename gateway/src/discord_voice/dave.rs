use anyhow::{Context as _, Result, bail, ensure};
use davey::{DAVE_PROTOCOL_VERSION, DaveSession, MediaType, ProposalsOperationType};
use serde::Deserialize;
use serde_json::json;
use std::num::NonZeroU16;

use super::super::policy::{Id, Route};
use super::{
    codec::SILENCE,
    voice_wire::{Frame, Wire},
};

#[derive(Deserialize)]
struct Transition {
    transition_id: u16,
    protocol_version: u16,
}

pub(super) struct Dave {
    route: Route,
    channel: Id,
    crypto: Option<DaveSession>,
    pending: Option<u16>,
    verified: bool,
    external_sender: Option<Vec<u8>>,
}

impl Dave {
    pub(super) async fn new(
        route: Route,
        channel: Id,
        version: u16,
        wire: &mut Wire,
    ) -> Result<Self> {
        let mut dave = Self {
            route,
            channel,
            crypto: None,
            pending: None,
            verified: false,
            external_sender: None,
        };
        if version != 0 {
            dave.initialize(version, wire).await?;
        }
        Ok(dave)
    }

    async fn initialize(&mut self, version: u16, wire: &mut Wire) -> Result<()> {
        ensure!(
            !self.verified && version == DAVE_PROTOCOL_VERSION,
            "Unsupported encrypted Discord voice version"
        );
        let version = NonZeroU16::new(version).context("Encrypted Discord voice unavailable")?;
        let mut session =
            DaveSession::new(version, self.route.account.get(), self.channel.get(), None)?;
        if let Some(external) = &self.external_sender {
            session.set_external_sender(external)?;
        }
        let package = session.create_key_package()?;
        self.crypto = Some(session);
        self.pending = None;
        wire.binary(26, &package).await
    }

    pub(super) async fn signal(&mut self, frame: Frame, wire: &mut Wire) -> Result<()> {
        match frame {
            Frame::Text(21, raw) => {
                let change: Transition = serde_json::from_value(raw)?;
                ensure!(
                    change.protocol_version == DAVE_PROTOCOL_VERSION,
                    "Discord voice requested an encryption downgrade"
                );
                self.prepare(change.transition_id, wire).await?;
            }
            Frame::Text(22, raw) => {
                #[derive(Deserialize)]
                struct Execute {
                    transition_id: u16,
                }
                let event: Execute = serde_json::from_value(raw)?;
                ensure!(
                    self.pending == Some(event.transition_id),
                    "Unexpected Discord voice DAVE execute"
                );
                let session = self
                    .crypto
                    .as_mut()
                    .context("Missing Discord DAVE session")?;
                ensure!(
                    session.execute_transition(event.transition_id)?,
                    "Discord DAVE transition was not executed"
                );
                self.pending = None;
                self.verified = true;
                self.require_pair()?;
            }
            Frame::Text(24, raw) => {
                #[derive(Deserialize)]
                struct Epoch {
                    epoch: u64,
                    protocol_version: u16,
                }
                let epoch: Epoch = serde_json::from_value(raw)?;
                ensure!(
                    epoch.protocol_version == DAVE_PROTOCOL_VERSION,
                    "Unsupported Discord voice DAVE epoch"
                );
                if epoch.epoch == 1 {
                    self.initialize(epoch.protocol_version, wire).await?;
                }
            }
            Frame::Binary(25, package) => {
                ensure!(
                    !self.verified,
                    "Discord voice changed the MLS external sender"
                );
                if let Some(session) = self.crypto.as_mut() {
                    session.set_external_sender(&package)?;
                }
                self.external_sender = Some(package.to_vec());
            }
            Frame::Binary(27, raw) => {
                let Some((op, proposals)) = raw.split_first() else {
                    bail!("Empty Discord MLS proposal");
                };
                let operation = match op {
                    0 => ProposalsOperationType::APPEND,
                    1 => ProposalsOperationType::REVOKE,
                    _ => bail!("Unknown Discord MLS proposal operation"),
                };
                let users = self.users();
                let crypto = self
                    .crypto
                    .as_mut()
                    .context("Discord MLS proposal before encrypted session")?;
                if let Some(result) =
                    crypto.process_proposals(operation, proposals, Some(&users))?
                {
                    let mut commit = result.commit;
                    if let Some(welcome) = result.welcome {
                        commit.extend(welcome);
                    }
                    wire.binary(28, &commit).await?;
                }
            }
            Frame::Binary(op @ (29 | 30), raw) => {
                let &[hi, lo, ref payload @ ..] = raw.as_ref() else {
                    bail!("Invalid Discord MLS transition");
                };
                let id = u16::from_be_bytes([hi, lo]);
                let users = self.users();
                let crypto = self
                    .crypto
                    .as_mut()
                    .context("Discord MLS transition before encrypted session")?;
                let accepted = if op == 29 {
                    crypto.process_commit(payload).map_err(anyhow::Error::new)
                } else {
                    crypto
                        .process_welcome(payload, Some(&users))
                        .map(|()| true)
                        .map_err(anyhow::Error::new)
                };
                match accepted {
                    Ok(true) => self.prepare(id, wire).await?,
                    Ok(false) => {} // Non-admitted pending members do not process this commit.
                    Err(_) => {
                        wire.json(31, json!({"transition_id":id})).await?;
                        bail!("Discord rejected the private DAVE group transition");
                    }
                }
            }
            Frame::Text(2 | 4, _) => bail!("Discord voice transport unexpectedly renegotiated"),
            Frame::Text(..) | Frame::Binary(..) | Frame::KeepAlive | Frame::Ignore => {}
        }
        Ok(())
    }

    async fn prepare(&mut self, id: u16, wire: &mut Wire) -> Result<()> {
        ensure!(
            self.pending.is_none_or(|pending| pending == id),
            "Overlapping Discord MLS transitions"
        );
        self.require_pair()?;
        let crypto = self
            .crypto
            .as_mut()
            .context("Discord MLS transition before encrypted session")?;
        crypto.prepare_transition(id, DAVE_PROTOCOL_VERSION)?;
        if id == 0 {
            self.pending = None;
            self.verified = true;
            self.require_pair()?;
        } else {
            self.pending = Some(id);
            wire.json(23, json!({"transition_id":id})).await?;
        }
        Ok(())
    }

    fn users(&self) -> [u64; 2] {
        [self.route.account.get(), self.route.owner.get()]
    }

    fn require_pair(&self) -> Result<()> {
        let crypto = self
            .crypto
            .as_ref()
            .context("Discord voice has no MLS group")?;
        let expected = self.users();
        ensure!(
            crypto
                .group()
                .is_some_and(|group| group.members().count() == 2)
                && crypto.get_user_ids().is_some_and(
                    |ids| ids.len() == 2 && expected.iter().all(|user| ids.contains(user))
                ),
            "Discord voice MLS group is not exactly the owner and the authenticated bot"
        );
        Ok(())
    }

    pub(super) fn ready(&self) -> bool {
        self.verified
            && self.pending.is_none()
            && self
                .crypto
                .as_ref()
                .is_some_and(|crypto| crypto.can_encrypt() && crypto.is_ready())
    }

    pub(super) fn decrypt(&mut self, payload: &[u8]) -> Option<Vec<u8>> {
        if !self.ready() {
            return None;
        }
        // The RFC-compatible Opus silence marker is not an owner speech frame.
        if payload == SILENCE {
            return Some(SILENCE.to_vec());
        }
        if !payload.ends_with(&[0xfa, 0xfa]) {
            return None;
        }
        self.crypto
            .as_mut()?
            .decrypt(self.route.owner.get(), MediaType::AUDIO, payload)
            .ok()
    }

    pub(super) fn encrypt<'a>(&mut self, payload: &'a [u8]) -> Result<std::borrow::Cow<'a, [u8]>> {
        ensure!(
            self.ready(),
            "Discord voice lacks a confirmed encrypted pair"
        );
        let crypto = self
            .crypto
            .as_mut()
            .context("Missing Discord DAVE session")?;
        Ok(crypto.encrypt_opus(payload)?)
    }
}

#[cfg(test)]
#[path = "dave_tests.rs"]
mod tests;
