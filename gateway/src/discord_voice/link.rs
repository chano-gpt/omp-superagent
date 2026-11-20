#[path = "codec.rs"]
mod codec;
#[path = "dave.rs"]
mod dave;
#[path = "rtp.rs"]
mod rtp;
#[path = "voice_wire.rs"]
mod voice_wire;

use anyhow::{Context as _, Result, bail, ensure};
use serde_json::json;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    net::UdpSocket,
    time::{Instant, sleep_until, timeout},
};

use self::{
    codec::Audio,
    dave::Dave,
    rtp::{Mode, Rtp},
    voice_wire::{Description, Frame, Ready, Wire},
};
use super::policy::{Id, Route};

const UDP_KEEPALIVE: Duration = Duration::from_secs(5);
const UDP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_DATAGRAM: usize = 2_048;

pub(super) struct Join {
    pub(super) endpoint: String,
    pub(super) session_id: String,
    pub(super) token: String,
}

pub(super) struct Link {
    wire: Wire,
    dave: Dave,
    socket: UdpSocket,
    rtp: Rtp,
    codec: Audio,
    ssrc: u32,
    speaking: bool,
    keepalive: Instant,
    ping: u32,
    input: [u8; MAX_DATAGRAM + 1],
    failures: u8,
}

pub(super) struct Transfer(Transport);

enum Transport {
    Signal(Frame),
    Packet(usize),
    KeepAlive,
}

impl Link {
    pub(super) async fn open(route: Route, channel: Id, join: Join) -> Result<Self> {
        let mut wire = Wire::connect(route, join).await?;
        let ready: Ready = loop {
            let frame = wire.recv().await?;
            match wire.control(frame).await? {
                Frame::Text(2, data) => break serde_json::from_value(data)?,
                Frame::Text(4, _) | Frame::Binary(..) => {
                    bail!("Discord voice handshake out of order")
                }
                _ => {}
            }
        };
        let mode = Mode::choose(&ready.modes)?;
        ensure!(ready.port != 0, "Discord voice server omitted its UDP port");
        let (socket, external) = timeout(
            Duration::from_secs(5),
            rtp::discover(SocketAddr::new(ready.ip, ready.port), ready.ssrc),
        )
        .await??;
        wire.json(
            1,
            json!({"protocol":"udp", "data":{
                "address":external.ip().to_string(), "port":external.port(), "mode":mode.name()
            }}),
        )
        .await?;
        let description: Description = loop {
            let frame = wire.recv().await?;
            match wire.control(frame).await? {
                Frame::Text(4, data) => break serde_json::from_value(data)?,
                Frame::Text(2, _) | Frame::Binary(..) => {
                    bail!("Discord voice handshake out of order")
                }
                _ => {}
            }
        };
        ensure!(
            description.mode == mode.name(),
            "Discord voice transport cipher changed"
        );
        let rtp = Rtp::new(ready.ssrc, mode, description.secret_key);
        let mut dave =
            Dave::new(route, channel, description.dave_protocol_version, &mut wire).await?;
        while !dave.ready() {
            let frame = wire.recv().await?;
            dave.signal(wire.control(frame).await?, &mut wire).await?;
        }
        Ok(Self {
            wire,
            dave,
            socket,
            rtp,
            codec: Audio::new()?,
            ssrc: ready.ssrc,
            speaking: false,
            keepalive: Instant::now(),
            ping: 0,
            input: [0; MAX_DATAGRAM + 1],
            failures: 0,
        })
    }

    pub(super) async fn poll(&mut self) -> Result<Transfer> {
        let event = tokio::select! {
            biased;
            frame = self.wire.recv() => Transport::Signal(frame?),
            _ = sleep_until(self.keepalive) => Transport::KeepAlive,
            count = self.socket.recv(&mut self.input) => Transport::Packet(count.context("Discord voice UDP receive failed")?),
        };
        Ok(Transfer(event))
    }

    pub(super) async fn handle(&mut self, Transfer(transfer): Transfer) -> Result<Option<Vec<u8>>> {
        match transfer {
            Transport::Signal(frame) => {
                let frame = self.wire.control(frame).await?;
                self.dave.signal(frame, &mut self.wire).await?;
            }
            Transport::KeepAlive => {
                let mut datagram = [0; 8];
                datagram[..4].copy_from_slice(&self.ping.to_le_bytes());
                self.send(&datagram).await?;
                self.ping = self.ping.wrapping_add(1);
                self.keepalive = Instant::now() + UDP_KEEPALIVE;
            }
            Transport::Packet(count) => {
                if count > MAX_DATAGRAM || !self.dave.ready() {
                    return Ok(None);
                }
                if let Some(ssrc) = self.wire.owner_ssrc
                    && let Some(payload) = self.rtp.open(&self.input[..count], ssrc)
                {
                    if let Some(clear) = self.dave.decrypt(&payload) {
                        self.failures = 0;
                        return self.codec.decode(&clear);
                    }
                    self.failures = self.failures.saturating_add(1);
                    ensure!(
                        self.failures < 36,
                        "Discord voice lost the authenticated DAVE media stream"
                    );
                }
            }
        }
        Ok(None)
    }

    pub(super) fn play(&mut self, pcm: &[u8]) -> Result<()> {
        self.codec.push(pcm)
    }

    pub(super) fn encrypted(&self) -> bool {
        self.dave.ready()
    }

    pub(super) async fn tick(&mut self) -> Result<()> {
        if !self.dave.ready() {
            return Ok(());
        }
        let mut opus = [0; MAX_DATAGRAM];
        let Some((size, still_speaking)) = self.codec.playback(&mut opus)? else {
            return Ok(());
        };
        if !self.speaking {
            self.speaking(true).await?;
        }
        let encrypted = self.dave.encrypt(&opus[..size])?;
        let packet = self.rtp.seal(&encrypted)?;
        self.send(&packet).await?;
        if !still_speaking {
            self.speaking(false).await?;
        }
        Ok(())
    }

    async fn speaking(&mut self, speaking: bool) -> Result<()> {
        self.wire
            .json(
                5,
                json!({"speaking":u8::from(speaking), "delay":0, "ssrc":self.ssrc}),
            )
            .await?;
        self.speaking = speaking;
        Ok(())
    }

    async fn send(&mut self, packet: &[u8]) -> Result<()> {
        let bytes = timeout(UDP_TIMEOUT, self.socket.send(packet)).await??;
        ensure!(bytes == packet.len(), "Incomplete Discord voice UDP write");
        Ok(())
    }
}
