use anyhow::{Context as _, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{future::pending, net::IpAddr, time::Duration};
use tokio::{
    net::TcpStream,
    time::{Instant, sleep_until, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Bytes, Message, protocol::WebSocketConfig},
};
use url::Url;

use super::super::policy::{Id, Route};
use super::Join;

const MAX_FRAME: usize = 256 * 1024;
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
pub(super) struct Ready {
    pub(super) ip: IpAddr,
    pub(super) port: u16,
    pub(super) ssrc: u32,
    pub(super) modes: Vec<String>,
}

#[derive(Deserialize)]
pub(super) struct Description {
    pub(super) mode: String,
    pub(super) secret_key: [u8; 32],
    #[serde(default)]
    pub(super) dave_protocol_version: u16,
}

pub(super) enum Frame {
    Text(u8, Value),
    Binary(u8, Bytes),
    KeepAlive,
    Ignore,
}

struct Heartbeat {
    interval: Duration,
    deadline: Instant,
    next: u64,
    awaiting: Option<u64>,
}

pub(super) struct Wire {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    seq: Option<u16>,
    heartbeat: Option<Heartbeat>,
    route: Route,
    pub(super) owner_ssrc: Option<u32>,
}

impl Wire {
    pub(super) async fn connect(route: Route, join: Join) -> Result<Self> {
        let endpoint = endpoint(&join.endpoint)?;
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_FRAME))
            .max_frame_size(Some(MAX_FRAME))
            .write_buffer_size(0)
            .max_write_buffer_size(MAX_FRAME * 2);
        let (socket, _) = connect_async_with_config(endpoint.as_str(), Some(config), true)
            .await
            .map_err(|_| anyhow::anyhow!("Discord voice WebSocket connection failed"))?;
        let mut wire = Self::from_socket(socket, route);
        wire.json(0, json!({"server_id": route.guild.to_string(), "user_id": route.account.to_string(),
            "session_id":join.session_id, "token":join.token, "max_dave_protocol_version":davey::DAVE_PROTOCOL_VERSION})).await?;
        Ok(wire)
    }

    pub(super) fn from_socket(
        socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
        route: Route,
    ) -> Self {
        Self {
            socket,
            seq: None,
            heartbeat: None,
            route,
            owner_ssrc: None,
        }
    }

    pub(super) async fn recv(&mut self) -> Result<Frame> {
        let deadline = self.heartbeat.as_ref().map(|clock| clock.deadline);
        let frame = tokio::select! {
            biased;
            frame = self.socket.next() => frame.context("Discord voice WebSocket ended")?
                .map_err(|_| anyhow::anyhow!("Discord voice WebSocket failed"))?,
            _ = wait_for(deadline) => return Ok(Frame::KeepAlive),
        };
        match frame {
            Message::Text(text) => {
                #[derive(Deserialize)]
                struct Envelope {
                    op: u8,
                    d: Value,
                    seq: Option<u16>,
                }
                let packet: Envelope = serde_json::from_str(text.as_str())
                    .map_err(|_| anyhow::anyhow!("Invalid Discord voice frame"))?;
                if packet.seq.is_some() {
                    self.seq = packet.seq;
                }
                Ok(Frame::Text(packet.op, packet.d))
            }
            Message::Binary(bytes) => {
                let &[a, b, op, ..] = bytes.as_ref() else {
                    bail!("Invalid Discord voice binary envelope");
                };
                self.seq = Some(u16::from_be_bytes([a, b]));
                Ok(Frame::Binary(op, bytes.slice(3..)))
            }
            Message::Ping(_) | Message::Pong(_) => Ok(Frame::Ignore),
            _ => bail!("Discord voice WebSocket closed"),
        }
    }

    pub(super) async fn control(&mut self, frame: Frame) -> Result<Frame> {
        match frame {
            Frame::Text(8, data) => {
                let interval = data["heartbeat_interval"]
                    .as_f64()
                    .filter(|ms| (100.0..=120_000.0).contains(ms))
                    .context("Invalid Discord voice heartbeat interval")?;
                let interval = Duration::from_secs_f64(interval / 1_000.0);
                ensure!(self.heartbeat.is_none(), "Unexpected voice heartbeat reset");
                self.heartbeat = Some(Heartbeat {
                    interval,
                    deadline: Instant::now() + interval,
                    next: 1,
                    awaiting: None,
                });
            }
            Frame::Text(6, data) => {
                if let Some(clock) = self.heartbeat.as_mut()
                    && clock.awaiting.is_some()
                    && data["t"].as_u64() == clock.awaiting
                {
                    clock.awaiting = None;
                }
            }
            Frame::KeepAlive => {
                let clock = self
                    .heartbeat
                    .as_mut()
                    .context("Discord voice heartbeat not initialized")?;
                ensure!(
                    clock.awaiting.is_none(),
                    "Discord voice heartbeat was not acknowledged"
                );
                let nonce = clock.next;
                clock.next = nonce.saturating_add(1);
                clock.awaiting = Some(nonce);
                clock.deadline = Instant::now() + clock.interval;
                self.json(
                    3,
                    json!({"t":nonce, "seq_ack":self.seq.map_or(-1, i32::from)}),
                )
                .await?;
            }
            Frame::Text(11, data) => {
                let users: Vec<Id> = serde_json::from_value(data["user_ids"].clone())?;
                ensure!(
                    users
                        .iter()
                        .all(|id| *id == self.route.account || *id == self.route.owner),
                    "Discord voice gateway announced an unexpected participant"
                );
            }
            Frame::Text(13, data) => {
                let id: Id = serde_json::from_value(data["user_id"].clone())?;
                ensure!(
                    id != self.route.owner && id != self.route.account,
                    "Discord voice participant disconnected"
                );
            }
            Frame::Text(5, data) => {
                #[derive(Deserialize)]
                struct Speaker {
                    user_id: Id,
                    ssrc: u32,
                }
                let speaker: Speaker = serde_json::from_value(data)?;
                ensure!(
                    speaker.user_id == self.route.account || speaker.user_id == self.route.owner,
                    "Discord voice gateway announced an unexpected speaker"
                );
                if speaker.user_id == self.route.owner {
                    self.owner_ssrc = Some(speaker.ssrc);
                }
            }
            Frame::Ignore => {}
            frame @ (Frame::Text(..) | Frame::Binary(..)) => return Ok(frame),
        }
        Ok(Frame::Ignore)
    }

    pub(super) async fn json(&mut self, op: u8, data: Value) -> Result<()> {
        self.send(Message::Text(json!({"op":op, "d":data}).to_string().into()))
            .await
    }

    pub(super) async fn binary(&mut self, op: u8, data: &[u8]) -> Result<()> {
        let mut bytes = Vec::with_capacity(1 + data.len());
        bytes.push(op);
        bytes.extend_from_slice(data);
        self.send(Message::Binary(bytes.into())).await
    }

    async fn send(&mut self, frame: Message) -> Result<()> {
        timeout(WRITE_TIMEOUT, self.socket.send(frame))
            .await?
            .map_err(|_| anyhow::anyhow!("Discord voice WebSocket write failed"))
    }
}

fn endpoint(advertised: &str) -> Result<Url> {
    let mut parsed = Url::parse(&format!("wss://{advertised}"))
        .map_err(|_| anyhow::anyhow!("Invalid Discord voice endpoint"))?;
    let trusted = parsed
        .host_str()
        .is_some_and(|host| host.ends_with(".discord.media") || host.ends_with(".discord.gg"));
    ensure!(
        trusted
            && parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.path() == "/"
            && parsed.query().is_none()
            && parsed.fragment().is_none(),
        "Untrusted Discord voice endpoint"
    );
    parsed.set_query(Some("v=8"));
    Ok(parsed)
}

async fn wait_for(deadline: Option<Instant>) {
    if let Some(deadline) = deadline {
        sleep_until(deadline).await;
    } else {
        pending().await
    }
}
