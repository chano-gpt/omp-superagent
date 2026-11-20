//! Offline driver for the production Unix HTTP server, ingress and effect ledgers.
//! The only replacement boundaries are the supervised peer's PID and the remote adapter.
use std::{
    future::pending,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context as _, Result, bail, ensure};
use async_trait::async_trait;
use omp_gateway::{
    adapter::{self, Adapter, Context, EffectError},
    server::{Hub, Server, Service},
    store::Store,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};

#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "kebab-case")]
enum Command {
    Admit { event: Value },
    HasAnswer { id: String, route: Value },
    Stop,
}

fn announce(value: &Value) -> Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}

struct OfflineService {
    store: Arc<Store>,
    head_pid: u32,
    socket: PathBuf,
}

#[async_trait]
impl Service for OfflineService {
    fn status(&self, connected: bool, head_ready: bool, admission_ready: bool) -> Result<Value> {
        Ok(json!({
            "gateway": {
                "pid": std::process::id(),
                "ready": true,
                "admissionReady": admission_ready,
                "pendingEvents": self.store.pending()?,
                "stopping": false,
            },
            "head": {
                "pid": self.head_pid,
                "connected": connected,
                "ready": head_ready,
                "socketPath": self.socket,
                "error": null,
            },
        }))
    }

    async fn authorize_head(&self, pid: u32) -> Result<()> {
        if pid != self.head_pid {
            return Err(EffectError::rejected("Fixture head identity rejected").into());
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        bail!("Unexpected fixture operation: stop")
    }

    fn finish_stop(&self) {}

    async fn restart(&self, _request: Value) -> Result<Value, EffectError> {
        Err(EffectError::rejected(
            "Unexpected fixture operation: restart",
        ))
    }

    fn run_restart(&self, _request_id: &str) -> Result<()> {
        bail!("Unexpected fixture operation: run_restart")
    }
}

struct OfflineAdapter {
    root: PathBuf,
    journal: Mutex<()>,
}

impl OfflineAdapter {
    async fn append(&self, name: &str, value: &Value) -> Result<()> {
        let mut row = serde_json::to_vec(value)?;
        row.push(b'\n');
        let _append = self.journal.lock().await;
        let mut journal = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(self.root.join(name))
            .await?;
        journal.write_all(&row).await?;
        journal.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl Adapter for OfflineAdapter {
    async fn start(&self) -> Result<()> {
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    async fn effect(&self, operation: &str, args: &[Value]) -> Result<Value, EffectError> {
        let fixture_error = |error: anyhow::Error| EffectError::uncertain(error.to_string());
        let argument = args
            .first()
            .ok_or_else(|| EffectError::rejected("Missing fixture argument"))?;
        match operation {
            "assertRoute" => {
                if argument["accountId"] != "B1"
                    || argument["authorityId"] != "T1"
                    || argument["channelId"] != "D1"
                {
                    return Err(EffectError::rejected("Fixture route identity rejected"));
                }
                Ok(Value::Null)
            }
            "send" => {
                let id = argument["id"]
                    .as_str()
                    .ok_or_else(|| EffectError::rejected("Missing fixture output ID"))?;
                let remote_id = format!("remote:{id}");
                self.append(
                    "remote.jsonl",
                    &json!({
                        "remoteId": remote_id,
                        "sourceId": argument["sourceId"],
                        "route": argument["route"],
                        "text": argument["text"],
                    }),
                )
                .await
                .map_err(fixture_error)?;
                if id == "lost" {
                    // Let the driver witness the remote mutation before killing
                    // this fixture's exact child, without an adapter result.
                    announce(&json!({"remoteCommitted": remote_id})).map_err(fixture_error)?;
                    return pending().await;
                }
                Ok(json!(remote_id))
            }
            "setActivity" => {
                self.append(
                    "activity.jsonl",
                    &json!({
                        "route": argument["route"],
                        "messageId": argument["messageId"],
                        "activity": args.get(1),
                    }),
                )
                .await
                .map_err(fixture_error)?;
                Ok(Value::Null)
            }
            _ => Err(EffectError::rejected("Unexpected fixture operation")),
        }
    }

    async fn attachment(
        &self,
        _route: &Value,
        _file: &Value,
    ) -> Result<reqwest::Response, EffectError> {
        Err(EffectError::rejected(
            "Unexpected fixture operation: attachment",
        ))
    }
}

async fn drive(ingress: &Context) -> Result<()> {
    announce(&json!({"started": true}))?;
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = input.next_line().await? {
        match serde_json::from_str::<Command>(&line)? {
            Command::Stop => return Ok(()),
            Command::HasAnswer { id, route } => {
                announce(&json!({"pending": ingress.has_pending_answer(&id, &route)?}))?;
            }
            Command::Admit { event } => {
                announce(&json!({"waiting": true}))?;
                ingress.ready().await?;
                let admitted = match ingress.admit(event) {
                    Ok(_) => json!({"admitted": true}),
                    Err(error) if error.to_string() == "Gateway ingress identity conflict" => {
                        json!({"admitted": false, "conflict": true})
                    }
                    Err(error) => return Err(error),
                };
                announce(&admitted)?;
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().context("missing fixture state directory")?);
    let socket = PathBuf::from(args.next().context("missing fixture socket path")?);
    ensure!(
        socket.parent() == Some(root.as_path()),
        "socket escaped fixture directory"
    );
    let head = args.next().context("missing fixture head PID")?;
    let head_pid = head.to_str().context("invalid fixture head PID")?.parse()?;
    let token = std::env::var("FIXTURE_GATEWAY_TOKEN").context("missing fixture authorization")?;
    let store = Arc::new(Store::open(&root)?);
    store.set_meta("gateway:token", &token)?;
    let mut ingress = None;
    let adapter =
        adapter::Hub::with_factory(&json!({"slack": {}}), Arc::clone(&store), |_, _, ctx| {
            ingress = Some(ctx);
            Ok(Box::new(OfflineAdapter {
                root: root.clone(),
                journal: Mutex::new(()),
            }))
        })?;
    let ingress = ingress.context("fixture adapter was not configured")?;
    let service = Arc::new(OfflineService {
        store: Arc::clone(&store),
        head_pid,
        socket: socket.clone(),
    });
    let hub = Arc::new(Hub::new(store, service, Arc::clone(&adapter)));
    let server = Server::listen(&socket, hub).await?;
    adapter.start().await?;
    let result = drive(&ingress).await;
    let server_result = server.close().await;
    let adapter_result = adapter.stop().await;
    result?;
    server_result?;
    adapter_result
}
