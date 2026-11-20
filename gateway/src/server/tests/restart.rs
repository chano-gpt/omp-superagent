use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    sync::Notify,
    time::timeout,
};
use uuid::Uuid;

use super::{GatewayClient, SocketDir};
use crate::{adapter::EffectError, server::Service, store::Store};

struct RestartFixture {
    store: Arc<Store>,
    entered: Notify,
    release: Notify,
    persisted: Notify,
    executed: Notify,
    acceptances: AtomicUsize,
    executions: AtomicUsize,
}

#[async_trait]
impl Service for RestartFixture {
    fn status(&self, _: bool, _: bool, _: bool) -> Result<Value> {
        Ok(json!({"gateway":{}}))
    }
    async fn authorize_head(&self, pid: u32) -> Result<()> {
        anyhow::ensure!(pid == std::process::id(), "Unexpected controller");
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
    fn finish_stop(&self) {}
    async fn restart(&self, mut request: Value) -> Result<Value, EffectError> {
        self.acceptances.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        request["state"] = json!("accepted");
        request["gatewayPid"] = json!(std::process::id());
        request["headPid"] = json!(std::process::id());
        request["sessionId"] = json!("original-native");
        request["sessionFile"] = json!("/original-native.jsonl");
        self.store
            .set_restart(&request)
            .map_err(|_| EffectError::uncertain("Test restart persistence failed"))?;
        self.persisted.notify_one();
        Ok(request)
    }
    fn run_restart(&self, _: &str) -> Result<()> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.executed.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn lost_restart_response_preserves_acceptance_without_launch_until_receipted() -> Result<()> {
    let root = SocketDir::new()?;
    let store = Arc::new(Store::open(&root.0)?);
    let head = Connection::open(root.0.join("coordination.sqlite"))?;
    head.execute_batch(
        "CREATE TABLE inputs (id TEXT PRIMARY KEY, data TEXT NOT NULL, target TEXT, state TEXT);
         CREATE TABLE outputs (id TEXT PRIMARY KEY, source_id TEXT NOT NULL, route TEXT NOT NULL, state TEXT, remote_id TEXT);",
    )?;
    let route = json!({"platform":"telegram","accountId":"bot","authorityId":"owner",
        "guildId":null,"channelId":"chat","threadId":null});
    head.execute(
        "INSERT INTO inputs VALUES('source',?,'main','done')",
        [json!({"id":"source","text":"/restart","route":route}).to_string()],
    )?;
    head.execute(
        "INSERT INTO outputs VALUES('ack','source',?,'sent','sent-message')",
        params![route.to_string()],
    )?;
    store.set_meta("user-source:source", "true")?;
    let service = Arc::new(RestartFixture {
        store: store.clone(),
        entered: Notify::new(),
        release: Notify::new(),
        persisted: Notify::new(),
        executed: Notify::new(),
        acceptances: AtomicUsize::new(0),
        executions: AtomicUsize::new(0),
    });
    let fixture =
        GatewayClient::with_service(&root, String::new(), store.clone(), service.clone()).await?;
    assert_eq!(fixture.call("/ready", &json!({})).await?["ok"], true);
    let id = Uuid::new_v4().to_string();
    let request = json!({"requestId":id,"inputId":"source","ackOutputId":"ack"});
    let mut disconnected = UnixStream::connect(root.0.join("g.sock")).await?;
    let serialized = request.to_string();
    let sent = format!(
        "POST /restart HTTP/1.1\r\nHost: localhost\r\nAuthorization: {}\r\nX-Gateway-Client: {}\r\nContent-Length: {}\r\n\r\n{}",
        fixture
            .headers
            .get(reqwest::header::AUTHORIZATION)
            .context("missing test authorization")?
            .to_str()?,
        fixture
            .headers
            .get("x-gateway-client")
            .context("missing test controller")?
            .to_str()?,
        serialized.len(),
        serialized,
    );
    disconnected.write_all(sent.as_bytes()).await?;
    timeout(Duration::from_secs(3), service.entered.notified()).await?;
    disconnected.shutdown().await?;
    // Observe the real HTTP server abandon its connection before allowing
    // the retained lifecycle transaction to complete.
    let mut byte = [0; 1];
    assert_eq!(
        timeout(Duration::from_secs(3), disconnected.read(&mut byte)).await??,
        0
    );
    drop(disconnected);
    service.release.notify_one();
    timeout(Duration::from_secs(3), service.persisted.notified()).await?;
    let persisted = store
        .get_restart(Some(&id))?
        .context("Acceptance vanished after peer disconnect")?;
    assert_eq!(persisted["state"], "accepted");
    assert_eq!(service.executions.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.call("/restart", &request).await?["value"],
        persisted
    );
    timeout(Duration::from_secs(3), service.executed.notified()).await?;
    fixture.server.close().await?;
    assert_eq!(service.acceptances.load(Ordering::SeqCst), 1);
    assert_eq!(service.executions.load(Ordering::SeqCst), 1);
    Ok(())
}
