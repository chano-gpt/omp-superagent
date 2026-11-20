use std::{
    fs::{self, DirBuilder, OpenOptions},
    future::Future,
    io::{ErrorKind, Write as _},
    os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _},
    path::PathBuf,
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
    net::{UnixListener, UnixStream},
    time::timeout,
};

use super::InventoryConfig;

const TOKEN: &str = "isolated-daemon-token";
const PROJECT: &str = "/exact/project";

struct Fixture {
    root: PathBuf,
    config: InventoryConfig,
}

impl Fixture {
    fn new() -> Self {
        let nonce = uuid::Uuid::new_v4().as_u128() & 0xffff_ffff_ffff;
        let root = std::env::temp_dir().join(format!("sa-i-{nonce:x}"));
        DirBuilder::new().mode(0o700).create(&root).unwrap();
        let token_path = root.join("token");
        let mut token = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&token_path)
            .unwrap();
        writeln!(token, " {TOKEN} ").unwrap();
        let config = serde_json::from_value(json!({
            "backend": "daemon",
            "endpoint": root.join("s"),
            "tokenPath": token_path,
            "lockPath": root.join("lock"),
            "dbPath": root.join("store"),
            "uid": token.metadata().unwrap().uid(),
            "version": 1,
            "clientBuild": "sdk-fixture",
            "clientExe": ["/fixture/bun", "/fixture/omp.ts"],
        }))
        .unwrap();
        Self { root, config }
    }

    fn listen(&self) -> UnixListener {
        UnixListener::bind(self.root.join("s")).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.root) {
            eprintln!("Could not remove isolated inventory fixture: {error}");
        }
    }
}

async fn bounded<T>(scenario: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(3), scenario)
        .await
        .expect("inventory fixture stalled")
}

async fn read_frame(peer: &mut BufReader<UnixStream>) -> Value {
    let mut line = String::new();
    peer.read_line(&mut line).await.unwrap();
    assert!(line.ends_with('\n'), "expected LF-terminated JSON");
    serde_json::from_str(&line).unwrap()
}

async fn welcome_peer(listener: &UnixListener) -> BufReader<UnixStream> {
    let (peer, _) = listener.accept().await.unwrap();
    let mut peer = BufReader::new(peer);
    assert_eq!(
        read_frame(&mut peer).await,
        json!({
            "v": 1,
            "token": TOKEN,
            "client": { "pid": std::process::id(), "build": "sdk-fixture", "exe": ["/fixture/bun", "/fixture/omp.ts"] }
        })
    );
    peer
}

fn welcome(version: u32) -> Value {
    json!({"v": version, "daemon": {"pid": std::process::id(), "build": "daemon-fixture", "startedAt": 1},
        "caps": {"lsp": 1}})
}

async fn send_frame(peer: &mut BufReader<UnixStream>, frame: &Value) {
    peer.write_all(format!("{frame}\n").as_bytes())
        .await
        .unwrap();
}

#[tokio::test]
async fn scoped_inventory_authenticates_and_projects_a_fresh_board() {
    let fixture = Fixture::new();
    let listener = fixture.listen();
    let service = json!({
        "id": "service-id", "key": "service", "owner": "session-id", "generation": "first",
        "startedAt": 42, "restartCount": 0, "state": "running", "pids": [123], "supervised": true
    });
    let mut snapshot = service.clone();
    snapshot["kind"] = json!("proc");
    snapshot["scope"] = json!({"kind":"project", "dir": PROJECT});
    snapshot["command"] = json!("not lifecycle evidence");
    let mut other_project = snapshot.clone();
    other_project["id"] = json!("other-project");
    other_project["scope"]["dir"] = json!("/other/project");
    let mut other_kind = snapshot.clone();
    other_kind["id"] = json!("other-kind");
    other_kind["kind"] = json!("lsp");
    let mut other_key = snapshot.clone();
    other_key["id"] = json!("other-key");
    other_key["key"] = json!("unrelated");
    let serve = async {
        let mut previous_id = None;
        for include_service in [true, false] {
            let mut peer = welcome_peer(&listener).await;
            send_frame(&mut peer, &welcome(1)).await;
            let request = read_frame(&mut peer).await;
            let id = request["id"].as_str().unwrap();
            assert_ne!(previous_id.as_deref(), Some(id));
            previous_id = Some(id.to_owned());
            assert_eq!(
                request,
                json!({
                    "id": id, "op": "list", "kind": "proc", "scope": {"kind": "project", "dir": PROJECT}
                })
            );
            let mut units = vec![other_project.clone(), other_kind.clone(), other_key.clone()];
            if include_service {
                units.push(snapshot.clone());
            }
            send_frame(
                &mut peer,
                &json!({"id": id, "ok": true, "result": {"units": units}}),
            )
            .await;
        }
    };
    let observe = async {
        for units in [vec![service], vec![]] {
            assert_eq!(
                fixture.config.probe(Some(PROJECT), Some("service")).await,
                Some(json!({"units": units}))
            );
        }
    };
    bounded(async { tokio::join!(serve, observe) }).await;
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    RejectedHello,
    ProtocolVersion,
    FailedList,
    WrongCorrelation,
    MissingTerminator,
    OversizedList,
}

#[tokio::test]
async fn unconfirmed_wire_replies_defer_to_sdk_instead_of_reporting_empty() {
    for fault in [
        Fault::RejectedHello,
        Fault::ProtocolVersion,
        Fault::FailedList,
        Fault::WrongCorrelation,
        Fault::MissingTerminator,
        Fault::OversizedList,
    ] {
        let fixture = Fixture::new();
        let listener = fixture.listen();
        let serve =
            async {
                let mut peer = welcome_peer(&listener).await;
                match fault {
                    Fault::RejectedHello | Fault::ProtocolVersion => {
                        let reply = if matches!(fault, Fault::RejectedHello) {
                            json!({"error": "Daemon authentication failed"})
                        } else {
                            welcome(2)
                        };
                        send_frame(&mut peer, &reply).await;
                        let mut unexpected = String::new();
                        assert_eq!(
                            peer.read_line(&mut unexpected).await.unwrap(),
                            0,
                            "must not send a list request before a valid welcome"
                        );
                    }
                    Fault::FailedList
                    | Fault::WrongCorrelation
                    | Fault::MissingTerminator
                    | Fault::OversizedList => {
                        send_frame(&mut peer, &welcome(1)).await;
                        let request = read_frame(&mut peer).await;
                        let id = request["id"].as_str().unwrap();
                        let success = json!({"id": id, "ok": true, "result": {"units": []}});
                        match fault {
                            Fault::FailedList => send_frame(
                                &mut peer,
                                &json!({"id": id, "ok": false, "error": "Daemon is restarting"}),
                            )
                            .await,
                            Fault::WrongCorrelation => {
                                send_frame(
                                    &mut peer,
                                    &json!({"id": "wrong-id", "ok": true, "result": {"units": []}}),
                                )
                                .await
                            }
                            Fault::MissingTerminator => peer
                                .write_all(success.to_string().as_bytes())
                                .await
                                .unwrap(),
                            Fault::OversizedList => {
                                let line = format!("{}{}\n", success, " ".repeat(1024 * 1024));
                                if let Err(error) = peer.write_all(line.as_bytes()).await {
                                    assert!(
                                        matches!(
                                            error.kind(),
                                            ErrorKind::BrokenPipe | ErrorKind::ConnectionReset
                                        ),
                                        "{error}"
                                    );
                                }
                            }
                            Fault::RejectedHello | Fault::ProtocolVersion => unreachable!(),
                        }
                    }
                }
            };
        let (_, observed) =
            bounded(async { tokio::join!(serve, fixture.config.probe(None, None)) }).await;
        assert_eq!(observed, None, "{fault:?}");
    }
}

#[tokio::test]
async fn daemon_absence_requires_a_supported_lock_proof_and_no_store_or_endpoint() {
    let fixture = Fixture::new();
    let expected = cfg!(target_os = "macos").then(|| json!({"units": []}));
    assert_eq!(fixture.config.probe(None, None).await, expected);
    for name in ["store", "lock", "s"] {
        let path = fixture.root.join(name);
        fs::write(&path, "").unwrap();
        assert_eq!(
            fixture.config.probe(None, None).await,
            None,
            "{name} is present"
        );
        fs::remove_file(path).unwrap();
    }
}

#[tokio::test]
async fn broker_absence_proves_only_an_unscoped_inventory_with_no_roots() {
    let fixture = Fixture::new();
    let project_root = fixture.root.join("projects");
    let global_root = fixture.root.join("global");
    let config: InventoryConfig = serde_json::from_value(json!({
        "backend": "broker", "projectRoot": project_root, "globalRoot": global_root
    }))
    .unwrap();
    assert_eq!(config.probe(None, None).await, Some(json!({"units": []})));
    assert_eq!(config.probe(Some(PROJECT), None).await, None);
    for root in [project_root, global_root] {
        fs::create_dir(&root).unwrap();
        assert_eq!(config.probe(None, None).await, None);
        fs::remove_dir(root).unwrap();
    }
}
