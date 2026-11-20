use std::{
    collections::HashMap,
    io::ErrorKind,
    os::unix::fs::{FileTypeExt as _, MetadataExt as _},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::{
    fs,
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::UnixStream,
    time::timeout,
};

use crate::transport::new_id;

const SUPPORTED_VERSION: u32 = 1;
const TOKEN_BYTES: u64 = 4096;
const REQUEST_BYTES: usize = 8 * 1024;
const WELCOME_BYTES: u64 = 8 * 1024;
const LIST_BYTES: u64 = 1024 * 1024;
const PROBE_TIMEOUT: Duration = Duration::from_millis(250);

/// Static SDK metadata, never an inventory result. Omitted for unsupported endpoints.
#[derive(Deserialize)]
#[serde(
    tag = "backend",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum InventoryConfig {
    Daemon(LocalDaemon),
    Broker {
        project_root: PathBuf,
        global_root: PathBuf,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LocalDaemon {
    endpoint: PathBuf,
    token_path: PathBuf,
    lock_path: PathBuf,
    db_path: PathBuf,
    uid: u32,
    version: u32,
    client_build: String,
    client_exe: Vec<String>,
}

impl InventoryConfig {
    /// Read the current board, or defer to the SDK when it cannot be established here.
    pub(crate) async fn probe(
        &self,
        project_dir: Option<&str>,
        service_name: Option<&str>,
    ) -> Option<Value> {
        let attempt = async {
            match self {
                Self::Daemon(daemon) => daemon.probe(project_dir, service_name).await,
                Self::Broker {
                    project_root,
                    global_root,
                } => {
                    // Unscoped broker inventory enumerates exactly these two roots.
                    // Scoped reads may still have a live endpoint outside their runtime dir.
                    if project_dir.is_some() {
                        return None;
                    }
                    let (projects_missing, globals_missing) =
                        tokio::join!(absent(project_root), absent(global_root));
                    (projects_missing && globals_missing).then(|| json!({"units": []}))
                }
            }
        };
        timeout(PROBE_TIMEOUT, attempt).await.unwrap_or(None)
    }
}

impl LocalDaemon {
    async fn probe(&self, project_dir: Option<&str>, service_name: Option<&str>) -> Option<Value> {
        if self.version != SUPPORTED_VERSION {
            return None;
        }
        match fs::symlink_metadata(&self.endpoint).await {
            Ok(metadata) if metadata.file_type().is_socket() && metadata.uid() == self.uid => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                // On Darwin the SDK queries an fcntl lock on this path. Linux
                // FileLock uses a socket probe; pathname absence does not prove it free.
                return (cfg!(target_os = "macos")
                    && absent(&self.db_path).await
                    && absent(&self.lock_path).await)
                    .then(|| json!({"units": []}));
            }
            Ok(_) | Err(_) => return None,
        }

        let token = self.read_token().await?;
        let socket = UnixStream::connect(&self.endpoint).await.ok()?;
        let peer = socket.peer_cred().ok()?;
        if peer.uid() != self.uid {
            return None;
        }
        let mut socket = BufReader::new(socket);
        let hello = json!({
            "v": self.version,
            "token": token.trim(),
            "client": { "pid": std::process::id(), "build": self.client_build, "exe": self.client_exe }
        });
        write_line(&mut socket, &hello).await?;
        let welcome: Welcome = read_line(&mut socket, WELCOME_BYTES).await?;
        if welcome.v != self.version
            || welcome.daemon.build.is_empty()
            || welcome.daemon.pid == 0
            || peer
                .pid()
                .is_some_and(|pid| u32::try_from(pid) != Ok(welcome.daemon.pid))
        {
            return None;
        }

        let id = new_id();
        let mut request = json!({"id": id, "op": "list", "kind": "proc"});
        if let Some(dir) = project_dir {
            if dir.len() >= REQUEST_BYTES {
                return None;
            }
            request["scope"] = json!({"kind": "project", "dir": dir});
        }
        write_line(&mut socket, &request).await?;
        let reply: ListReply = read_line(&mut socket, LIST_BYTES).await?;
        if !reply.ok || reply.id != id {
            return None;
        }
        let mut units = reply.result.units;
        units.retain(|unit| {
            unit.kind == "proc"
                && service_name.is_none_or(|key| unit.key.as_str() == key)
                && project_dir.is_none_or(|dir| {
                    matches!(&unit.scope, Scope::Project { dir: unit_dir } if unit_dir.as_str() == dir)
                })
        });
        Some(json!({ "units": units }))
    }

    async fn read_token(&self) -> Option<String> {
        let private_file = |metadata: &std::fs::Metadata| {
            metadata.is_file()
                && metadata.uid() == self.uid
                && metadata.mode() & 0o077 == 0
                && metadata.len() <= TOKEN_BYTES
        };
        if !private_file(&fs::symlink_metadata(&self.token_path).await.ok()?) {
            return None;
        }
        let file = fs::File::open(&self.token_path).await.ok()?;
        if !private_file(&file.metadata().await.ok()?) {
            return None;
        }
        let mut token = String::new();
        let size = file
            .take(TOKEN_BYTES + 1)
            .read_to_string(&mut token)
            .await
            .ok()?;
        (u64::try_from(size).ok()? <= TOKEN_BYTES && token.is_ascii() && !token.trim().is_empty())
            .then_some(token)
    }
}

async fn absent(path: &Path) -> bool {
    matches!(fs::symlink_metadata(path).await, Err(error) if error.kind() == ErrorKind::NotFound)
}

async fn write_line(socket: &mut BufReader<UnixStream>, value: &Value) -> Option<()> {
    let mut data = serde_json::to_vec(value).ok()?;
    if data.len() >= REQUEST_BYTES {
        return None;
    }
    data.push(b'\n');
    socket.write_all(&data).await.ok()
}

async fn read_line<T: DeserializeOwned>(
    socket: &mut BufReader<UnixStream>,
    limit: u64,
) -> Option<T> {
    let mut data = Vec::new();
    socket.take(limit).read_until(b'\n', &mut data).await.ok()?;
    // `read_until` also succeeds on EOF or the limit, even with a partial line.
    if data.pop()? != b'\n' {
        return None;
    }
    serde_json::from_slice(&data).ok()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Welcome {
    v: u32,
    daemon: DaemonIdentity,
    #[serde(rename = "caps")]
    _caps: HashMap<String, u32>,
}

#[derive(Deserialize)]
struct DaemonIdentity {
    pid: u32,
    build: String,
    #[serde(rename = "startedAt")]
    _started_at: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListReply {
    id: String,
    ok: bool,
    result: ListResult,
}

#[derive(Deserialize)]
struct ListResult {
    units: Vec<ServiceUnit>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum Scope {
    Global,
    Project {
        dir: String,
    },
    Session {
        #[serde(rename = "id")]
        _id: String,
    },
}

// Same projection as listServiceUnits: no environment, command line or logs.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ServiceUnit {
    #[serde(skip_serializing)]
    kind: String,
    #[serde(skip_serializing)]
    scope: Scope,
    id: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    generation: String,
    started_at: u64,
    restart_count: u64,
    state: UnitState,
    #[serde(skip_serializing_if = "Option::is_none")]
    exited_at: Option<u64>,
    pids: Vec<u32>,
    supervised: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum UnitState {
    Starting,
    Running,
    Ready,
    Restarting,
    Stopping,
    Exited,
    Failed,
}

#[cfg(test)]
mod tests;
