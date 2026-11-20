use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fs,
    os::unix::ffi::OsStrExt,
    path::{Component, Path, PathBuf},
};

use super::fs::private_directory;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Normalized {
    path: PathBuf,
    state_dir: PathBuf,
    head_cwd: PathBuf,
    command: Vec<String>,
    max_hosts: u64,
    max_subagents: u64,
    model: Option<String>,
    discord: Option<Discord>,
    telegram: Option<Token>,
    slack: Option<Slack>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Token {
    token_env: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Discord {
    token_env: String,
    voice: Option<Voice>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Voice {
    api_key_env: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Slack {
    bot_token_env: String,
    app_token_env: String,
}

pub struct LifecycleConfig {
    pub path: PathBuf,
    pub state_dir: PathBuf,
    pub(super) cwd: PathBuf,
    pub(super) command: Vec<String>,
    pub(super) model: Option<String>,
    pub(super) concurrency: u64,
    pub(super) bun: PathBuf,
    pub(super) root: PathBuf,
    pub(super) head_socket: PathBuf,
    pub(super) head_session: PathBuf,
    pub(super) head_log: PathBuf,
    pub gateway_socket: PathBuf,
    pub(super) gateway_log: PathBuf,
    messenger_credentials: Vec<String>,
    voice_credential: Option<String>,
}

impl LifecycleConfig {
    pub fn new(config: &Value) -> Result<Self> {
        let raw =
            Normalized::deserialize(config).context("Invalid normalized Gateway configuration")?;
        let bun = launcher_path("OMP_SUPERAGENT_BUN")?;
        let root = launcher_path("OMP_SUPERAGENT_ROOT")?;
        ensure!(
            raw.path.is_absolute() && raw.state_dir.is_absolute() && raw.head_cwd.is_absolute(),
            "Lifecycle paths must be absolute"
        );
        let head_socket = raw.state_dir.join("run/main.sock");
        let gateway_socket = raw.state_dir.join("run/gateway.sock");
        ensure!(
            gateway_socket.as_os_str().as_bytes().len() <= 100,
            "stateDir is too long for the Gateway socket"
        );
        let concurrency = raw
            .max_subagents
            .checked_div(raw.max_hosts)
            .filter(|value| *value > 0)
            .context("Invalid native subagent concurrency")?;
        let mut credentials = Vec::new();
        let mut voice_credential = None;
        if let Some(discord) = raw.discord {
            credentials.push(discord.token_env);
            voice_credential = discord.voice.and_then(|voice| voice.api_key_env);
        }
        if let Some(telegram) = raw.telegram {
            credentials.push(telegram.token_env);
        }
        if let Some(slack) = raw.slack {
            credentials.extend([slack.bot_token_env, slack.app_token_env]);
        }
        Ok(Self {
            path: raw.path,
            cwd: raw.head_cwd,
            command: raw.command,
            model: raw.model,
            concurrency,
            bun,
            root,
            head_socket,
            gateway_socket,
            head_session: raw.state_dir.join("sessions/main/session.jsonl"),
            head_log: raw.state_dir.join("logs/main.log"),
            gateway_log: raw.state_dir.join("logs/gateway.log"),
            state_dir: raw.state_dir,
            messenger_credentials: credentials,
            voice_credential,
        })
    }

    pub(super) fn prepare_directories(&self) -> Result<()> {
        private_directory(&self.state_dir)?;
        for directory in ["run", "logs", "sessions"] {
            private_directory(&self.state_dir.join(directory))?;
        }
        Ok(())
    }

    pub(super) fn require_credentials(&self) -> Result<()> {
        for name in &self.messenger_credentials {
            ensure!(
                env::var_os(name).is_some_and(|value| !value.is_empty()),
                "Missing messenger credential environment variable {name}"
            );
        }
        Ok(())
    }

    pub(super) fn head_environment(&self) -> BTreeMap<OsString, OsString> {
        let mut environment: BTreeMap<_, _> = env::vars_os().collect();
        for name in self
            .messenger_credentials
            .iter()
            .chain(self.voice_credential.iter())
        {
            environment.remove(&OsString::from(name));
        }
        for name in [
            "OMP_RPC_SOCKET",
            "OMP_SUPERAGENT_HOST_SOCKET",
            "OMP_SUPERAGENT_NATIVE_SOCKET",
            "OMP_SUPERAGENT_HOST_CONFIG",
            "OMP_SUPERAGENT_GATEWAY_TOKEN",
        ] {
            environment.remove(&OsString::from(name));
        }
        environment
    }

    pub(super) fn config_hash(&self) -> Result<String> {
        Ok(hex::encode(Sha256::digest(fs::read(&self.path)?)))
    }
}

pub(super) fn launcher_path(name: &str) -> Result<PathBuf> {
    let path = env::var_os(name)
        .map(PathBuf::from)
        .with_context(|| format!("Missing launcher path {name}"))?;
    ensure!(path.is_absolute(), "{name} must be an absolute path");
    Ok(path)
}

/// Node path.resolve uses lexical path equality, without following aliases.
pub(super) fn same_path(left: &Path, right: &Path) -> bool {
    fn normalize(path: &Path) -> PathBuf {
        let mut result = PathBuf::from("/");
        for part in path.components() {
            match part {
                Component::Normal(part) => result.push(part),
                Component::ParentDir => {
                    result.pop();
                }
                Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
            }
        }
        result
    }
    left.is_absolute() && right.is_absolute() && normalize(left) == normalize(right)
}
