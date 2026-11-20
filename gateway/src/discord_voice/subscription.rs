use anyhow::{Context as _, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{collections::HashSet, path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::{Instant, timeout},
};

use super::{Event, MAX_INPUT, pcm};

const MAX_LINE: usize = 65_536;
const MAX_DRAFTS: usize = 32;
const SPEECH_GAP: Duration = Duration::from_millis(200);
const RECENT_SPEECH: Duration = Duration::from_secs(20);

pub(super) struct Subscription {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    partial: Vec<u8>,
    last_speech: Option<Instant>,
    utterance: u64,
    drafted: u64,
    seen: HashSet<String>,
    pending: Option<String>,
}

impl Subscription {
    pub(super) async fn open(voice: &str) -> Result<Self> {
        ensure!(
            matches!(
                voice,
                "arbor"
                    | "breeze"
                    | "cove"
                    | "ember"
                    | "juniper"
                    | "maple"
                    | "sol"
                    | "spruce"
                    | "vale"
            ),
            "Unsupported Codex subscription voice"
        );
        let home = absolute_env("HOME")?.context("Codex voice requires an absolute HOME")?;
        let root = absolute_env("OMP_SUPERAGENT_ROOT")?
            .context("Missing trusted Superagent installation path")?;
        let bridge = root.join("src/codex-live-bridge.py");
        let python = absolute_env("OMP_SA_CODEX_VOICE_PYTHON")?
            .unwrap_or_else(|| home.join(".local/share/omp-superagent/codex-voice/bin/python"));
        // Do not inherit any platform or API tokens, Python injection, or proxy variables.
        let mut command = Command::new(python);
        command
            .args(["-E", "-s", "-B", "-u"])
            .arg(bridge)
            .arg(voice)
            .current_dir(&home)
            .env_clear()
            .env("HOME", &home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        for key in [
            "CODEX_HOME",
            "SYSTEMROOT",
            "USERPROFILE",
            "LOCALAPPDATA",
            "TMPDIR",
            "TEMP",
            "TMP",
        ] {
            if let Some(path) = absolute_env(key)? {
                command.env(key, path);
            }
        }
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .context("Could not start the optional Codex voice bridge")?;
        let input = child
            .stdin
            .take()
            .context("Codex voice bridge stdin unavailable")?;
        let output = BufReader::new(
            child
                .stdout
                .take()
                .context("Codex voice bridge stdout unavailable")?,
        );
        let mut bridge = Self {
            child,
            input,
            output,
            partial: Vec::new(),
            last_speech: None,
            utterance: 0,
            drafted: 0,
            seen: HashSet::new(),
            pending: None,
        };
        ensure!(
            bridge.read().await?["type"] == "ready",
            "Codex subscription voice is unavailable"
        );
        Ok(bridge)
    }

    pub(super) async fn input(&mut self, pcm: &[u8]) -> Result<()> {
        if energetic(pcm) {
            let now = Instant::now();
            if self
                .last_speech
                .is_none_or(|last| now.duration_since(last) >= SPEECH_GAP)
            {
                self.utterance = self.utterance.saturating_add(1);
            }
            self.last_speech = Some(now);
        }
        self.send(json!({"type":"input", "audio":STANDARD.encode(pcm)}))
            .await
    }

    pub(super) async fn recv(&mut self) -> Result<Event> {
        let event = self.read().await?;
        match event["type"].as_str() {
            Some("audio") => {
                let audio = pcm(&event["audio"], MAX_INPUT)?;
                ensure!(!audio.is_empty(), "Empty Codex voice bridge audio");
                Ok(Event::Audio(audio))
            }
            Some("draft") => {
                let id = event["id"]
                    .as_str()
                    .filter(|id| {
                        !id.is_empty()
                            && id.len() <= 128
                            && id
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                    })
                    .context("Invalid Codex voice draft ID")?;
                let text = event["text"]
                    .as_str()
                    .filter(|text| {
                        *text == text.trim()
                            && !text.is_empty()
                            && text.encode_utf16().count() <= 1_000
                    })
                    .context("Invalid Codex voice draft text")?;
                if self.seen.contains(id) {
                    return Ok(Event::None);
                }
                ensure!(
                    self.seen.len() < MAX_DRAFTS,
                    "Codex voice draft budget reached"
                );
                self.seen.insert(id.to_owned());
                if self.pending.is_some()
                    || self.drafted == self.utterance
                    || self
                        .last_speech
                        .is_none_or(|at| at.elapsed() > RECENT_SPEECH)
                {
                    return Ok(Event::Reject(id.to_owned()));
                }
                self.drafted = self.utterance;
                self.pending = Some(id.to_owned());
                Ok(Event::Draft {
                    id: id.to_owned(),
                    text: Some(text.to_owned()),
                })
            }
            Some("ended") => bail!("Codex subscription voice is unavailable or ended"),
            _ => bail!("Unexpected Codex voice bridge event"),
        }
    }

    pub(super) async fn draft_result(&mut self, id: &str, posted: bool) -> Result<()> {
        if self.pending.as_deref() == Some(id) {
            self.pending = None;
        } else {
            ensure!(!posted, "Unexpected Codex voice draft result");
        }
        self.send(json!({"type":"draft_result", "id":id, "posted":posted}))
            .await
    }

    pub(super) async fn close(&mut self) {
        if let Err(error) = timeout(Duration::from_secs(2), async {
            self.input.write_all(b"{\"type\":\"stop\"}\n").await?;
            self.input.shutdown().await?;
            self.child.wait().await
        })
        .await
        {
            drop(error);
        }
    }

    async fn read(&mut self) -> Result<Value> {
        loop {
            let chunk = self
                .output
                .fill_buf()
                .await
                .context("Codex voice bridge output failed")?;
            ensure!(!chunk.is_empty(), "Codex voice bridge disconnected");
            let newline = chunk.iter().position(|&b| b == b'\n');
            let measured = newline.map_or(chunk.len(), |at| at + 1);
            ensure!(
                measured <= MAX_LINE.saturating_sub(self.partial.len()),
                "Codex voice bridge frame exceeded its limit"
            );
            self.partial.extend_from_slice(&chunk[..measured]);
            self.output.consume(measured);
            if newline.is_some() {
                let result = serde_json::from_slice(&self.partial)
                    .map_err(|_| anyhow::anyhow!("Invalid Codex voice bridge frame"));
                self.partial.clear();
                return result;
            }
        }
    }

    async fn send(&mut self, payload: Value) -> Result<()> {
        let frame = format!("{payload}\n");
        ensure!(
            frame.len() <= MAX_LINE,
            "Codex voice bridge command exceeded its limit"
        );
        timeout(
            Duration::from_secs(2),
            self.input.write_all(frame.as_bytes()),
        )
        .await?
        .context("Codex voice bridge input closed")
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        #[cfg(unix)]
        if matches!(self.child.try_wait(), Ok(None))
            && let Some(pid) = self.child.id().and_then(|id| i32::try_from(id).ok())
            && let Err(error) = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            )
            && error != nix::errno::Errno::ESRCH
        {
            eprintln!("Codex voice subprocess group cleanup failed");
        }
    }
}

fn absolute_env(key: &str) -> Result<Option<PathBuf>> {
    let Some(raw) = std::env::var_os(key) else {
        return Ok(None);
    };
    let path = PathBuf::from(raw);
    ensure!(
        path.is_absolute(),
        "Codex voice path configuration must be absolute"
    );
    Ok(Some(path))
}

pub(super) fn energetic(pcm: &[u8]) -> bool {
    let samples = pcm.chunks_exact(2);
    let count = u64::try_from(samples.len()).unwrap_or(u64::MAX);
    let energy: u64 = samples
        .map(|s| {
            i64::from(i16::from_le_bytes([s[0], s[1]]))
                .unsigned_abs()
                .pow(2)
        })
        .sum();
    count > 0 && energy >= count.saturating_mul(256 * 256)
}
