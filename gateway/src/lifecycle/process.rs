use anyhow::{Context, Result, bail, ensure};
use nix::unistd::{AccessFlags, access, setsid};
use std::{
    env,
    ffi::OsStr,
    fs::File,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use super::config::launcher_path;

pub(super) enum ChildKind {
    Gateway,
    RestartRunner,
    HeadOwner,
}

impl ChildKind {
    fn arg(&self) -> &'static str {
        match self {
            Self::Gateway => "serve",
            Self::RestartRunner => "restart-runner",
            Self::HeadOwner => "host-owner",
        }
    }
}

/// Spawn the native trampoline; it replaces itself with the exact child PID.
/// No resident proxy, shell, or terminal descriptors are involved.
pub(super) fn detached(kind: ChildKind, log: &File) -> Result<Command> {
    let mut command = Command::new(env::current_exe()?);
    command
        .args(["__detach", kind.arg()])
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log.try_clone()?));
    Ok(command)
}

pub(super) fn host_owner_executable(root: &Path) -> Result<PathBuf> {
    let os = if env::consts::OS == "macos" {
        "darwin"
    } else {
        env::consts::OS
    };
    let arch = match env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => other,
    };
    let path = root.join(format!("host-owner/bin/omp-host-owner-{os}-{arch}"));
    access(&path, AccessFlags::X_OK).context(
        "Host owner is missing; reinstall the platform package or run bun run build:owner",
    )?;
    Ok(path)
}

/// Private command entry, not an IPC operation. Run only before async
/// initialization, so the process never holds sockets, threads, or a writer lock.
pub fn exec_detached(mut args: impl Iterator<Item = String>) -> Result<()> {
    let kind = args.next().context("Missing internal lifecycle command")?;
    let mut command = match kind.as_str() {
        "serve" | "restart-runner" => {
            let mut command = Command::new(env::current_exe()?);
            command.arg(&kind).args(args);
            command
        }
        "host-owner" => {
            let manifest = args
                .next()
                .map(PathBuf::from)
                .context("Missing owned Host manifest")?;
            ensure!(
                manifest.is_absolute()
                    && manifest.file_name() == Some(OsStr::new("owner.json"))
                    && args.next().is_none(),
                "Invalid owned Host launch manifest path"
            );
            let root = launcher_path("OMP_SUPERAGENT_ROOT")?;
            let mut command = Command::new(host_owner_executable(&root)?);
            command.arg(manifest);
            command
        }
        _ => bail!("Unknown internal lifecycle command"),
    };
    setsid().context("Could not detach lifecycle process from its terminal session")?;
    Err(command.exec()).context("Could not execute the owned lifecycle process")
}
