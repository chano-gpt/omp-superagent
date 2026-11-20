use anyhow::{Context, Result, bail, ensure};
use nix::{
    errno::Errno,
    fcntl::OFlag,
    sys::signal::kill,
    unistd::{Pid, getuid},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fs::{self, DirBuilder, File, Metadata, OpenOptions, Permissions},
    io::ErrorKind,
    os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
    time::Duration,
};
use tokio::time::{Instant, sleep};

pub(super) const EXIT_TIMEOUT: Duration = Duration::from_secs(15);
pub(super) const WAIT_SLICE: Duration = Duration::from_millis(25);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
pub(super) struct Endpoint {
    pub pid: u32,
    pub dev: u64,
    pub ino: u64,
}

impl Endpoint {
    pub fn matches(self, stat: &Metadata) -> bool {
        stat.file_type().is_socket()
            && stat.uid() == getuid().as_raw()
            && self.dev == stat.dev()
            && self.ino == stat.ino()
    }
}

pub(super) fn alive(pid: u32) -> Result<bool> {
    ensure!(pid > 0, "Invalid recorded process identity");
    match kill(Pid::from_raw(i32::try_from(pid)?), None) {
        Ok(()) => Ok(true),
        Err(Errno::ESRCH) => Ok(false),
        Err(error) => Err(error).context("Unable to probe owned process"),
    }
}

pub(super) fn metadata(path: &Path) -> Result<Option<Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(stat) => Ok(Some(stat)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("Cannot inspect owned endpoint"),
    }
}

pub(super) fn private_directory(path: &Path) -> Result<()> {
    DirBuilder::new().recursive(true).mode(0o700).create(path)?;
    let stat = fs::symlink_metadata(path)?;
    ensure!(
        stat.is_dir() && stat.uid() == getuid().as_raw(),
        "Not an owned directory"
    );
    fs::set_permissions(path, Permissions::from_mode(0o700))?;
    Ok(())
}

pub(super) fn socket_stat(path: &Path, exact_mode: bool) -> Result<Metadata> {
    let parent = fs::symlink_metadata(path.parent().context("Missing socket directory")?)?;
    ensure!(
        parent.is_dir() && parent.uid() == getuid().as_raw() && parent.mode() & 0o777 == 0o700,
        "Endpoint directory is not private"
    );
    let stat = fs::symlink_metadata(path)?;
    let mode = stat.mode() & 0o777;
    ensure!(
        stat.file_type().is_socket()
            && stat.uid() == getuid().as_raw()
            && if exact_mode {
                mode == 0o600
            } else {
                mode & 0o077 == 0
            },
        "Endpoint is not a private owned Unix socket"
    );
    Ok(stat)
}

pub(super) async fn remove_dead_socket(
    path: &Path,
    identity: Option<Endpoint>,
    owner: Option<u32>,
) -> Result<()> {
    let deadline = Instant::now() + EXIT_TIMEOUT;
    loop {
        let observed = metadata(path)?;
        match (identity, owner, observed.as_ref()) {
            (Some(endpoint), Some(_), _) if alive(endpoint.pid)? => {
                bail!(
                    "Existing HEAD ownership is unresolved; no endpoint was removed or second host started"
                );
            }
            (Some(endpoint), _, Some(stat)) if alive(endpoint.pid)? || !endpoint.matches(stat) => {
                bail!("Existing endpoint ownership is unresolved; no socket was removed");
            }
            (None, _, Some(_)) => {
                bail!("Existing endpoint has no durable owner; no socket was removed")
            }
            _ => {}
        }
        if let Some(owner_pid) = owner {
            if alive(owner_pid)? {
                ensure!(
                    Instant::now() < deadline,
                    "Owned adapter exit is pending; no endpoint was removed"
                );
                // Wait for the pipe owner to persist its exit receipt and drop its endpoint.
                sleep(WAIT_SLICE).await;
                continue;
            }
        }
        if observed.is_some() {
            fs::remove_file(path)?;
        }
        return Ok(());
    }
}

pub(super) fn log(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .custom_flags(OFlag::O_NOFOLLOW.bits() | OFlag::O_NONBLOCK.bits())
        .open(path)?;
    let stat = file.metadata()?;
    ensure!(
        stat.is_file() && stat.uid() == getuid().as_raw() && stat.mode() & 0o077 == 0,
        "Lifecycle log must be a private owned regular file"
    );
    Ok(file)
}

pub(super) fn private_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    serde_json::to_writer(file, value)?;
    Ok(())
}

pub(super) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    Ok(serde_json::from_reader(File::open(path)?)?)
}

pub(super) fn read_private_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(OFlag::O_NOFOLLOW.bits() | OFlag::O_NONBLOCK.bits())
        .open(path)?;
    let stat = file.metadata()?;
    ensure!(
        stat.is_file() && stat.uid() == getuid().as_raw() && stat.mode() & 0o077 == 0,
        "Lifecycle receipt must be a private owned regular file"
    );
    Ok(serde_json::from_reader(file)?)
}
