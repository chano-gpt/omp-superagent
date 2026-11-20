use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    env, fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::Command;

use super::{
    LifecycleConfig,
    fs::{metadata, read_json},
};

const FIXTURE_PACKAGE: &str = "OMP_SUPERAGENT_TEST_MANAGED_ULW_PATH";
const FIXTURE_ROOT: &str = "OMP_SUPERAGENT_TEST_MANAGED_ULW_ROOT";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    repository: String,
    #[serde(rename = "ref")]
    reference: String,
    directory: PathBuf,
    check_interval_ms: u64,
    command_timeout_ms: u64,
    probe_timeout_ms: u64,
    materialize: Value,
}

#[derive(Deserialize)]
struct Revision {
    revision: String,
    repository: String,
    branch: String,
}

pub(super) struct Ulw {
    root: PathBuf,
    package_fixture: Option<PathBuf>,
    root_fixture: bool,
    pub interval: Duration,
}

impl Ulw {
    pub fn load(config: &LifecycleConfig) -> Result<Self> {
        let fixture = env::var_os(FIXTURE_PACKAGE)
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);
        if let Some(path) = fixture {
            return Ok(Self {
                root: PathBuf::new(),
                package_fixture: Some(validate_package(&path)?),
                root_fixture: false,
                interval: Duration::from_secs(3600),
            });
        }
        let manifest: Manifest = read_json(&config.root.join("upstream/ulw.json"))?;
        ensure!(
            !manifest.repository.is_empty()
                && manifest.reference == "HEAD"
                && !manifest.directory.as_os_str().is_empty()
                && manifest.check_interval_ms >= 60_000
                && manifest.command_timeout_ms >= 1_000
                && manifest.probe_timeout_ms >= 1_000
                && (manifest.materialize.is_null() || manifest.materialize.is_string()),
            "Invalid managed ULW manifest"
        );
        let root_fixture = env::var_os(FIXTURE_ROOT)
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);
        let root = match &root_fixture {
            Some(path) if path.is_absolute() => path.clone(),
            Some(path) => env::current_dir()?.join(path),
            None => config.root.join(manifest.directory),
        };
        Ok(Self {
            root,
            package_fixture: None,
            root_fixture: root_fixture.is_some(),
            interval: Duration::from_millis(manifest.check_interval_ms),
        })
    }

    pub fn fixture(&self) -> bool {
        self.package_fixture.is_some() || self.root_fixture
    }

    pub fn can_update(&self) -> bool {
        self.package_fixture.is_none()
    }

    fn active(&self) -> Result<(Revision, PathBuf)> {
        let owner = fs::symlink_metadata(&self.root)
            .context("Mandatory managed ULW is not installed; run bun run prepare:ulw")?;
        ensure!(
            owner.is_dir()
                && owner.uid() == nix::unistd::getuid().as_raw()
                && owner.mode() & 0o077 == 0,
            "Managed ULW installation is not a private owned directory"
        );
        ensure!(
            fs::read(self.root.join("owner.json"))?
                == br#"{"owner":"omp-superagent-managed-ulw-v1"}"#,
            "Managed ULW installation has no ownership record"
        );
        let active: Revision = read_json(&self.root.join("active.json"))
            .context("Mandatory managed ULW is not installed; run bun run prepare:ulw")?;
        ensure!(
            (40..=64).contains(&active.revision.len())
                && active
                    .revision
                    .bytes()
                    .all(|ch| ch.is_ascii_digit() || (b'a'..=b'f').contains(&ch))
                && !active.repository.is_empty()
                && !active.branch.is_empty(),
            "Invalid managed ULW active pointer"
        );
        let directory = self.root.join("revisions").join(&active.revision);
        let provenance: Revision = read_json(&directory.join("provenance.json"))?;
        ensure!(
            provenance.revision == active.revision && provenance.repository == active.repository,
            "Managed ULW provenance does not match its active pointer"
        );
        Ok((active, directory.join("package")))
    }

    pub fn package(&self) -> Result<PathBuf> {
        if let Some(path) = &self.package_fixture {
            return Ok(path.clone());
        }
        validate_package(&self.active()?.1)
    }

    pub fn status(&self) -> Result<Value> {
        if let Some(path) = &self.package_fixture {
            return Ok(
                json!({"revision": null, "packagePath": path, "upstreamRevision": null,
                "checkedAt": null, "error": null, "updating": false, "injected": true}),
            );
        }
        let (active, package) = self.active()?;
        let status = self.root.join("status.json");
        let saved: Value = if metadata(&status)?.is_some() {
            read_json(&status)?
        } else {
            Value::Null
        };
        Ok(
            json!({"revision": active.revision, "packagePath": package, "upstreamRevision": saved["upstreamRevision"],
            "checkedAt": saved["checkedAt"], "error": saved["error"], "updating": saved["updating"] == true}),
        )
    }

    pub async fn update(&self, config: &LifecycleConfig) -> Result<()> {
        // The existing installer is a one-shot source maintenance job. It
        // owns the immutable-revision transaction, never a Gateway or HEAD.
        let mut command = Command::new(&config.bun);
        command
            .arg("--no-env-file")
            .arg(config.root.join("scripts/prepare-ulw.ts"))
            .current_dir(&config.root)
            .env_clear()
            .envs(config.head_environment())
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if self.root_fixture {
            command.arg("--root").arg(&self.root);
        }
        ensure!(
            command.status().await?.success(),
            "Managed ULW update failed"
        );
        Ok(())
    }
}

fn validate_package(path: &Path) -> Result<PathBuf> {
    ensure!(
        path.is_absolute(),
        "Managed ULW package must have an absolute path"
    );
    let canonical = fs::canonicalize(path)?;
    let package: Value = read_json(&canonical.join("package.json"))?;
    ensure!(
        matches!(package["name"].as_str(), Some("omp-superkit" | "omp-ulw")),
        "Managed ULW package has no native extension manifest"
    );
    let entries = package["omp"]["extensions"]
        .as_array()
        .filter(|items| !items.is_empty())
        .context("Managed ULW package has no native extension manifest")?;
    for entry in entries {
        let entry = Path::new(
            entry
                .as_str()
                .filter(|value| !value.is_empty())
                .context("Invalid managed ULW extension entry")?,
        );
        ensure!(!entry.is_absolute(), "Invalid managed ULW extension entry");
        let resolved = fs::canonicalize(canonical.join(entry))?;
        ensure!(
            resolved.starts_with(&canonical) && resolved.is_file(),
            "Managed ULW extension leaves its package"
        );
    }
    ensure!(
        fs::symlink_metadata(canonical.join("skills"))?.is_dir(),
        "Managed ULW skills are missing"
    );
    Ok(canonical)
}
