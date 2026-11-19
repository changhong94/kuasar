// SPDX-License-Identifier: Apache-2.0
//! Durable RAM roots for the exclusive checkpoint Store's background collector.
use std::{
    fs::File,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde_json::{json, Value};

pub(super) async fn transaction() -> Result<Option<File>> {
    let Some(root) = std::env::var_os("KUASAR_MEMORY_MAINTENANCE") else {
        return Ok(None);
    };
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&root)?;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(PathBuf::from(root).join("transactions.lock"))?;
        // SAFETY: the descriptor is owned until the transaction guard is dropped.
        if unsafe { nix::libc::flock(f.as_raw_fd(), nix::libc::LOCK_SH) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Some(f))
    })
    .await?
}

fn atomic(path: &Path, value: &Value) -> Result<()> {
    let temporary = path.with_file_name(format!(".memory-roots-{}", uuid::Uuid::new_v4()));
    let result = (|| {
        std::fs::write(&temporary, serde_json::to_vec(value)?)?;
        File::open(&temporary)?.sync_all()?;
        std::fs::rename(&temporary, path)?;
        File::open(path.parent().context("missing root directory")?)?.sync_all()?;
        Ok(())
    })();
    if temporary.exists() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

pub(super) fn pin(base: &Path, slot: &str, root: &Value) -> Result<()> {
    if std::env::var_os("KUASAR_MEMORY_MAINTENANCE").is_none() {
        return Ok(());
    }
    let owner =
        PathBuf::from(std::env::var_os("KUASAR_MEMORY_MAINTENANCE").unwrap()).join("owner.json");
    if owner.exists() {
        let configured: Value = serde_json::from_slice(&std::fs::read(owner)?)?;
        let state = Path::new(
            configured["state"]
                .as_str()
                .context("missing GC state root")?,
        );
        anyhow::ensure!(
            base.canonicalize()?.starts_with(state),
            "checkpoint owner is outside GC's registered state root"
        );
    }
    let entry = root["files"]
        .as_array()
        .context("missing checkpoint files")?
        .iter()
        .find(|f| f["name"] == "memory-ranges")
        .context("missing memory root")?;
    let path = base.join("memory-roots.json");
    let mut roots: Value = match std::fs::read(&path) {
        Ok(data) => serde_json::from_slice(&data)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => json!({}),
        Err(e) => return Err(e.into()),
    };
    anyhow::ensure!(roots.is_object(), "invalid memory root registry");
    // A durable published pin already protects the committed checkpoint. Drop
    // the legacy duplicate slot atomically with its replacement, not beforehand.
    if slot == "published" && std::env::var("KUASAR_MEMORY_TUNING").as_deref() != Ok("0") {
        roots.as_object_mut().unwrap().remove("checkpoint");
    }
    roots[slot] = json!({"manifest":entry["manifest"],"kind":entry["kind"].as_str().unwrap_or("raw"),"logical_bytes":entry["logical_bytes"]});
    atomic(&path, &roots)
}

pub(super) fn inherit(parent: &Path, child: &Path) -> Result<()> {
    if std::env::var_os("KUASAR_MEMORY_MAINTENANCE").is_none() {
        return Ok(());
    }
    match std::fs::read(parent.join("memory-roots.json")) {
        Ok(b) => atomic(
            &child.join("memory-roots.json"),
            &serde_json::from_slice(&b)?,
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Resume durable queued compactions when the controller starts. The worker has
/// an interprocess singleton lock and never reads live guest memory.
pub(super) fn start() {
    if std::env::var_os("KUASAR_MEMORY_MAINTENANCE").is_none() {
        return;
    }
    let (Ok(executable), Ok(config)) = (
        std::env::var("KUASAR_CHECKPOINT_MANIFEST_RAW"),
        std::env::var("KUASAR_CHECKPOINT_MANIFEST_CONFIG"),
    ) else {
        return;
    };
    tokio::spawn(async move {
        match tokio::process::Command::new(executable)
            .args(["--manifest-config", &config, "maintain-memory", "run"])
            .status()
            .await
        {
            Ok(status) if status.success() => {}
            result => tracing::warn!(?result, "memory maintenance startup retry remains queued"),
        }
    });
}
