// SPDX-License-Identifier: Apache-2.0
//! Checkpoint payloads use the accelerator's authenticated Manifest/chunk store.
//! Only the small recovery identity and root Manifest reference remain local.
use std::{collections::BTreeSet, os::unix::fs::MetadataExt, path::Path};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

pub(super) struct Pager {
    child: tokio::process::Child,
    runtime: std::path::PathBuf,
}
impl Pager {
    pub(super) async fn wait(&mut self) {
        let _ = self.child.wait().await;
    }
}
impl Drop for Pager {
    fn drop(&mut self) {
        // kill_on_drop closes a failed/unclaimed server. Once connected, VMM
        // shutdown closes its socket and the normal waiter reaps the process.
        let _ = std::fs::remove_file(self.runtime.join("p"));
        let _ = std::fs::remove_dir(&self.runtime);
    }
}
pub(super) struct Store {
    executable: String,
    config: String,
}
impl Store {
    pub(super) fn configured() -> Result<Option<Self>> {
        match std::env::var("KUASAR_CHECKPOINT_MANIFEST_CONFIG") {
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(e) => Err(e.into()),
            Ok(config) => {
                if config.is_empty() {
                    return Err(anyhow!("empty checkpoint Manifest configuration"));
                }
                Ok(Some(Self {
                    config,
                    executable: std::env::var("KUASAR_CHECKPOINT_MANIFEST_RAW").context(
                        "checkpoint Manifest store requires KUASAR_CHECKPOINT_MANIFEST_RAW",
                    )?,
                }))
            }
        }
    }
    async fn run(&self, args: &[&str]) -> Result<Value> {
        let started = std::time::Instant::now();
        let output = tokio::process::Command::new(&self.executable)
            .args(["--manifest-config", &self.config])
            .args(args)
            .kill_on_drop(true)
            .output()
            .await?;
        // Configuration errors may contain credentials; do not expose stderr.
        if !output.status.success() {
            return Err(anyhow!(
                "checkpoint Manifest operation failed ({})",
                output.status
            ));
        }
        let value: Value = serde_json::from_slice(&output.stdout)?;
        tracing::info!(target: "checkpoint_profile", command=args[0], file=Path::new(args.last().unwrap()).file_name().and_then(|s|s.to_str()).unwrap_or(""),
            elapsed_us=started.elapsed().as_micros() as u64, metadata=?value["maintenance"], "CHECKPOINT_STORE");
        Ok(value)
    }
    pub(super) async fn start_pager(&self, payload: &Path, work: &Path) -> Result<Pager> {
        use tokio::io::AsyncReadExt;
        let descriptor: Value = serde_json::from_slice(
            &tokio::fs::read(payload.join("memory-ranges.manifest.json")).await?,
        )?;
        // Keep Unix socket names below sockaddr_un's limit. This private runtime
        // directory is not the recovery authority and is deleted on disconnect.
        let key = descriptor["manifest"]
            .as_str()
            .context("missing memory Manifest")?;
        let runtime = std::env::temp_dir().join(format!("kp-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&runtime)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))?;
        let socket = runtime.join("p");
        let child = tokio::process::Command::new(&self.executable)
            .args([
                "--manifest-config",
                &self.config,
                if descriptor["kind"] == "manifest-memory-graph" {
                    "serve-memory"
                } else {
                    "serve-pages"
                },
                key,
                socket.to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                let _ = std::fs::remove_dir(&runtime);
                e
            })?;
        let mut pager = Pager { child, runtime };
        let mut ready = [0];
        tokio::time::timeout(
            std::time::Duration::from_secs(35),
            pager.child.stdout.as_mut().unwrap().read_exact(&mut ready),
        )
        .await??;
        if ready != [1] {
            return Err(anyhow!("Manifest pager failed to initialize"));
        }
        let file = work.join("memory-ranges.manifest.pending");
        tokio::fs::write(
            &file,
            serde_json::to_vec(
                &json!({"socket":socket,"logical_bytes":descriptor["logical_bytes"],
                    "incremental":std::env::var("KUASAR_INCREMENTAL_MEMORY").as_deref()!=Ok("0"),
                    "baseline":{"manifest":descriptor["manifest"],"kind":descriptor["kind"].as_str().unwrap_or("raw"),"logical_bytes":descriptor["logical_bytes"]}}),
            )?,
        )
        .await?;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o400))?;
        std::fs::File::open(&file)?.sync_all()?;
        tokio::fs::rename(&file, work.join("memory-ranges.manifest.json")).await?;
        std::fs::File::open(work)?.sync_all()?;
        Ok(pager)
    }
    /// Called while the guest barrier and drained device state are held. The
    /// Store acknowledges durable objects before we publish their root key.
    pub(super) async fn publish(&self, directory: &Path) -> Result<Value> {
        let _memory_transaction = super::memory_maintenance::transaction().await?;
        let mut paths = std::fs::read_dir(directory)?
            .map(|e| Ok(e?.path()))
            .collect::<Result<Vec<_>>>()?;
        paths.sort();
        let mut files = Vec::new();
        let mut memory_objects = Vec::new();
        let incremental = directory.join("memory-delta.json").is_file();
        if incremental {
            let mut spec: Value = serde_json::from_slice(
                &tokio::fs::read(directory.join("memory-delta.json")).await?,
            )?;
            let delta = self
                .run(&[
                    "store",
                    directory.join("memory-ranges.delta").to_str().unwrap(),
                ])
                .await?;
            spec["delta"] = json!({"manifest":delta["manifest"],"logical_bytes":delta["logical_bytes"],"kind":"raw"});
            let pending = directory.join("compose-memory.pending");
            tokio::fs::write(&pending, serde_json::to_vec(&spec)?).await?;
            let graph = self
                .run(&["compose-memory", pending.to_str().unwrap()])
                .await?;
            tokio::fs::remove_file(pending).await?;
            memory_objects = graph["objects"]
                .as_array()
                .context("Missing memory object references")?
                .clone();
            let stored = delta["stats"]["StoredBytes"].as_u64().unwrap_or(0)
                + graph["stats"]["StoredBytes"].as_u64().unwrap_or(0);
            files.push(json!({"name":"memory-ranges","manifest":graph["manifest"],"logical_bytes":graph["logical_bytes"],
                "kind":"manifest-memory-graph","capture_allocated_bytes":spec["written_bytes"],
                "capture_read_bytes":spec["read_bytes"],"stats":{"StoredBytes":stored},"objects":memory_objects}));
        }
        for path in paths {
            if !path.symlink_metadata()?.is_file() {
                return Err(anyhow!("non-regular checkpoint payload"));
            }
            let name = path
                .file_name()
                .unwrap()
                .to_str()
                .context("checkpoint filename")?;
            if name == "checkpoint.json"
                || name == "allow-memory-delta"
                || (incremental
                    && matches!(
                        name,
                        "memory-ranges" | "memory-ranges.delta" | "memory-delta.json"
                    ))
            {
                continue;
            }
            let cached = super::disk_checkpoint::stored_reference(&path)?;
            if let Some(reference) = cached.clone().or(super::disk_checkpoint::manifest_reference(&path)?) {
                let inspected = self
                    .run(&[
                        if reference["kind"] == "manifest-disk-graph" {
                            "inspect-disk"
                        } else {
                            "inspect"
                        },
                        reference["manifest"].as_str().unwrap(),
                    ])
                    .await;
                if inspected.as_ref().is_ok_and(|v| v["logical_bytes"] == reference["logical_bytes"]) {
                    files.push(json!({"name":name,"manifest":reference["manifest"],"logical_bytes":reference["logical_bytes"],
                        "kind":reference["kind"],"capture_allocated_bytes":path.metadata()?.blocks()*512,"stats":{"StoredBytes":0}}));
                    continue;
                }
                if cached.is_none() {
                    inspected?;
                    return Err(anyhow!("disk Manifest size mismatch"));
                }
                // An old checkpoint may have been reclaimed. Full immutable
                // bytes are still here, so re-ingest rather than trusting or
                // requiring a stale receipt. Lazy descriptors cannot do this.
                tracing::info!(file=name, "CHECKPOINT_STORED_RECEIPT_REINGEST");
            }
            let stored = self
                .run(&["store", path.to_str().context("checkpoint path")?])
                .await?;
            files.push(json!({"name":name,"manifest":stored["manifest"],
                "logical_bytes":stored["logical_bytes"],"capture_allocated_bytes":path.metadata()?.blocks()*512,
                "stats":stored["stats"]}));
        }
        let mut stored_bytes: u64 = files
            .iter()
            .filter_map(|f| f["stats"]["StoredBytes"].as_u64())
            .sum();
        let captured_bytes: u64 = files
            .iter()
            .filter_map(|f| f["capture_allocated_bytes"].as_u64())
            .sum();
        let original_files = files.clone();
        let config_path = directory.join("config.json");
        let mut config: Value = serde_json::from_slice(&tokio::fs::read(&config_path).await?)?;
        let mut disk_names = BTreeSet::new();
        let mut objects = BTreeSet::new();
        for key in &memory_objects {
            objects.insert(
                key.as_str()
                    .context("Invalid memory object reference")?
                    .to_owned(),
            );
        }
        let mut changed = false;
        for disk in config["disks"]
            .as_array_mut()
            .context("missing snapshot disks")?
        {
            let name = Path::new(disk["path"].as_str().context("missing disk path")?)
                .file_name()
                .and_then(|n| n.to_str())
                .context("bad disk name")?
                .to_owned();
            disk_names.insert(name.clone());
            if disk["image_type"] != "Qcow2" {
                continue;
            }
            let spec =
                super::disk_checkpoint::composition(&directory.join(&name), &original_files)?;
            let spec_path = directory.join("compose-disk.pending");
            tokio::fs::write(&spec_path, serde_json::to_vec(&spec)?).await?;
            let graph = self
                .run(&["compose-disk", spec_path.to_str().unwrap()])
                .await?;
            tokio::fs::remove_file(spec_path).await?;
            stored_bytes += graph["stats"]["StoredBytes"].as_u64().unwrap_or(0);
            for key in graph["objects"]
                .as_array()
                .context("missing disk object references")?
            {
                objects.insert(
                    key.as_str()
                        .context("invalid disk object reference")?
                        .to_owned(),
                );
            }
            let entry = files
                .iter_mut()
                .find(|f| f["name"] == name)
                .context("missing disk payload")?;
            *entry = json!({"name":name,"manifest":graph["manifest"],"logical_bytes":graph["logical_bytes"],"kind":"manifest-disk-graph","stats":graph["stats"]});
            disk["image_type"] = json!("Raw");
            disk["backing_files"] = json!(false);
            changed = true;
        }
        if changed {
            // Saved QCOW data stays in chunks, reachable through the flat block
            // map. Only disk roots are restore payloads, not their old data files.
            files.retain(|f| {
                let n = f["name"].as_str().unwrap();
                (!n.ends_with(".raw") && !n.ends_with(".qcow2")) || disk_names.contains(n)
            });
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o600))?;
            tokio::fs::write(&config_path, serde_json::to_vec(&config)?).await?;
            let result = self.run(&["store", config_path.to_str().unwrap()]).await?;
            stored_bytes += result["stats"]["StoredBytes"].as_u64().unwrap_or(0);
            let entry = files
                .iter_mut()
                .find(|f| f["name"] == "config.json")
                .context("missing config")?;
            entry["manifest"] = result["manifest"].clone();
            entry["logical_bytes"] = result["logical_bytes"].clone();
            entry["stats"] = result["stats"].clone();
        }
        let index = json!({"version":1,"files":files,"objects":objects});
        validate(&index)?;
        let index_path = directory.join("chunk-index.pending");
        tokio::fs::write(&index_path, serde_json::to_vec(&index)?).await?;
        let result = self.run(&["store", index_path.to_str().unwrap()]).await?;
        tokio::fs::remove_file(index_path).await?;
        let logical_bytes: u64 = files
            .iter()
            .filter_map(|f| f["logical_bytes"].as_u64())
            .sum();
        let capture_allocated_bytes = captured_bytes;
        let new_store_bytes = stored_bytes + result["stats"]["StoredBytes"].as_u64().unwrap_or(0);
        let root = json!({"version":1,"manifest":result["manifest"],"files":files,
            "logical_bytes":logical_bytes,"capture_allocated_bytes":capture_allocated_bytes,
            "new_store_bytes":new_store_bytes});
        super::memory_maintenance::pin(directory.parent().context("missing publication owner")?, "published", &root)?;
        Ok(root)
    }
    /// Build the local RAM cache from Manifest runs, preserving zero/hole runs
    /// instead of retaining CH's dense export. This is preparation work, not a
    /// hidden part of a later create request.
    pub(super) async fn sparsify_memory(&self, directory: &Path, root: &Value) -> Result<()> {
        let entry = root["files"]
            .as_array()
            .context("missing payload index")?
            .iter()
            .find(|f| f["name"] == "memory-ranges")
            .context("missing RAM Manifest")?;
        let path = directory.join(format!(".memory-{}", uuid::Uuid::new_v4()));
        let result = async {
            self.run(&[
                if entry["kind"] == "manifest-memory-graph" {
                    "load-memory"
                } else {
                    "load"
                },
                entry["manifest"].as_str().context("missing RAM key")?,
                path.to_str().context("RAM path")?,
            ])
            .await?;
            std::fs::rename(&path, directory.join("memory-ranges"))?;
            std::fs::File::open(directory)?.sync_all()?;
            Ok(())
        }
        .await;
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        result
    }
    /// Native files are a disposable cache. Materialize into a private directory
    /// and publish it only after every chunk has been authenticated and synced.
    pub(super) async fn materialize(
        &self,
        directory: &Path,
        root: &Value,
    ) -> Result<std::path::PathBuf> {
        self.materialize_mode(directory, root, false).await
    }
    pub(super) async fn materialize_uffd(
        &self,
        directory: &Path,
        root: &Value,
    ) -> Result<std::path::PathBuf> {
        self.materialize_mode(directory, root, true).await
    }
    async fn materialize_mode(
        &self,
        directory: &Path,
        root: &Value,
        uffd: bool,
    ) -> Result<std::path::PathBuf> {
        let destination = directory.join(if uffd {
            "materialized-demand-v2"
        } else {
            "materialized-v3-provenance"
        });
        if destination.is_dir() {
            return Ok(destination);
        }
        let temporary = directory.join(format!(".materializing-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&temporary)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o700))?;
        let result = async {
            let key = root["manifest"]
                .as_str()
                .context("missing checkpoint root Manifest")?;
            self.run(&["load", key, temporary.join("index.json").to_str().unwrap()])
                .await?;
            let index: Value =
                serde_json::from_slice(&tokio::fs::read(temporary.join("index.json")).await?)?;
            validate(&index)?;
            for entry in index["files"].as_array().unwrap() {
                let path = temporary.join(entry["name"].as_str().unwrap());
                if uffd && entry["name"] == "memory-ranges" {
                    let descriptor = json!({"executable":self.executable,"config":self.config,
                        "manifest":entry["manifest"],"logical_bytes":entry["logical_bytes"],"kind":entry["kind"].as_str().unwrap_or("raw")});
                    let path = temporary.join("memory-ranges.manifest.json");
                    tokio::fs::write(&path, serde_json::to_vec(&descriptor)?).await?;
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400))?;
                    std::fs::File::open(&path)?.sync_all()?;
                    continue;
                }
                if uffd
                    && (matches!(
                        entry["kind"].as_str(),
                        Some("manifest-disk" | "manifest-disk-graph")
                    ) || entry["name"].as_str().unwrap().ends_with(".raw"))
                {
                    super::disk_checkpoint::write_manifest_reference(&path, entry)?;
                    continue;
                }
                let loaded = self
                    .run(&[
                        if entry["kind"] == "manifest-memory-graph" {
                            "load-memory"
                        } else if entry["kind"] == "manifest-disk-graph" {
                            "load-disk"
                        } else {
                            "load"
                        },
                        entry["manifest"].as_str().context("missing Manifest key")?,
                        path.to_str().unwrap(),
                    ])
                    .await?;
                if loaded["logical_bytes"] != entry["logical_bytes"] {
                    return Err(anyhow!("checkpoint payload length mismatch"));
                }
                if entry["kind"] == "manifest-disk-graph" || entry["name"].as_str().unwrap().ends_with(".raw") {
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400))?;
                    super::disk_checkpoint::write_stored_reference(&path, entry)?;
                }
            }
            // All disks are captured into this index. Restore by indexed name,
            // never by an external path from the originating host.
            let config_path = temporary.join("config.json");
            let mut config: Value = serde_json::from_slice(&std::fs::read(&config_path)?)?;
            for disk in config["disks"]
                .as_array_mut()
                .context("missing snapshot disks")?
            {
                let name = Path::new(disk["path"].as_str().context("missing disk path")?)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .context("invalid snapshot disk")?;
                if !index["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|f| f["name"] == name)
                {
                    return Err(anyhow!("snapshot has an unpublished disk dependency"));
                }
                disk["path"] = json!(destination.join(name));
            }
            std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o600))?;
            std::fs::write(&config_path, serde_json::to_vec(&config)?)?;
            std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o400))?;
            std::fs::File::open(&config_path)?.sync_all()?;
            std::fs::remove_file(temporary.join("index.json"))?;
            std::fs::File::open(&temporary)?.sync_all()?;
            if let Err(error) = tokio::fs::rename(&temporary, &destination).await {
                // Two slots may miss the same immutable template cache. A
                // competing atomic publication contains this same root index.
                if !destination.is_dir() {
                    return Err(error.into());
                }
            }
            std::fs::File::open(directory)?.sync_all()?;
            Ok(destination)
        }
        .await;
        if temporary.exists() {
            tokio::fs::remove_dir_all(temporary).await?;
        }
        result
    }
}
fn validate(index: &Value) -> Result<()> {
    if index["version"] != 1 {
        return Err(anyhow!("unsupported checkpoint index"));
    }
    let files = index["files"]
        .as_array()
        .context("missing checkpoint files")?;
    let mut names = BTreeSet::new();
    for entry in files {
        let name = entry["name"]
            .as_str()
            .context("missing checkpoint filename")?;
        if name.is_empty()
            || name.starts_with('.')
            || name.contains('/')
            || name.contains('\\')
            || matches!(name, "checkpoint.json" | "index.json" | "materialized")
            || !names.insert(name)
        {
            return Err(anyhow!("invalid checkpoint filename"));
        }
        if entry["logical_bytes"].as_u64().is_none() || entry["manifest"].as_str().is_none() {
            return Err(anyhow!("invalid checkpoint entry"));
        }
    }
    for required in ["config.json", "state.json", "memory-ranges"] {
        if !names.contains(required) {
            return Err(anyhow!("incomplete checkpoint: {required}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn reclaimed_materialized_receipt_falls_back_to_full_ingest() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = temp_dir::TempDir::new().unwrap();
        let payload = tmp.path().join("payload");
        std::fs::create_dir(&payload).unwrap();
        std::fs::write(payload.join("config.json"), br#"{"disks":[]}"#).unwrap();
        std::fs::write(payload.join("state.json"), b"{}").unwrap();
        std::fs::write(payload.join("memory-ranges"), b"RAM").unwrap();
        let disk = payload.join("image.raw");
        std::fs::write(&disk, vec![0x55; 4096]).unwrap();
        std::fs::set_permissions(&disk, std::fs::Permissions::from_mode(0o400)).unwrap();
        super::super::disk_checkpoint::write_stored_reference(
            &disk,
            &json!({"manifest":"a".repeat(64),"logical_bytes":4096,"kind":"raw"}),
        )
        .unwrap();
        let executable = tmp.path().join("store");
        let script = format!(
            r#"#!/bin/sh
echo "$3:$4" >> "$0.calls"
case "$3" in
inspect*) exit 1 ;;
store) size=$(wc -c < "$4"); printf '{{"manifest":"{}","logical_bytes":%s,"stats":{{"StoredBytes":%s}}}}' "$size" "$size" ;;
*) exit 2 ;;
esac
"#,
            "b".repeat(64)
        );
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = Store {
            executable: executable.to_str().unwrap().into(),
            config: "unused".into(),
        };
        let root = store.publish(&payload).await.unwrap();
        let calls = std::fs::read_to_string(tmp.path().join("store.calls")).unwrap();
        assert!(calls.contains(&format!("inspect:{}", "a".repeat(64))));
        assert!(calls.contains(&format!("store:{}", disk.display())));
        let published = root["files"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["name"] == "image.raw")
            .unwrap();
        assert_eq!(published["manifest"], "b".repeat(64));
        assert_eq!(published["stats"]["StoredBytes"], 4096);
        assert_eq!(std::fs::read(disk).unwrap(), vec![0x55; 4096]);
    }
    #[test]
    fn index_rejects_escape_duplicate_and_missing_payloads() {
        let files: Vec<_> = ["config.json", "state.json", "memory-ranges"]
            .iter()
            .map(|name| json!({"name":name,"manifest":"key","logical_bytes":0}))
            .collect();
        let valid = json!({"version":1,"files":files});
        assert!(validate(&valid).is_ok());
        for name in [
            "../escape",
            "/absolute",
            "checkpoint.json",
            "index.json",
            "state.json",
        ] {
            let mut bad = valid.clone();
            bad["files"][0]["name"] = json!(name);
            assert!(validate(&bad).is_err());
        }
    }
    #[tokio::test]
    async fn failed_publication_keeps_capture_and_has_no_root() {
        let tmp = temp_dir::TempDir::new().unwrap();
        for name in ["config.json", "state.json", "memory-ranges"] {
            std::fs::write(tmp.path().join(name), b"captured state").unwrap();
        }
        let store = Store {
            executable: "/bin/false".into(),
            config: "unused".into(),
        };
        assert!(store.publish(tmp.path()).await.is_err());
        assert_eq!(
            std::fs::read(tmp.path().join("memory-ranges")).unwrap(),
            b"captured state"
        );
        assert!(!tmp.path().join("chunk-index.pending").exists());
    }
    #[tokio::test]
    async fn failed_restore_never_publishes_partial_carrier() {
        let tmp = temp_dir::TempDir::new().unwrap();
        let store = Store {
            executable: "/bin/false".into(),
            config: "unused".into(),
        };
        assert!(store
            .materialize(tmp.path(), &json!({"manifest":"key"}))
            .await
            .is_err());
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
    }
}
