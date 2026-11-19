// SPDX-License-Identifier: Apache-2.0
//! Running checkpoints: freeze once, resume the source, publish asynchronously.
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use super::*;
use crate::cloud_hypervisor::live_fork::request;

async fn timed<F: std::future::Future>(future: F) -> (F::Output, f64) {
    let started = Instant::now();
    let value = future.await;
    (value, started.elapsed().as_secs_f64() * 1000.0)
}

fn root() -> anyhow::Result<PathBuf> {
    let root = PathBuf::from(std::env::var("KUASAR_SNAPSHOT_POOL_ROOT")?).canonicalize()?;
    Ok(root)
}
fn sync(path: &Path) -> anyhow::Result<()> {
    for e in std::fs::read_dir(path)? {
        let p = e?.path();
        if p.is_dir() {
            sync(&p)?;
        } else {
            std::fs::File::open(p)?.sync_all()?;
        }
    }
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}
fn status(path: &Path, value: &Value) -> anyhow::Result<()> {
    let tmp = path.with_extension("pending");
    std::fs::write(&tmp, serde_json::to_vec(value)?)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(tmp, path)?;
    std::fs::File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

pub(super) async fn handle<F>(handle: &Handle<F>, req: &Value, mode: &str) -> anyhow::Result<Value>
where
    F: VMFactory + Sync + Send + 'static,
    F::VM: VM + Snapshottable + Sync + Send + 'static,
{
    let key = req["template"]
        .as_str()
        .ok_or_else(|| anyhow!("missing template"))?;
    validate_id(key)?;
    let root = root()?;
    let jobs = root.join(".live-jobs");
    std::fs::create_dir_all(&jobs)?;
    let job = jobs.join(format!("{key}.json"));
    if mode == "status" {
        let mut state: Value = serde_json::from_slice(&std::fs::read(job)?)?;
        state["ok"] = json!(true);
        return Ok(state);
    }
    if mode == "restore" {
        let state: Value = serde_json::from_slice(&std::fs::read(job)?)?;
        if state["state"] != "durable" {
            return Err(anyhow!("snapshot is not durable"));
        }
        return super::snapshot_pool::handle(handle, req, "bind").await;
    }
    let started = Instant::now();
    let id = req["sandbox_id"]
        .as_str()
        .ok_or_else(|| anyhow!("missing sandbox_id"))?;
    validate_id(id)?;
    let source = handle
        .sandboxes
        .read()
        .await
        .get(id)
        .cloned()
        .ok_or_else(|| anyhow!("sandbox not found"))?;
    let mut sb = source.lock().await;
    if !sb.vm.live_fork_agent() || !sb.containers.is_empty() {
        return Err(anyhow!("requires an E2B live-fork runtime"));
    }
    if job.exists() || root.join(key).exists() {
        return Err(anyhow!("snapshot identity already exists"));
    }
    let network = sb
        .network
        .as_ref()
        .ok_or_else(|| anyhow!("missing network"))?;
    if network.interfaces().len() != 1 || network.deferred_old_mac.is_some() {
        return Err(anyhow!(
            "live snapshot requires completed network preparation"
        ));
    }
    let remote = handle.snapshot_config.checkpoint_mode == crate::config::CheckpointMode::Remote;
    let store = super::checkpoint_store::Store::configured()?;
    if remote && store.is_none() {
        return Err(anyhow!("remote snapshot requires Manifest storage"));
    }
    let api = sb.vm.live_fork_info()["api_socket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing API socket"))?
        .to_owned();
    let current = request(&api, "GET", "vm.info", None).await?;
    if current["state"] != "Running" {
        return Err(anyhow!("source must be running"));
    }
    let pending = root.join(format!(".capture-{key}"));
    std::fs::create_dir(&pending)?;
    std::fs::set_permissions(&pending, std::fs::Permissions::from_mode(0o700))?;
    let disk_root = root.join(format!(".capture-disks-{key}"));
    std::fs::create_dir(&disk_root)?;
    std::fs::set_permissions(&disk_root, std::fs::Permissions::from_mode(0o700))?;
    // Pin immutable disks before the pause so source deletion cannot unlink a
    // file still needed by the asynchronous exporter. No RAM is copied here.
    let mut pinned = std::collections::BTreeMap::new();
    for (i, disk) in current["config"]["disks"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        if disk["readonly"] == true {
            let path = disk["path"]
                .as_str()
                .ok_or_else(|| anyhow!("disk lacks path"))?;
            let pin = disk_root.join(format!("immutable-{i}"));
            if std::fs::hard_link(path, &pin).is_err() {
                let result = tokio::process::Command::new("cp")
                    .args(["--reflink=auto", "--preserve=xattr", "--", path])
                    .arg(&pin)
                    .status()
                    .await?;
                if !result.success() {
                    return Err(anyhow!("cannot pin immutable snapshot disk"));
                }
            }
            pinned.insert(path.to_string(), pin);
        }
    }
    let disk_dir = disk_root.join("fork-disks");
    let published = root.join(key);
    let token = uuid::Uuid::new_v4().to_string();
    let writable = current["config"]["disks"]
        .as_array()
        .is_some_and(|a| a.iter().any(|d| d["readonly"] == false));
    status(
        &job,
        &json!({"state":"capturing","template":key,"source":id}),
    )?;
    // These identify the offline capture operation; no socket is opened.
    // Keep them independent of the storage path's length (sockaddr_un limit).
    let child_api = std::env::temp_dir().join(format!("ks-{token}.api"));
    let child_serial = std::env::temp_dir().join(format!("ks-{token}.serial"));
    // This is best-effort preparation only; the stopped-device checkpoint
    // remains mandatory. Count preparation in request-to-durable time while
    // keeping the source runnable during potentially slow writeback.
    let fork_request = json!({
        "snapshot_directory":pending,"child_api_socket":child_api,"child_serial_socket":child_serial,
        "parent_disk_dir":writable.then(||Path::new(&sb.base_dir).join("fork-disks")),
        "child_disk_dir":writable.then(||disk_dir.clone())
    });
    let disk_prepare_start = Instant::now();
    if let Err(error) = request(&api, "PUT", "vm.fork-prepare", Some(fork_request.clone())).await {
        status(
            &job,
            &json!({"state":"failed","phase":"disk_prepare","error":error.to_string(),"source_untouched":true}),
        )?;
        return Err(error.into());
    }
    let disk_prepare_ms = disk_prepare_start.elapsed().as_secs_f64() * 1000.0;
    let preflush_start = Instant::now();
    let preflush = request(&api, "PUT", "vm.preflush", None).await;
    let preflush_ms = preflush_start.elapsed().as_secs_f64() * 1000.0;
    if let Err(error) = preflush {
        tracing::warn!(%error, "live snapshot preflush failed; retaining final checkpoint barrier");
    }
    let pause_start = Instant::now();
    let mut phases = Vec::new();
    let mut last = 0.0;
    macro_rules! phase {
        ($name:expr) => {{
            let end = pause_start.elapsed().as_secs_f64() * 1000.0;
            phases.push(json!({"phase":$name,"start_ms":last,"end_ms":end,"duration_ms":end-last}));
            last = end;
        }};
    }
    let mut overlap = json!({});
    let captured = async {
        sb.fork_agent_control(&token,false).await?;
        phase!("guest_freeze");
        sb.network.as_ref().ok_or_else(||anyhow!("missing network"))?.fork_block(true).await?;
        phase!("network_gate");
        request(&api,"PUT","vm.pause",None).await?;
        phase!("vcpu_device_pause");
        let accepted = request(&api,"PUT","vm.fork",Some(fork_request)).await;
        let deadline=Instant::now()+Duration::from_secs(30);
        loop {
            if let Ok(ops)=request(&api,"GET","vm.fork-status",None).await {
                if let Some(op)=ops.as_array().and_then(|a|a.iter().find(|o|o["child_api_socket"]==child_api.to_string_lossy().as_ref())) {
                    match op["state"].as_str() {
                        Some("Ready"|"ChildExited") => return Ok::<_,anyhow::Error>(op.clone()),
                        Some("Failed") => return Err(anyhow!("capture fork failed: {}",op["error"])),
                        _ => {}
                    }
                } else if accepted.is_err() { return Err(anyhow!("capture request rejected: {accepted:?}")); }
            }
            if Instant::now()>=deadline {return Err(anyhow!("capture readiness timed out"));}
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }.await;
    phase!("fork_and_runtime_rebuild");
    // Release even on a rejected capture. Never publish while this fails.
    // The transport reset fence and Check RPC remain mandatory. Begin them
    // alongside resume, while old-disk durability completes independently.
    let (resumed, prepared) = if captured.is_ok() {
        let ((resumed, resume_ms), (disks, disk_ms), (connection, connection_ms)) = tokio::join!(
            timed(request(&api, "PUT", "vm.resume", None)),
            timed(sb.vm.finish_fork_disks()),
            timed(sb.prepare_live_snapshot_agent()),
        );
        overlap = json!({"vcpu_resume_ms":resume_ms,"disk_retirement_ms":disk_ms,"agent_connect_ms":connection_ms});
        phase!("resume_disks_agent_parallel");
        (resumed, Some((disks, connection)))
    } else {
        let resumed = request(&api, "PUT", "vm.resume", None).await;
        phase!("vcpu_resume");
        (resumed, None)
    };
    let released = async {
        if captured.is_ok() {
            let (disks, connection) = prepared.expect("successful capture prepares both gates");
            disks?;
            sb.install_fork_agent(connection?).await;
        } else {
            sb.reconnect_fork_agent().await?;
        }
        phase!("agent_install_events");
        sb.fork_agent_control(&token, true).await
    }
    .await;
    phase!("guest_release");
    let unblocked = if resumed.is_ok() && released.is_ok() {
        sb.network.as_ref().unwrap().fork_block(false).await
    } else {
        Err(anyhow!("network remains gated because source release failed").into())
    };
    phase!("network_release");
    let _ = last;
    let pause_ms = pause_start.elapsed().as_secs_f64() * 1000.0;
    if captured.is_err() || resumed.is_err() || released.is_err() || unblocked.is_err() {
        let error = format!(
            "capture={captured:?}; resume={resumed:?}; release={released:?}; network={unblocked:?}"
        );
        status(
            &job,
            &json!({"state":"failed","error":error,"pause_ms":pause_ms}),
        )?;
        return Err(anyhow!(error));
    }
    let manifest =
        json!({"version":1,"token":token,"network":sb.network,"tcp_empty_verified":false});
    sb.dump().await?;
    drop(sb);
    let response = json!({"ok":true,"state":"saving","template":key,"pause_ms":pause_ms,"source_running":true,"phases":phases,"parallel":overlap,"preflush_ms":preflush_ms,"disk_prepare_ms":disk_prepare_ms});
    status(&job, &response)?;
    let key = key.to_string();
    tokio::spawn(async move {
        let mut persistence_phases = Vec::new();
        let mut persistence_last = started.elapsed().as_secs_f64() * 1000.0;
        macro_rules! persist_phase {
            ($name:expr) => {{
                let end = started.elapsed().as_secs_f64() * 1000.0;
                persistence_phases.push(json!({"phase":$name,"start_ms":persistence_last,"end_ms":end,"duration_ms":end-persistence_last}));
                persistence_last = end;
            }};
        }
        let save = async {
            let deadline = Instant::now() + Duration::from_secs(300);
            while !pending.join("capture-status.json").exists() {
                if Instant::now() >= deadline {
                    return Err(anyhow!("capture writer timed out"));
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let result: std::result::Result<(), String> =
                serde_json::from_slice(&std::fs::read(pending.join("capture-status.json"))?)?;
            result.map_err(|e| anyhow!(e))?;
            persist_phase!("capture_writer_wait");
            std::fs::remove_file(pending.join("capture-status.json"))?;
            let mut config: Value =
                serde_json::from_slice(&std::fs::read(pending.join("config.json"))?)?;
            for disk in config["disks"].as_array_mut().into_iter().flatten() {
                if let Some(pin) = disk["path"].as_str().and_then(|p| pinned.get(p)) {
                    disk["path"] = json!(pin);
                }
            }
            super::disk_checkpoint::capture_mode(&mut config, &pending, &published, !remote)
                .await?;
            persist_phase!("disk_export");
            std::fs::write(pending.join("config.json"), serde_json::to_vec(&config)?)?;
            std::fs::write(
                pending.join("manifest.json"),
                serde_json::to_vec(&manifest)?,
            )?;
            if remote {
                let reference = store.as_ref().unwrap().publish(&pending).await?;
                persist_phase!("chunk_publish");
                for e in std::fs::read_dir(&pending)? {
                    std::fs::remove_file(e?.path())?;
                }
                std::fs::write(
                    pending.join("chunk-reference.json"),
                    serde_json::to_vec(&reference)?,
                )?;
            }
            let durable = pending.clone();
            tokio::task::spawn_blocking(move || sync(&durable)).await??;
            std::fs::rename(&pending, &published)?;
            std::fs::File::open(&root)?.sync_all()?;
            persist_phase!("sync_and_atomic_publish");
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let _ = persistence_last;
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let value = match save {
            Ok(()) => {
                json!({"state":"durable","template":key,"pause_ms":pause_ms,"durable_ms":elapsed,"storage_mode":if remote{"remote"}else{"local"},"persistence_phases":persistence_phases,"phases":phases,"parallel":overlap,"preflush_ms":preflush_ms,"disk_prepare_ms":disk_prepare_ms})
            }
            Err(e) => {
                json!({"state":"failed","template":key,"pause_ms":pause_ms,"elapsed_ms":elapsed,"error":e.to_string()})
            }
        };
        if let Err(e) = status(&job, &value) {
            tracing::error!(error=%e,"live snapshot status publication failed");
        }
        // Private capture disk names are no longer needed after durable export.
        if value["state"] == "durable" {
            let _ = std::fs::remove_dir_all(&disk_root);
        }
    });
    Ok(response)
}
