// SPDX-License-Identifier: Apache-2.0
//! Durable same-identity suspension for E2B runtimes. Unlike pool templates,
//! application RAM and processes are captured; no identity reset is performed.
use super::*;
use crate::cloud_hypervisor::live_fork::{request, request_with_timeout};
use std::{path::Path, time::Duration};

async fn ch(api: &str, action: &str, body: Option<Value>) -> anyhow::Result<Value> {
    Ok(request_with_timeout(api, "PUT", action, body, Duration::from_secs(300)).await?)
}

async fn info(api: &str) -> anyhow::Result<Option<Value>> {
    match request(api, "GET", "vm.info", None).await {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.to_string().contains("VM is not created") => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn sync_tree(path: &Path) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() { sync_tree(&path)?; }
        else { std::fs::File::open(&path)?.sync_all()?; }
    }
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

pub(super) async fn handle<F>(handle: &Handle<F>, req: &Value, mode: &str) -> anyhow::Result<Value>
where F: VMFactory + Sync + Send + 'static,
      F::VM: VM + Snapshottable + Sync + Send + 'static,
{
    let mut stage_started = std::time::Instant::now();
    let mut smark = |stage: &str| {
        let now = std::time::Instant::now();
        tracing::info!(target: "checkpoint_profile", sandbox_id = req["sandbox_id"].as_str().unwrap_or(""), mode, stage,
            elapsed_us = now.duration_since(stage_started).as_micros() as u64, "CHECKPOINT_STAGE");
        stage_started = now;
    };
    let _memory_transaction = super::memory_maintenance::transaction().await?;
    smark("maintenance_lock");
    let id = req["sandbox_id"].as_str().ok_or_else(|| anyhow!("missing sandbox_id"))?;
    validate_id(id)?;
    let sandbox = handle.sandboxes.read().await.get(id).cloned()
        .ok_or_else(|| anyhow!("sandbox not found"))?;
    let mut sb = sandbox.lock().await;
    smark("sandbox_lock");
    if !sb.vm.live_fork_agent() || !sb.containers.is_empty() {
        return Err(anyhow!("checkpoint requires an E2B live-fork runtime"));
    }
    let api = sb.vm.live_fork_info()["api_socket"].as_str()
        .ok_or_else(|| anyhow!("missing API socket"))?.to_owned();
    let directory = Path::new(&sb.base_dir).join("durable-checkpoint");
    let started = std::time::Instant::now();
    let remote = handle.snapshot_config.checkpoint_mode == crate::config::CheckpointMode::Remote;
    let store = if remote { super::checkpoint_store::Store::configured()? } else { None };
    if mode == "status" {
        if sb.vm.pids().vmm_pid.is_none() && directory.join("checkpoint.json").exists() {
            return Ok(json!({"ok":true,"status":"paused","controller_missing":true}));
        }
        let running = info(&api).await?.is_some_and(|v| v["state"] == "Running");
        let state = match (directory.join("checkpoint.json").exists(), running) {
            (true, true) => "resuming", (true, false) => "paused",
            (false, true) => "running", (false, false) => "failed",
        };
        return Ok(json!({"ok":true,"status":state}));
    }
    if mode == "recover" {
        if !directory.join("checkpoint.json").exists() {
            return Err(anyhow!("cold recovery requires a durable checkpoint"));
        }
        if sb.vm.pids().vmm_pid.is_none() {
            let netns = req["netns"].as_str().ok_or_else(|| anyhow!("missing recovery namespace"))?;
            let recovered = async {
            sb.vm.launch_checkpoint_controller(netns).await?;
            sb.data.netns = netns.to_string();
            sb.prepare_network().await?;
            sb.network.as_ref().unwrap().fork_block(true).await?;
            // A previously staged restore is tied to the previous host. Preserve
            // the immutable checkpoint; stage a new disk generation on resume.
            let previous = directory.join("restore.json");
            if previous.exists() { tokio::fs::remove_file(previous).await?; }
            let marker = directory.join("cold-network.json");
            tokio::fs::write(&marker, serde_json::to_vec(&sb.vm.live_fork_info()["net"])?).await?;
            std::fs::File::open(&marker)?.sync_all()?;
            let mut manifest: Value = serde_json::from_slice(&tokio::fs::read(directory.join("checkpoint.json")).await?)?;
            manifest["controller_boot_id"] = json!(std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?.trim());
            let next = directory.join("controller.pending");
            tokio::fs::write(&next, serde_json::to_vec(&manifest)?).await?;
            std::fs::File::open(&next)?.sync_all()?;
            tokio::fs::rename(next, directory.join("checkpoint.json")).await?;
            std::fs::File::open(&directory)?.sync_all()?;
            let pid = sb.vm.pids().vmm_pid.ok_or_else(|| anyhow!("recovered controller has no PID"))?;
            sb.status = SandboxStatus::Running(pid);
            sb.dump().await?;
            Ok::<_,anyhow::Error>(())
            }.await;
            if let Err(error) = recovered {
                // Only the newly adopted pidfd can be signalled here. Keep the
                // immutable checkpoint and owned namespace available for retry.
                sb.vm.stop(true).await?;
                sb.vm.forget_checkpoint_process();
                sb.network = None;
                sb.data.netns.clear();
                sb.dump().await?;
                return Err(error);
            }
            // Startup skipped monitoring while the old host PID was invalid.
            // Register the newly created controller with the normal lifecycle.
            monitor(sandbox.clone());
        }
        return Ok(json!({"ok":true,"status":"paused","live_fork":sb.vm.live_fork_info()}));
    }
    if mode == "resume" {
        let mut phase_started = std::time::Instant::now();
        let mut mark = |phase: &str| {
            let now = std::time::Instant::now();
            tracing::info!(target: "checkpoint_timing", sandbox_id = id, phase,
                elapsed_us = now.duration_since(phase_started).as_micros() as u64, "checkpoint resume phase");
            phase_started = now;
        };
        if !directory.exists() && info(&api).await?.is_some_and(|v| v["state"] == "Running") {
            return Ok(json!({"ok":true,"status":"running"}));
        }
        let manifest: Value = serde_json::from_slice(&tokio::fs::read(directory.join("checkpoint.json")).await?)?;
        if manifest["sandbox_id"] != id { return Err(anyhow!("checkpoint identity mismatch")); }
        if (!manifest["chunk_snapshot"].is_null()) != remote {
            return Err(anyhow!("checkpoint storage format differs from configured mode"));
        }
        let payload = if !manifest["chunk_snapshot"].is_null() {
            store.as_ref().ok_or_else(|| anyhow!("Manifest checkpoint requires its configured store"))?
                .materialize_uffd(&directory, &manifest["chunk_snapshot"]).await?
        } else { directory.clone() };
        if remote { super::memory_maintenance::pin(Path::new(&sb.base_dir), "active", &manifest["chunk_snapshot"])?; }
        let token = manifest["token"].as_str().ok_or_else(|| anyhow!("missing checkpoint token"))?;
        // A published checkpoint is the recovery authority. No new VM identity,
        // MAC, CID or image template is substituted on resume.
        let info = info(&api).await?;
        if info.as_ref().is_some_and(|i| i["state"] != "Running") {
            ch(&api, "vm.delete", None).await?;
        }
        mark("reset_controller");
        let restore_manifest = directory.join("restore.json");
        if !restore_manifest.exists() {
            let work = Path::new(&sb.base_dir).join(format!(".checkpoint-restore-{}",uuid::Uuid::new_v4()));
            tokio::fs::create_dir(&work).await?;
            std::fs::set_permissions(&work, std::fs::Permissions::from_mode(0o700))?;
            let mut config: Value = serde_json::from_slice(&tokio::fs::read(payload.join("config.json")).await?)?;
            let cold_network = directory.join("cold-network.json");
            if cold_network.exists() {
                let net: Value = serde_json::from_slice(&tokio::fs::read(cold_network).await?)?;
                // The guest can change its runtime MAC after device creation.
                // Preserve the snapshotted device config/state, and validate the
                // restored host endpoint against the saved runtime identity.
                if net["mac"] != manifest["network"]["intfs"][0]["hwAddr"] {
                    return Err(anyhow!("recovery network identity differs from checkpoint"));
                }
                config["net"][0]["tap"] = net["tap"].clone();
            }
            // The durable checkpoint is authoritative and vm.delete has drained
            // the old writer. Children own hard links; clear only this VM's names
            // so preserved QCOW dependencies can be restored without collisions.
            let disk_dir=Path::new(&sb.base_dir).join("fork-disks");
            if disk_dir.exists() {
                for entry in std::fs::read_dir(&disk_dir)? {
                    let entry=entry?;
                    if !entry.file_type()?.is_file() {return Err(anyhow!("non-regular stale disk dependency"));}
                    std::fs::remove_file(entry.path())?;
                }
                std::fs::File::open(&disk_dir)?.sync_all()?;
            }
            let owned = super::disk_checkpoint::stage(&mut config,
                &Path::new(&sb.base_dir).join("fork-disks"),
                &format!("checkpoint-{}",uuid::Uuid::new_v4()),
                sb.vm.pids().vmm_pid.ok_or_else(|| anyhow!("missing restore controller"))?).await?;
            mark("disk_stage");
            tokio::fs::write(work.join("config.json"),serde_json::to_vec(&config)?).await?;
            tokio::fs::copy(payload.join("state.json"),work.join("state.json")).await?;
            if payload.join("memory-ranges.manifest.json").exists() {
                tokio::fs::copy(payload.join("memory-ranges.manifest.json"),work.join("memory-ranges.manifest.json")).await?;
            } else {
                tokio::fs::symlink(payload.join("memory-ranges"),work.join("memory-ranges")).await?;
            }
            sync_tree(&work)?;
            std::fs::File::open(&sb.base_dir)?.sync_all()?;
            let data=json!({"directory":work,"disk_files":owned});
            let temporary=directory.join("restore.pending");
            tokio::fs::write(&temporary,serde_json::to_vec(&data)?).await?;
            std::fs::File::open(&temporary)?.sync_all()?;
            tokio::fs::rename(temporary,&restore_manifest).await?;
            std::fs::File::open(&directory)?.sync_all()?;
        }
        mark("restore_metadata");
        let restore: Value=serde_json::from_slice(&tokio::fs::read(&restore_manifest).await?)?;
        let work=std::path::PathBuf::from(restore["directory"].as_str().ok_or_else(|| anyhow!("missing restore directory"))?);
        if !info.as_ref().is_some_and(|i| i["state"] == "Running") {
            let pager = if work.join("memory-ranges.manifest.json").exists() {
                Some(store.as_ref().ok_or_else(||anyhow!("missing Manifest store"))?.start_pager(&payload,&work).await?)
            } else { None };
            ch(&api, "vm.restore", Some(json!({"source_url":format!("file://{}", work.display()),
                "memory_restore_mode":if work.join("memory-ranges.manifest.json").exists() { "OnDemand" } else { "CopyOnWrite" }, "prefault":false}))).await?;
            mark("vm_restore");
            // The VMM owns the connected endpoint; disconnect terminates the helper.
            // Reap it even if the later guest handshake fails.
            if let Some(mut pager) = pager { tokio::spawn(async move { let _ = pager.wait().await; }); }
            ch(&api, "vm.resume", None).await?;
            mark("vm_resume");
        }
        let client = crate::client::new_snapshot_sandbox_client(&sb.vm.socket_address()).await?;
        *sb.client.lock().await = Some(client);
        mark("guest_connect");
        if directory.join("cold-network.json").exists() {
            let old: crate::network::Network = serde_json::from_value(manifest["network"].clone())?;
            sb.fork_agent_release_network(token, Some(&old), None).await?;
        } else {
            sb.fork_agent_control(token, true).await?;
        }
        mark("guest_barrier_release");
        sb.forward_events().await;
        sb.network.as_ref().ok_or_else(|| anyhow!("missing network"))?.fork_block(false).await?;
        sb.dump().await?;
        mark("identity_commit");
        // Each child owns hard links in its own directory; removing stale names
        // here cannot remove another VM's live backing files.
        let keep: Vec<std::path::PathBuf> = serde_json::from_value(restore["disk_files"].clone())?;
        if !keep.is_empty() {
            let disks=Path::new(&sb.base_dir).join("fork-disks");
            for entry in std::fs::read_dir(&disks)? {
                let entry=entry?;
                if entry.file_type()?.is_file() && !keep.contains(&entry.path()) {
                    std::fs::remove_file(entry.path())?;
                }
            }
            std::fs::File::open(disks)?.sync_all()?;
        }
        // Native RAM mappings retain file references; a UFFD pager owns its open
        // authenticated Manifest stream independently of these runtime names.
        // Delete the ownership marker only after the running identity is durable.
        let retired = directory.with_file_name(format!(".resumed-{}",uuid::Uuid::new_v4()));
        tokio::fs::rename(&directory, &retired).await?;
        std::fs::File::open(&sb.base_dir)?.sync_all()?;
        tokio::fs::remove_dir_all(&work).await?;
        tokio::fs::remove_dir_all(&retired).await?;
        mark("cleanup");
        return Ok(json!({"ok":true,"status":"running","elapsed_ms":started.elapsed().as_secs_f64()*1000.0}));
    }
    if directory.join("checkpoint.json").exists() {
        let info = info(&api).await?;
        if info.as_ref().is_some_and(|i| i["state"] == "Running") {
            return Err(anyhow!("resume in progress; complete resume before pausing again"));
        }
        if info.is_some() { ch(&api, "vm.delete", None).await?; }
        sb.client.lock().await.take();
        return Ok(json!({"ok":true,"status":"paused","checkpoint":directory,"guest_ram_released":true}));
    }
    if remote && store.is_none() {
        return Err(anyhow!("remote checkpoint requires its configured Manifest store"));
    }
    let current = request(&api, "GET", "vm.info", None).await?;
    if current["state"] != "Running" { return Err(anyhow!("VM is not running")); }
    let network = sb.network.as_ref().ok_or_else(|| anyhow!("missing network"))?;
    if network.deferred_old_mac.is_some() { return Err(anyhow!("network preparation is incomplete")); }
    let pending = Path::new(&sb.base_dir).join(format!(".checkpoint-{}",uuid::Uuid::new_v4()));
    tokio::fs::create_dir(&pending).await?;
    std::fs::set_permissions(&pending, std::fs::Permissions::from_mode(0o700))?;
    let token = uuid::Uuid::new_v4().to_string();
    smark("preflight");
    if let Err(error) = sb.fork_agent_control(&token, false).await {
        let _ = sb.fork_agent_control(&token, true).await;
        let _ = tokio::fs::remove_dir_all(&pending).await;
        return Err(error.into());
    }
    smark("guest_barrier");
    let capture = async {
        sb.network.as_ref().unwrap().fork_block(true).await?;
        ch(&api, "vm.pause", None).await?;
        smark("vcpu_device_pause");
        if remote && std::env::var("KUASAR_INCREMENTAL_MEMORY").as_deref()!=Ok("0") {
            tokio::fs::write(pending.join("allow-memory-delta"), b"").await?;
        }
        ch(&api, "vm.snapshot", Some(json!({"destination_url":format!("file://{}",pending.display())}))).await?;
        smark("ram_capture");
        let mut config: Value=serde_json::from_slice(&tokio::fs::read(pending.join("config.json")).await?)?;
        super::disk_checkpoint::capture_mode(&mut config,&pending,&directory,!remote).await
            .map_err(|e| anyhow!("checkpoint disk capture: {e}"))?;
        if !remote {
            for entry in std::fs::read_dir(&pending)? {
                if super::disk_checkpoint::manifest_reference(&entry?.path())?.is_some() { return Err(anyhow!("local checkpoint cannot contain remote disk dependencies")); }
            }
        }
        tokio::fs::write(pending.join("config.json"),serde_json::to_vec(&config)?).await?;
        let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        let mut manifest=json!({"version":2,"sandbox_id":id,"token":token,"boot_id":boot.trim(),"network":sb.network});
        manifest["storage_mode"] = json!(if remote { "remote" } else { "local" });
        smark("disk_capture");
        if remote {
            manifest["chunk_snapshot"] = store.as_ref().unwrap().publish(&pending).await
                .map_err(|e| anyhow!("checkpoint chunk publication: {e}"))?;
            smark("chunk_publish");
            if std::env::var("KUASAR_MEMORY_TUNING").as_deref() == Ok("0") {
                super::memory_maintenance::pin(Path::new(&sb.base_dir), "checkpoint", &manifest["chunk_snapshot"])?;
            }
            smark("checkpoint_pin");
            // All payloads and the authenticated root index are now durable in
            // Store. Do not retain a second native durable snapshot locally.
            for entry in std::fs::read_dir(&pending)? { std::fs::remove_file(entry?.path())?; }
        }
        tokio::fs::write(pending.join("checkpoint.json"),serde_json::to_vec(&manifest)?).await?;
        let durable=pending.clone();
        tokio::task::spawn_blocking(move || sync_tree(&durable)).await??;
        tokio::fs::rename(&pending,&directory).await?;
        std::fs::File::open(&sb.base_dir)?.sync_all()?;
        Ok::<_,anyhow::Error>(())
    }.await;
    if let Err(error)=capture {
        // Before publication the original VM remains authoritative.
        if !directory.join("checkpoint.json").exists() {
            let resumed=ch(&api,"vm.resume",None).await;
            let released=sb.fork_agent_control(&token,true).await;
            if resumed.is_ok() && released.is_ok() {
                sb.network.as_ref().unwrap().fork_block(false).await?;
                if pending.exists() { tokio::fs::remove_dir_all(&pending).await?; }
            }
        }
        return Err(error);
    }
    // RAM may be released only after all snapshot bytes and directory entries
    // have crossed the persistence barrier. The API process keeps its identity.
    smark("local_commit");
    ch(&api,"vm.delete",None).await?;
    sb.client.lock().await.take();
    smark("ram_release");
    Ok(json!({"ok":true,"status":"paused","checkpoint":directory,
        "guest_ram_released":true,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0}))
}
