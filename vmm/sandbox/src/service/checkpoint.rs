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
    let id = req["sandbox_id"].as_str().ok_or_else(|| anyhow!("missing sandbox_id"))?;
    validate_id(id)?;
    let sandbox = handle.sandboxes.read().await.get(id).cloned()
        .ok_or_else(|| anyhow!("sandbox not found"))?;
    let mut sb = sandbox.lock().await;
    if !sb.vm.live_fork_agent() || !sb.containers.is_empty() {
        return Err(anyhow!("checkpoint requires an E2B live-fork runtime"));
    }
    let api = sb.vm.live_fork_info()["api_socket"].as_str()
        .ok_or_else(|| anyhow!("missing API socket"))?.to_owned();
    let directory = Path::new(&sb.base_dir).join("durable-checkpoint");
    let started = std::time::Instant::now();
    if mode == "status" {
        let running = info(&api).await?.is_some_and(|v| v["state"] == "Running");
        let state = match (directory.join("checkpoint.json").exists(), running) {
            (true, true) => "resuming", (true, false) => "paused",
            (false, true) => "running", (false, false) => "failed",
        };
        return Ok(json!({"ok":true,"status":state}));
    }
    if mode == "resume" {
        if !directory.exists() && info(&api).await?.is_some_and(|v| v["state"] == "Running") {
            return Ok(json!({"ok":true,"status":"running"}));
        }
        let manifest: Value = serde_json::from_slice(&tokio::fs::read(directory.join("checkpoint.json")).await?)?;
        if manifest["sandbox_id"] != id { return Err(anyhow!("checkpoint identity mismatch")); }
        let token = manifest["token"].as_str().ok_or_else(|| anyhow!("missing checkpoint token"))?;
        // A published checkpoint is the recovery authority. No new VM identity,
        // MAC, CID or image template is substituted on resume.
        let info = info(&api).await?;
        if info.as_ref().is_some_and(|i| i["state"] != "Running") {
            ch(&api, "vm.delete", None).await?;
        }
        let restore_manifest = directory.join("restore.json");
        if !restore_manifest.exists() {
            let work = Path::new(&sb.base_dir).join(format!(".checkpoint-restore-{}",uuid::Uuid::new_v4()));
            tokio::fs::create_dir(&work).await?;
            std::fs::set_permissions(&work, std::fs::Permissions::from_mode(0o700))?;
            let mut config: Value = serde_json::from_slice(&tokio::fs::read(directory.join("config.json")).await?)?;
            let owned = super::disk_checkpoint::stage(&mut config,
                &Path::new(&sb.base_dir).join("fork-disks"),
                &format!("checkpoint-{}",uuid::Uuid::new_v4())).await?;
            tokio::fs::write(work.join("config.json"),serde_json::to_vec(&config)?).await?;
            tokio::fs::copy(directory.join("state.json"),work.join("state.json")).await?;
            tokio::fs::symlink(directory.join("memory-ranges"),work.join("memory-ranges")).await?;
            sync_tree(&work)?;
            std::fs::File::open(&sb.base_dir)?.sync_all()?;
            let data=json!({"directory":work,"disk_files":owned});
            let temporary=directory.join("restore.pending");
            tokio::fs::write(&temporary,serde_json::to_vec(&data)?).await?;
            std::fs::File::open(&temporary)?.sync_all()?;
            tokio::fs::rename(temporary,&restore_manifest).await?;
            std::fs::File::open(&directory)?.sync_all()?;
        }
        let restore: Value=serde_json::from_slice(&tokio::fs::read(&restore_manifest).await?)?;
        let work=std::path::PathBuf::from(restore["directory"].as_str().ok_or_else(|| anyhow!("missing restore directory"))?);
        if !info.as_ref().is_some_and(|i| i["state"] == "Running") {
            ch(&api, "vm.restore", Some(json!({"source_url":format!("file://{}", work.display()),
                "memory_restore_mode":"CopyOnWrite", "prefault":false}))).await?;
            ch(&api, "vm.resume", None).await?;
        }
        let client = crate::client::new_snapshot_sandbox_client(&sb.vm.socket_address()).await?;
        *sb.client.lock().await = Some(client);
        sb.fork_agent_control(token, true).await?;
        sb.forward_events().await;
        sb.network.as_ref().ok_or_else(|| anyhow!("missing network"))?.fork_block(false).await?;
        sb.dump().await?;
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
        // Restored private RAM mappings retain their file references after unlink.
        // Delete the ownership marker only after the running identity is durable.
        let retired = directory.with_file_name(format!(".resumed-{}",uuid::Uuid::new_v4()));
        tokio::fs::rename(&directory, &retired).await?;
        std::fs::File::open(&sb.base_dir)?.sync_all()?;
        tokio::fs::remove_dir_all(&work).await?;
        tokio::fs::remove_dir_all(&retired).await?;
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
    let current = request(&api, "GET", "vm.info", None).await?;
    if current["state"] != "Running" { return Err(anyhow!("VM is not running")); }
    let network = sb.network.as_ref().ok_or_else(|| anyhow!("missing network"))?;
    if network.deferred_old_mac.is_some() { return Err(anyhow!("network preparation is incomplete")); }
    let pending = Path::new(&sb.base_dir).join(format!(".checkpoint-{}",uuid::Uuid::new_v4()));
    tokio::fs::create_dir(&pending).await?;
    std::fs::set_permissions(&pending, std::fs::Permissions::from_mode(0o700))?;
    let token = uuid::Uuid::new_v4().to_string();
    if let Err(error) = sb.fork_agent_control(&token, false).await {
        let _ = sb.fork_agent_control(&token, true).await;
        let _ = tokio::fs::remove_dir_all(&pending).await;
        return Err(error.into());
    }
    let capture = async {
        sb.network.as_ref().unwrap().fork_block(true).await?;
        ch(&api, "vm.pause", None).await?;
        ch(&api, "vm.snapshot", Some(json!({"destination_url":format!("file://{}",pending.display())}))).await?;
        let mut config: Value=serde_json::from_slice(&tokio::fs::read(pending.join("config.json")).await?)?;
        super::disk_checkpoint::capture(&mut config,&pending,&directory).await?;
        tokio::fs::write(pending.join("config.json"),serde_json::to_vec(&config)?).await?;
        let manifest=json!({"version":1,"sandbox_id":id,"token":token});
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
    ch(&api,"vm.delete",None).await?;
    sb.client.lock().await.take();
    Ok(json!({"ok":true,"status":"paused","checkpoint":directory,
        "guest_ram_released":true,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0}))
}
