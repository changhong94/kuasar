// SPDX-License-Identifier: Apache-2.0
//! File-backed, immutable templates for image-independent runtime slots.
//! Only the privileged adapter calls these endpoints, before publishing a slot.
use std::path::{Path, PathBuf};

use super::*;
use crate::{cloud_hypervisor::live_fork::request, network::Network};

// Optional diagnostics only; disabled for formal latency measurements.
fn process_sample(pid: u64) -> Value {
    let root = PathBuf::from(format!("/proc/{pid}"));
    let mut threads = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root.join("task")) {
        for entry in entries.flatten() {
            let p = entry.path();
            let sched = std::fs::read_to_string(p.join("schedstat")).unwrap_or_default();
            let cpu = sched
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<u64>().ok());
            threads.push(json!({"tid":entry.file_name().to_string_lossy(),
                "name":std::fs::read_to_string(p.join("comm")).unwrap_or_default().trim(),"cpu_ns":cpu}));
        }
    }
    let stat = std::fs::read_to_string(root.join("stat")).unwrap_or_default();
    let fields: Vec<_> = stat
        .rsplit_once(") ")
        .map(|(_, s)| s.split_whitespace().collect())
        .unwrap_or_default();
    json!({"threads":threads,"minor_faults":fields.get(7).and_then(|s|s.parse::<u64>().ok()),
        "major_faults":fields.get(9).and_then(|s|s.parse::<u64>().ok())})
}

fn template_path(req: &Value) -> anyhow::Result<PathBuf> {
    let root = std::env::var("KUASAR_SNAPSHOT_POOL_ROOT")?;
    let key = req["template"]
        .as_str()
        .ok_or_else(|| anyhow!("missing template"))?;
    validate_id(key)?;
    Ok(Path::new(&root).join(key))
}

fn rewrite_mac(snapshot: &mut Value, id: &str, mac: &[u8]) -> anyhow::Result<bool> {
    if let Some(children) = snapshot["snapshots"].as_object_mut() {
        for (name, child) in children {
            if name == id {
                let mut state: Value = serde_json::from_str(
                    child["snapshot_data"]["state"]
                        .as_str()
                        .ok_or_else(|| anyhow!("missing NIC state"))?,
                )?;
                state["config"]["mac"] = json!(mac);
                child["snapshot_data"]["state"] = json!(serde_json::to_string(&state)?);
                return Ok(true);
            }
            if rewrite_mac(child, id, mac)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn seal(path: &Path) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_file() {
            std::fs::File::open(&path)?.sync_all()?;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400))?;
            std::fs::File::open(&path)?.sync_all()?;
        }
    }
    std::fs::File::open(path)?.sync_all()?;
    std::fs::File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

pub(super) async fn handle<F>(handle: &Handle<F>, req: &Value, mode: &str) -> anyhow::Result<Value>
where
    F: VMFactory + Sync + Send + 'static,
    F::VM: VM + Snapshottable + Sync + Send + 'static,
{
    let id = req["sandbox_id"]
        .as_str()
        .ok_or_else(|| anyhow!("missing sandbox_id"))?;
    validate_id(id)?;
    let capture = mode == "capture";
    let directory = if mode == "prepare" {
        PathBuf::new()
    } else {
        template_path(req)?
    };
    let sandbox = handle
        .sandboxes
        .read()
        .await
        .get(id)
        .cloned()
        .ok_or_else(|| anyhow!("sandbox not found"))?;
    let mut sandbox = sandbox.lock().await;
    if !matches!(sandbox.status, SandboxStatus::Running(_))
        || !sandbox.vm.live_fork_agent()
        || !sandbox.containers.is_empty()
    {
        return Err(anyhow!(
            "template operations require a running empty live-fork runtime"
        ));
    }
    let network = sandbox
        .network
        .as_ref()
        .ok_or_else(|| anyhow!("template requires one NIC"))?;
    if network.interfaces().len() != 1 || network.deferred_old_mac.is_some() {
        return Err(anyhow!("template requires completed network preparation"));
    }
    let api = sandbox.vm.live_fork_info()["api_socket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing API socket"))?
        .to_owned();
    let profile = std::env::var_os("KUASAR_SNAPSHOT_PROFILE").is_some();
    let pid = sandbox.vm.live_fork_info()["pid"]
        .as_u64()
        .unwrap_or_default();
    let initial_sample = profile.then(|| process_sample(pid));
    let started = std::time::Instant::now();
    let mut phases = Vec::new();
    let mut last = started;
    let mut mark = |name: &str| {
        let now = std::time::Instant::now();
        phases.push(json!({"phase":name,"ms":(now-last).as_secs_f64()*1000.0,
            "process":profile.then(|| process_sample(pid))}));
        last = now;
    };
    let generic = Path::new(&sandbox.base_dir).join("generic-slot.json");
    let prepared = !capture && mode != "prepare" && generic.exists();
    let current: Value = if prepared {
        serde_json::from_slice(&tokio::fs::read(&generic).await?)?
    } else {
        request(&api, "GET", "vm.info", None).await?["config"].clone()
    };
    if mode == "prepare" {
        tokio::fs::write(&generic, serde_json::to_vec(&current)?).await?;
        std::fs::File::open(&generic)?.sync_all()?;
        std::fs::File::open(generic.parent().unwrap())?.sync_all()?;
        sandbox.network.as_ref().unwrap().fork_block(true).await?;
        sandbox
            .fork_agent_control(&uuid::Uuid::new_v4().to_string(), false)
            .await?;
        request(&api, "PUT", "vm.delete", None).await?;
        request(&api, "PUT", "vmm.prepare", Some(current.clone())).await?;
        sandbox.client.lock().await.take();
        return Ok(json!({"ok":true,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0}));
    }
    mark("slot_configuration");
    if capture {
        if current["disks"]
            .as_array()
            .is_none_or(|ds| ds.iter().any(|d| d["readonly"] != true))
        {
            return Err(anyhow!(
                "template capture requires exclusively read-only disks"
            ));
        }
        tokio::fs::create_dir_all(directory.parent().unwrap()).await?;
        tokio::fs::create_dir(&directory).await?; // Never overwrite a published template.
        let token = uuid::Uuid::new_v4().to_string();
        let tcp_empty_verified = match sandbox.fork_agent_prepare_template(&token).await {
            Ok(verified) => verified,
            Err(error) => {
                let _ = sandbox.fork_agent_control(&token, true).await;
                return Err(error.into());
            }
        };
        let result = async {
            request(&api, "PUT", "vm.pause", None).await?;
            request(
                &api,
                "PUT",
                "vm.snapshot",
                Some(json!({"destination_url": format!("file://{}", directory.display())})),
            )
            .await?;
            let mut config: Value =
                serde_json::from_slice(&tokio::fs::read(directory.join("config.json")).await?)?;
            for (n, disk) in config["disks"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .enumerate()
            {
                let target = directory.join(format!("disk-{n}.raw"));
                // A reflink is independently owned even when the preparation sandbox is destroyed.
                let status = tokio::process::Command::new("cp")
                    .args(["--reflink=auto", "--"])
                    .arg(
                        disk["path"]
                            .as_str()
                            .ok_or_else(|| anyhow!("missing disk path"))?,
                    )
                    .arg(&target)
                    .status()
                    .await?;
                if !status.success() {
                    return Err(anyhow!("template disk reflink failed"));
                }
                disk["path"] = json!(target);
            }
            tokio::fs::write(directory.join("config.json"), serde_json::to_vec(&config)?).await?;
            let manifest = json!({"version":1,"token":token,"network":sandbox.network,"tcp_empty_verified":tcp_empty_verified});
            tokio::fs::write(
                directory.join("manifest.pending"),
                serde_json::to_vec(&manifest)?,
            )
            .await?;
            let dir = directory.clone();
            tokio::task::spawn_blocking(move || seal(&dir)).await??;
            tokio::fs::rename(
                directory.join("manifest.pending"),
                directory.join("manifest.json"),
            )
            .await?;
            std::fs::File::open(&directory)?.sync_all()?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        // A failed snapshot must still release the source's execution barrier.
        let resumed = request(&api, "PUT", "vm.resume", None).await;
        let released = sandbox.fork_agent_control(&token, true).await;
        result?;
        resumed?;
        released?;
    } else {
        let manifest: Value =
            serde_json::from_slice(&tokio::fs::read(directory.join("manifest.json")).await?)?;
        let old: Network = serde_json::from_value(manifest["network"].clone())?;
        let token = manifest["token"]
            .as_str()
            .ok_or_else(|| anyhow!("invalid template token"))?;
        if mode == "stage" && !prepared {
            return Err(anyhow!("metadata staging requires a prepared generic slot"));
        }
        let key = directory
            .file_name()
            .ok_or_else(|| anyhow!("missing template key"))?
            .to_string_lossy();
        let work = Path::new(&sandbox.base_dir).join(format!("image-restore-{key}"));
        let binding = json!({"source":directory,"slot":current});
        let cached = work.exists();
        if cached {
            let found: Value =
                serde_json::from_slice(&tokio::fs::read(work.join("binding.json")).await?)?;
            if found != binding {
                return Err(anyhow!("staged template/slot identity mismatch"));
            }
            // Files were synchronized before atomic publication. Re-sync the
            // parent even on hits to cover a crash between rename and dir sync.
            let parent = PathBuf::from(&sandbox.base_dir);
            tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all()).await??;
            mark("stage_cache_validate");
        } else {
            let published = work.clone();
            let mut config: Value =
                serde_json::from_slice(&tokio::fs::read(directory.join("config.json")).await?)?;
            if config["memory"]["shared"] == true
                || config["memory"]["hugepages"] == true
                || config["memory"].get("zones").is_some_and(|v| !v.is_null())
            {
                return Err(anyhow!("template memory must support private file CoW"));
            }
            // Keep the slot's transport endpoints and TAP, but restore application/CPU state.
            for key in ["net", "vsock", "serial", "console"] {
                config[key] = current[key].clone();
            }
            let work =
                Path::new(&sandbox.base_dir).join(format!(".stage-{}", uuid::Uuid::new_v4()));
            tokio::fs::create_dir(&work).await?; // Each slot is single-use.
            for name in ["memory-ranges"] {
                tokio::fs::symlink(directory.join(name), work.join(name)).await?;
            }
            let mut state: Value =
                serde_json::from_slice(&tokio::fs::read(directory.join("state.json")).await?)?;
            let net = &config["net"][0];
            let mac = net["mac"]
                .as_str()
                .ok_or_else(|| anyhow!("missing slot MAC"))?
                .split(':')
                .map(|v| u8::from_str_radix(v, 16))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if mac.len() != 6
                || !rewrite_mac(&mut state, net["id"].as_str().unwrap_or("fork-net"), &mac)?
            {
                return Err(anyhow!("invalid slot NIC snapshot"));
            }
            mark("stage_parse_patch");
            tokio::fs::write(work.join("state.json"), serde_json::to_vec(&state)?).await?;
            mark("stage_write_state");
            tokio::fs::write(work.join("config.json"), serde_json::to_vec(&config)?).await?;
            mark("stage_write_config");
            tokio::fs::write(work.join("binding.json"), serde_json::to_vec(&binding)?).await?;
            // Both writes finish before either barrier starts. Keep blocking storage
            // work off Tokio workers and allow the filesystem to coalesce flushes.
            // On a sync error, await both jobs before returning so cleanup
            // does not race another sync.
            let state_path = work.join("state.json");
            let config_path = work.join("config.json");
            let (state_sync, config_sync) = tokio::join!(
                tokio::task::spawn_blocking(move || std::fs::File::open(state_path)?.sync_all()),
                tokio::task::spawn_blocking(move || std::fs::File::open(config_path)?.sync_all()),
            );
            state_sync??;
            config_sync??;
            mark("stage_sync_files");
            let durable_work = work.clone();
            tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                std::fs::File::open(durable_work.join("binding.json"))?.sync_all()?;
                std::fs::File::open(&durable_work)?.sync_all()?;
                // Persist the newly created image-restore directory entry too.
                std::fs::File::open(durable_work.parent().unwrap())?.sync_all()
            })
            .await??;
            mark("stage_sync_directory");
            tokio::fs::rename(&work, &published).await?;
            let parent = PathBuf::from(&sandbox.base_dir);
            tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all()).await??;
            mark("stage_publish");
        }
        if mode == "stage" {
            return Ok(
                json!({"ok":true,"cached":cached,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0,"phases":phases}),
            );
        }
        if !prepared {
            sandbox.network.as_ref().unwrap().fork_block(true).await?;
            sandbox
                .fork_agent_control(&uuid::Uuid::new_v4().to_string(), false)
                .await?;
            request(&api, "PUT", "vm.delete", None).await?;
        }
        mark("retire_generic_vm");
        request(
            &api,
            "PUT",
            "vm.restore",
            Some(json!({"source_url":format!("file://{}",work.display()),
            "memory_restore_mode":"CopyOnWrite", "prefault":false})),
        )
        .await?;
        mark("restore_kvm_devices");
        sandbox.dump().await?; // Identity/lifecycle ownership is durable before resume.
        mark("persist_identity");
        // Ownership is durable and the host network gate stays closed. Pipeline
        // identity release immediately after CONNECT while vm.resume completes.
        // Neither transport is published until both branches have succeeded.
        let address = sandbox.vm.socket_address();
        let seed: [u8; 32] = rand::random();
        let ((client, connect_us, identity_us), (_, resume_us)) = tokio::try_join!(
            async {
                let start = std::time::Instant::now();
                let client = crate::client::new_snapshot_sandbox_client(&address).await?;
                let connect_us = start.elapsed().as_micros();
                let start = std::time::Instant::now();
                sandbox.fork_agent_release_network_with_client(&client, token, Some(&old), Some(&seed), manifest["tcp_empty_verified"].as_bool().unwrap_or(false)).await?;
                Ok::<_, anyhow::Error>((client, connect_us, start.elapsed().as_micros()))
            },
            async {
                let start = std::time::Instant::now();
                let result = request(&api, "PUT", "vm.resume", None).await?;
                Ok::<_, anyhow::Error>((result, start.elapsed().as_micros()))
            }
        )?;
        tracing::info!(sandbox_id = %sandbox.id, resume_us, connect_us, identity_us,
            "snapshot resume/connect/identity pipeline");
        *sandbox.client.lock().await = Some(client);
        mark("resume_connect_identity");
        sandbox.forward_events().await;
        sandbox.network.as_ref().unwrap().fork_block(false).await?;
        mark("connect_network");
    }
    Ok(
        json!({"ok":true,"directory":directory,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0,"phases":phases,"initial_process":initial_sample}),
    )
}
