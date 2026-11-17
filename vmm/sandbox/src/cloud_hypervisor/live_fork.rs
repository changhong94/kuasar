// Copyright 2026 The Kuasar Authors.
// SPDX-License-Identifier: Apache-2.0
//! Restricted same-host fork with optional independently reconnected guest transport.
use std::{
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use containerd_sandbox::error::Result;
use nix::libc;
use serde_json::{json, Value};
use time::OffsetDateTime;
use tokio::{
    io::{unix::AsyncFd, AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    sync::watch::channel,
};

use super::CloudHypervisorVM;

/// Always reconnect: fork closes the old HTTP connections in both branches.
pub(super) async fn request(
    path: &str,
    method: &str,
    endpoint: &str,
    body: Option<Value>,
) -> Result<Value> {
    let start = Instant::now();
    let operation = async {
        let mut socket = UnixStream::connect(path).await?;
        let body = body.map(|v| v.to_string()).unwrap_or_default();
        socket.write_all(format!(
            "{method} /api/v1/{endpoint} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{}",
            body.len(), body).as_bytes()).await?;
        read_response(&mut socket).await
    };
    let result = tokio::time::timeout(Duration::from_secs(3), operation)
        .await
        .map_err(|_| anyhow!("Cloud Hypervisor {} request timed out", endpoint))?;
    if std::env::var_os("KUASAR_RPC_TIMING").is_some() {
        tracing::info!(
            endpoint,
            milliseconds = start.elapsed().as_secs_f64() * 1000.0,
            "fork timing: async CH request"
        );
    }
    result
}

// CH uses Content-Length for bodies; HTTP 204 has no body or length. Bound headers
// and bodies, reject truncated/error responses, and never replay mutations.
async fn read_response(socket: &mut UnixStream) -> Result<Value> {
    let mut bytes = Vec::new();
    let header_end = loop {
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            if end > 16384 {
                return Err(anyhow!("CH response headers too large").into());
            }
            break end + 4;
        }
        if bytes.len() > 16384 {
            return Err(anyhow!("CH response headers too large").into());
        }
        let mut chunk = [0u8; 4096];
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Err(anyhow!("Truncated CH response headers").into());
        }
        bytes.extend_from_slice(&chunk[..n]);
    };
    let header = std::str::from_utf8(&bytes[..header_end]).map_err(|e| anyhow!(e))?;
    let mut lines = header.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let mut status = status_line.split_whitespace();
    if status.next() != Some("HTTP/1.1") {
        return Err(anyhow!("Invalid CH HTTP version").into());
    }
    let status: u16 = status
        .next()
        .ok_or_else(|| anyhow!("Missing CH HTTP status"))?
        .parse()
        .map_err(|e| anyhow!("Invalid CH HTTP status: {}", e))?;
    let mut length = None;
    for line in lines.filter(|line| !line.is_empty()) {
        let (key, value) = line
            .split_once(':')
            .ok_or_else(|| anyhow!("Invalid CH header"))?;
        if key.eq_ignore_ascii_case("transfer-encoding") {
            return Err(anyhow!("Unsupported CH transfer encoding").into());
        }
        if key.eq_ignore_ascii_case("content-length") {
            if length.is_some() {
                return Err(anyhow!("Duplicate CH Content-Length").into());
            }
            length = Some(value.trim().parse::<usize>().map_err(|e| anyhow!(e))?);
        }
    }
    let length = if status == 204 {
        if length.is_some_and(|n| n != 0) {
            return Err(anyhow!("Nonempty CH 204 response").into());
        }
        0
    } else {
        length.ok_or_else(|| anyhow!("Missing CH Content-Length"))?
    };
    if length > 16 * 1024 * 1024 {
        return Err(anyhow!("CH response body too large").into());
    }
    let end = header_end + length;
    if bytes.len() > end {
        return Err(anyhow!("Trailing CH response data").into());
    }
    let present = bytes.len();
    bytes.resize(end, 0);
    socket.read_exact(&mut bytes[present..]).await?;
    let body = &bytes[header_end..];
    if !(200..300).contains(&status) {
        return Err(anyhow!(
            "Cloud Hypervisor HTTP {}: {}",
            status,
            String::from_utf8_lossy(body)
        )
        .into());
    }
    if body.is_empty() {
        Ok(Value::Null)
    } else {
        serde_json::from_slice(body).map_err(|e| anyhow!("decode CH response: {}", e).into())
    }
}

pub(super) async fn wait_disk_retirement(path: &str) -> Result<()> {
    let operation = async {
        loop {
            let ops = request(path, "GET", "vm.fork-status", None).await?;
            let op = ops
                .as_array()
                .and_then(|ops| ops.last())
                .ok_or_else(|| anyhow!("Missing fork operation for disk completion"))?;
            match op.get("disk_retirement") {
                // Older CH versions complete all disk barriers before Ready.
                None => return Ok(()),
                Some(value) if value.get("Ok").is_some() => return Ok(()),
                Some(value) if value.get("Err").is_some() => {
                    return Err(anyhow!("Fork disk retirement: {}", value["Err"]).into())
                }
                _ => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(45), operation)
        .await
        .map_err(|_| anyhow!("Timed out awaiting fork disk durability"))?
}

impl CloudHypervisorVM {
    pub(super) fn fork_serial_path(&self) -> String {
        format!("{}/serial.sock", self.base_dir)
    }

    /// pidfds work for a forked VMM that is NOT a child of the sandboxer. They
    /// also prevent accidentally signaling a recycled PID after parent exit.
    pub(super) async fn monitor_pidfd(&mut self, pid: u32) -> Result<()> {
        // SAFETY: pidfd_open has scalar arguments; successful fd ownership is
        // transferred exactly once to OwnedFd below.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            return Err(anyhow!("pidfd_open {}: {}", pid, std::io::Error::last_os_error()).into());
        }
        // SAFETY: fd was returned by pidfd_open and is exclusively owned here.
        let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
        let start_time = procfs::process::Process::new(pid as i32)
            .and_then(|p| p.stat())
            .map_err(|e| anyhow!("inspect VMM PID: {}", e))?
            .starttime;
        if self.pid_start_time.is_some_and(|saved| saved != start_time) {
            return Err(anyhow!("VMM PID has been reused; refusing adoption").into());
        }
        self.pid_start_time = Some(start_time);
        let fd = Arc::new(AsyncFd::new(fd)?);
        self.pidfd = Some(Arc::clone(&fd));
        self.pids.vmm_pid = Some(pid);
        let (tx, rx) = channel((0u32, 0i128));
        self.wait_chan = Some(rx);
        tokio::spawn(async move {
            if fd.readable().await.is_ok() {
                let _ = tx.send((0, OffsetDateTime::now_utc().unix_timestamp_nanos()));
            }
        });
        // Fork/recovery use the serialized PID + starttime in sandbox.json.
        // The pidfd owns monitoring; a duplicate PID file is neither read by
        // recovery nor needed to signal this process. Do not sync it while both
        // guests are paused (or rewrite it on every recovery).
        Ok(())
    }

    pub(super) async fn stop_fork_vm(&mut self, force: bool) -> Result<()> {
        if self.pids.vmm_pid.is_none() {
            if let Some(source) = self.fork_source_api.clone() {
                let operations = request(&source, "GET", "vm.fork-status", None).await?;
                if let Some(op) = operations.as_array().and_then(|ops| {
                    ops.iter().rev().find(|op| {
                        op["child_api_socket"] == self.config.api_socket
                            && !self
                                .fork_previous_operations
                                .iter()
                                .any(|id| op["operation_id"] == *id)
                    })
                }) {
                    match op["state"].as_str() {
                        Some("Failed" | "ChildExited") => return Ok(()),
                        Some("Ready") => {
                            let pid = op["child_pid"]
                                .as_u64()
                                .filter(|p| *p > 1 && *p <= u32::MAX as u64)
                                .ok_or_else(|| anyhow!("invalid child PID during cleanup"))?
                                as u32;
                            self.monitor_pidfd(pid).await?;
                        }
                        _ => {
                            return Err(anyhow!(
                                "fork still pending; retaining sandbox for reconciliation"
                            )
                            .into())
                        }
                    }
                } else {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }
        if self.pidfd.is_none() {
            self.monitor_pidfd(self.pid()?).await?;
        }
        let fd = self.pidfd.as_ref().unwrap();
        let signal = if force { libc::SIGKILL } else { libc::SIGTERM };
        // SAFETY: pidfd is held by Arc throughout the syscall; no siginfo pointer.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                fd.get_ref().as_raw_fd(),
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if ret < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            return Err(std::io::Error::last_os_error().into());
        }
        self.wait_stop(Duration::from_secs(10)).await?;
        self.client = None;
        self.pidfd = None;
        self.pids.vmm_pid = None;
        Ok(())
    }

    pub(super) async fn fork_live_vm(&mut self, parent: &mut Self) -> Result<u32> {
        if !self.live_fork || !parent.live_fork || self.pids.vmm_pid.is_some() {
            return Err(
                anyhow!("both sandboxes must use live_fork, and child must be unstarted").into(),
            );
        }
        let parent_info = request(&parent.config.api_socket, "GET", "vm.info", None).await?;
        if parent_info["state"] != "Running" {
            return Err(anyhow!("source VM must be Running").into());
        }
        let child_api = self.config.api_socket.clone();
        let child_serial = self.fork_serial_path();
        self.config = parent.config.clone();
        self.config.api_socket = child_api.clone();
        self.devices.clear();
        self.fork_parent = Some(parent.id.clone());
        parent.client = None;
        let previous = request(&parent.config.api_socket, "GET", "vm.fork-status", None).await?;
        self.fork_previous_operations = previous
            .as_array()
            .ok_or_else(|| anyhow!("invalid fork operation log"))?
            .iter()
            .filter_map(|op| op["operation_id"].as_str().map(str::to_owned))
            .collect();
        self.fork_source_api = Some(parent.config.api_socket.clone());
        // Never replay a mutation after a broken response: reconcile the unique
        // child path against the source's operation log instead.
        let writable = parent_info["config"]["disks"]
            .as_array()
            .is_some_and(|disks| disks.iter().any(|disk| disk["readonly"] == false));
        let accepted = request(
            &parent.config.api_socket,
            "PUT",
            "vm.fork",
            Some(json!({
                "child_api_socket": child_api, "child_serial_socket": child_serial,
                "parent_disk_dir": writable.then(|| format!("{}/fork-disks", parent.base_dir)),
                "child_disk_dir": writable.then(|| format!("{}/fork-disks", self.base_dir)),
                "child_net": self.fork_net,
                "child_netns": self.fork_net.as_ref().map(|_| self.netns.clone()),
                "child_vsock": self.fork_cid.map(|cid| json!({"cid": cid,
                    "socket": format!("{}/{}", self.base_dir, crate::vm::TASK_VSOCK_FILENAME)})),
            })),
        )
        .await;
        let started = Instant::now();
        loop {
            if let Ok(operations) =
                request(&parent.config.api_socket, "GET", "vm.fork-status", None).await
            {
                if let Some(operation) = operations.as_array().and_then(|ops| {
                    ops.iter().rev().find(|op| {
                        op["child_api_socket"] == child_api
                            && !self
                                .fork_previous_operations
                                .iter()
                                .any(|id| op["operation_id"] == *id)
                    })
                }) {
                    self.fork_operation = Some(operation.clone());
                    match operation["state"].as_str().unwrap_or("") {
                        "Ready" => {
                            let pid = operation["child_pid"]
                                .as_u64()
                                .filter(|pid| *pid > 1 && *pid <= u32::MAX as u64)
                                .ok_or_else(|| anyhow!("fork returned invalid child PID"))?
                                as u32;
                            self.monitor_pidfd(pid).await?;
                            self.fork_source_api = None;
                            request(&child_api, "GET", "vm.info", None).await?;
                            return Ok(pid);
                        }
                        "Failed" | "ChildExited" => {
                            return Err(anyhow!("VM fork failed: {}", operation).into())
                        }
                        _ => {}
                    }
                } else if accepted.is_err() {
                    return Err(anyhow!("fork was not accepted: {:?}", accepted.err()).into());
                }
            }
            if started.elapsed() > Duration::from_secs(45) {
                return Err(anyhow!(
                    "fork readiness timed out; operation={:?}",
                    self.fork_operation
                )
                .into());
            }
            // Read-only reconciliation can poll closely during the short runtime
            // reconstruction window. Back off for slow/error paths; never replay
            // the vm.fork mutation when a response or socket is lost.
            let interval = if started.elapsed() < Duration::from_millis(250) {
                1
            } else {
                20
            };
            tokio::time::sleep(Duration::from_millis(interval)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn async_http_handles_fragmented_and_invalid_responses() {
        async fn response(parts: Vec<&'static [u8]>) -> Result<Value> {
            let (mut client, mut server) = UnixStream::pair().unwrap();
            let send = tokio::spawn(async move {
                for part in parts {
                    if server.write_all(part).await.is_err() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            });
            let value = read_response(&mut client).await;
            send.await.unwrap();
            value
        }
        assert_eq!(
            response(vec![
                b"HTTP/1.1 200 OK\r\nContent-Len",
                b"gth: 7\r\n\r\n{\"x\":",
                b"1}"
            ])
            .await
            .unwrap(),
            json!({"x":1})
        );
        assert_eq!(
            response(vec![
                b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"
            ])
            .await
            .unwrap(),
            Value::Null
        );
        assert_eq!(
            response(vec![
                b"HTTP/1.1 204 No Content\r\nServer: Cloud Hypervisor\r\n\r\n"
            ])
            .await
            .unwrap(),
            Value::Null
        );
        for input in [
            &b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\n\r\n"[..],
            &b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\n{}"[..],
            &b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n"[..],
            &b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n"[..],
            &b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
        ] {
            assert!(response(vec![input]).await.is_err());
        }
    }
    #[tokio::test]
    async fn pidfd_adoption_needs_no_duplicate_pid_file() {
        let mut vm = CloudHypervisorVM::default();
        vm.base_dir = "/nonexistent/kuasar-pidfd-adoption".into();
        vm.monitor_pidfd(std::process::id()).await.unwrap();
        assert_eq!(vm.pids.vmm_pid, Some(std::process::id()));
        assert!(vm.pid_start_time.is_some());
        let saved = serde_json::to_vec(&vm).unwrap();
        let mut recovered: CloudHypervisorVM = serde_json::from_slice(&saved).unwrap();
        recovered.monitor_pidfd(std::process::id()).await.unwrap();
        assert_eq!(recovered.pid_start_time, vm.pid_start_time);
        recovered.pid_start_time = Some(0);
        assert!(recovered.monitor_pidfd(std::process::id()).await.is_err());
    }
}
