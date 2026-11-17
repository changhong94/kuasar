/*
Copyright 2022 The Kuasar Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::{
    collections::HashMap,
    ops::Add,
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use containerd_sandbox::PodSandboxConfig;
use containerd_shim::{
    error::Result, protos::protobuf::MessageDyn, util::convert_to_any, Error, TtrpcContext,
    TtrpcResult,
};
use log::debug;
use nix::{
    sys::time::{TimeSpec, TimeValLike},
    time::{clock_gettime, clock_settime, ClockId},
};
use tokio::sync::{mpsc::Receiver, Mutex};
use vmm_common::{
    api,
    api::{
        empty::Empty,
        events::Envelope,
        sandbox::{
            AdoptContainerRequest, AdoptContainerResponse, CheckInjectSocketRequest,
            CheckInjectSocketResponse, CheckRequest, ContainerInjectResult, ContainerTask,
            ExecVMProcessRequest, ExecVMProcessResponse, InjectPhase, InjectStatus,
            InjectTaskRequest, InjectTaskResponse, ReseedEntropyRequest, SetupSandboxRequest,
            SyncClockPacket, TargetCheckResult, TargetCheckStatus, UpdateInterfacesRequest,
            UpdateRoutesRequest,
        },
    },
    warmfork::message::{
        read_framed_message, write_framed_message, PreparePayload, SandboxerMessage,
        WorkloadMessage, MAX_FRAME_LEN, PROTOCOL_VERSION,
    },
};

#[cfg(not(feature = "youki"))]
use crate::container::KuasarContainer;
#[cfg(feature = "youki")]
use crate::youki::YoukiContainer as KuasarContainer;
use crate::{
    netlink::Handle,
    sandbox::{setup_sandbox, SandboxResources},
    NAMESPACE,
};

pub struct SandboxService {
    pub connections: ttrpc::asynchronous::ConnectionTracker,
    pub fork_gate: Arc<crate::fork::ForkGate>,
    tcp_template: Arc<crate::tcp_template::TemplateTcp>,
    fork_control_lock: Mutex<()>,
    pub namespace: String,
    pub handle: Arc<Mutex<Handle>>,
    #[allow(clippy::type_complexity)]
    pub rx: Arc<Mutex<Receiver<(String, Box<dyn MessageDyn>)>>>,
    /// Shared container map with TaskService — used by adopt_container to read orphan PIDs.
    pub containers: Arc<Mutex<HashMap<String, KuasarContainer>>>,
    /// Shared sandbox resources — used to register pending adoptions.
    pub sandbox_resources: Arc<Mutex<SandboxResources>>,
}

impl SandboxService {
    pub fn new(
        rx: Receiver<(String, Box<dyn MessageDyn>)>,
        containers: Arc<Mutex<HashMap<String, KuasarContainer>>>,
        sandbox_resources: Arc<Mutex<SandboxResources>>,
    ) -> Result<Self> {
        let handle = Handle::new()?;
        Ok(Self {
            fork_gate: Arc::default(),
            tcp_template: Arc::default(),
            fork_control_lock: Mutex::new(()),
            connections: Default::default(),
            namespace: NAMESPACE.to_string(),
            handle: Arc::new(Mutex::new(handle)),
            rx: Arc::new(Mutex::new(rx)),
            containers,
            sandbox_resources,
        })
    }

    pub(crate) async fn handle_localhost(&self) -> Result<()> {
        self.handle.lock().await.enable_lo().await
    }
}

#[async_trait]
impl api::sandbox_ttrpc::SandboxService for SandboxService {
    async fn fork_control(
        &self,
        _ctx: &TtrpcContext,
        req: api::sandbox::ForkControlRequest,
    ) -> TtrpcResult<api::sandbox::ForkControlResponse> {
        let _control = self.fork_control_lock.lock().await;
        let started = std::time::Instant::now();
        let mut previous = started;
        let mut timing = Vec::new();
        let mut identity_work = None;
        let mut mark = |phase: &str| {
            let now = std::time::Instant::now();
            timing.push((phase.to_owned(), (now-previous).as_micros() as u64));
            previous = now;
        };
        if !req.entropy.is_empty() && (!req.release || req.entropy.len() != 32) {
            return Err(ttrpc::Error::Others("Fork entropy requires release and 32 bytes".into()));
        }
        if req.prepare_tcp_empty && req.release {
            return Err(ttrpc::Error::Others("TCP preparation requires a closed barrier".into()));
        }
        let certificate = if req.skip_tcp_if_certified {
            self.tcp_template.certificate(&req.token).await
        } else { None };
        let skip_tcp = certificate.is_some();
        let reset_network = req.release && !req.old_mac.is_empty();
        if !req.entropy.is_empty() || reset_network {
            // Validate ownership once, and keep both gates closed on any error.
            self.fork_gate.control(&req.token, false).await?;
        }
        if reset_network {
            let seed = req.entropy.clone();
            let queued = std::time::Instant::now();
            let cleanup = tokio::task::spawn_blocking(move || -> std::io::Result<(u64, u64, u64)> {
                let queue_us = queued.elapsed().as_micros() as u64;
                let start = std::time::Instant::now();
                if !seed.is_empty() { write_entropy_sync(&seed)?; }
                let entropy_us = start.elapsed().as_micros() as u64;
                let start = std::time::Instant::now();
                if let Some(state) = certificate { crate::tcp_template::check_frozen(&state)?; }
                else { crate::tcp_reset::reset()?; }
                Ok((queue_us, entropy_us, start.elapsed().as_micros() as u64))
            });
            let mut handle = self.handle.lock().await;
            // join! (not try_join!) drains the blocking job even if lookup fails.
            // A failed lookup never races later cleanup with a released gate.
            let (cleaned, link) = tokio::join!(cleanup, handle.find_fork_interface(&req.old_mac, &req.interfaces));
            let (queue_us, entropy_us, tcp_us) = cleaned
                .map_err(|e| Error::Other(format!("identity cleanup worker failed: {}", e)))?
                .map_err(|e| Error::Other(format!("identity cleanup failed: {}", e)))?;
            let link = link?;
            mark("cleanup_and_link_lookup");
            identity_work = Some((queue_us, entropy_us, tcp_us));
            handle.apply_fork_interface(req.interfaces.clone(), link).await?;
            mark("interface");
            handle.update_routes(req.routes.clone()).await?;
            mark("routes");
        } else if !req.entropy.is_empty() {
            write_entropy(&req.entropy).await?;
            mark("entropy");
        }
        if req.release {
            // Transport reset can leave old-CID vsock readers asleep in epoll.
            // Wake only registered, already-disconnected sockets; their owners
            // perform cancellation and close. Newly connected RPCs stay alive.
            let count = self
                .connections
                .wake_disconnected()
                .map_err(|e| Error::Other(format!("wake reset RPC transports: {}", e)))?;
            debug!("fork woke {} disconnected RPC transports", count);
        }
        if req.release {
            self.tcp_template.release(&req.token).await?;
            mark("template_thaw");
        }
        self.fork_gate.control(&req.token, req.release).await?;
        let mut response = api::sandbox::ForkControlResponse::new();
        response.entropy_applied = !req.entropy.is_empty();
        response.tcp_scan_skipped = reset_network && skip_tcp;
        if req.prepare_tcp_empty {
            response.tcp_empty_verified = self.tcp_template.prepare(&req.token).await?;
        }
        mark("release");
        if let Some((queue_us, entropy_us, tcp_us)) = identity_work {
            // Submeasurements overlap cleanup_and_link_lookup; do not sum them.
            timing.extend([("worker_queue".into(), queue_us),
                           ("entropy_work".into(), entropy_us),
                           ("tcp_reset_work".into(), tcp_us)]);
        }
        if req.release {
            response.timing_json = serde_json::to_string(&timing).unwrap_or_default();
        }
        if !req.release {
            let containers = self.containers.lock().await;
            for (id, c) in containers.iter() {
                response.containers.push(api::sandbox::ForkContainer {
                    id: id.clone(),
                    pid: c.init.pid as u32,
                    processes: c
                        .processes
                        .iter()
                        .map(|(id, p)| api::sandbox::ForkProcess {
                            id: id.clone(),
                            pid: p.pid as u32,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                });
            }
        }
        Ok(response)
    }

    async fn update_interfaces(
        &self,
        _ctx: &TtrpcContext,
        req: UpdateInterfacesRequest,
    ) -> TtrpcResult<Empty> {
        self.handle
            .lock()
            .await
            .update_interfaces(req.interfaces)
            .await?;
        Ok(Empty::new())
    }

    async fn update_routes(
        &self,
        _ctx: &TtrpcContext,
        req: UpdateRoutesRequest,
    ) -> TtrpcResult<Empty> {
        self.handle.lock().await.update_routes(req.routes).await?;
        Ok(Empty::new())
    }

    async fn setup_sandbox(
        &self,
        _ctx: &TtrpcContext,
        req: SetupSandboxRequest,
    ) -> TtrpcResult<Empty> {
        let _fork_guard = self.fork_gate.enter()?;
        match req.config.type_url.as_str() {
            "PodSandboxConfig" => {
                let config =
                    serde_json::from_slice::<PodSandboxConfig>(req.config.value.as_slice())
                        .map_err(|e| {
                            ttrpc::Error::Others(format!("convert PodSandboxConfig failed: {}", e))
                        })?;
                setup_sandbox(&config).await?;
            }
            _ => {
                return Err(ttrpc::Error::RpcStatus(ttrpc::get_status(
                    ::ttrpc::Code::NOT_FOUND,
                    format!(
                        "SetUpSandbox/config/{} is not supported",
                        &req.config.type_url
                    ),
                )));
            }
        }

        // Set interfaces
        self.handle
            .lock()
            .await
            .update_interfaces(req.interfaces)
            .await?;

        // Set Routes
        self.handle.lock().await.update_routes(req.routes).await?;

        Ok(Empty::new())
    }

    async fn check(&self, _ctx: &TtrpcContext, _req: CheckRequest) -> TtrpcResult<Empty> {
        Ok(Empty::new())
    }

    async fn exec_vm_process(
        &self,
        _ctx: &TtrpcContext,
        req: ExecVMProcessRequest,
    ) -> TtrpcResult<ExecVMProcessResponse> {
        let _exec_guard = self.tcp_template.exec_gate.clone().try_read_owned()
            .map_err(|_| ttrpc::Error::Others("template workload is frozen".into()))?;
        let (out, stderr, code, timing) = execute_result(&req).await?;
        let mut resp = ExecVMProcessResponse::new();
        resp.out = out;
        if req.structured_output {
            resp.stderr = stderr;
            resp.exit_code = code;
            resp.pid = timing.pid;
            resp.structured_output = true;
        }
        if req.profile {
            resp.timing_json =
                serde_json::to_string(&timing).map_err(|e| Error::Other(e.to_string()))?;
        }
        Ok(resp)
    }

    async fn exec_vm_process_streaming(
        &self,
        _ctx: &TtrpcContext,
        req: ExecVMProcessRequest,
        stream: ttrpc::r#async::ServerStreamSender<api::sandbox::ExecVMProcessEvent>,
    ) -> TtrpcResult<()> {
        let exec_guard = self.tcp_template.exec_gate.clone().try_read_owned()
            .map_err(|_| ttrpc::Error::Others("template workload is frozen".into()))?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        // Keep draining/reaping even if the transport is cancelled. The bounded
        // channel closes on disconnect; no output accumulates for absent clients.
        let running = tokio::spawn(async move {
            let _exec_guard = exec_guard;
            let mut cmd = process_command(&req.command, &req.argv)?;
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
            cmd.stdin(if req.stdin.is_empty() { Stdio::null() } else { Stdio::piped() });
            let mut timing = crate::util::ProcessTiming::default();
            let (_, _, code) = crate::util::spawn_and_wait_stream(
                cmd, &req.stdin, Some(&mut timing), Some(&tx)).await?;
            Ok::<_, Error>((code, timing, req.profile))
        });
        let mut transport_error = None;
        while let Some(event) = rx.recv().await {
            let mut response = api::sandbox::ExecVMProcessEvent::new();
            match event {
                crate::util::ProcessEvent::Start(pid) => response.pid = pid,
                crate::util::ProcessEvent::Stdout(bytes) => response.stdout = bytes,
                crate::util::ProcessEvent::Stderr(bytes) => response.stderr = bytes,
            }
            if let Err(error) = stream.send(&response).await {
                transport_error = Some(error);
                break;
            }
        }
        drop(rx);
        let (code, timing, profile) = running.await
            .map_err(|e| Error::Other(format!("process worker failed: {e}")))??;
        if let Some(error) = transport_error { return Err(error); }
        let mut response = api::sandbox::ExecVMProcessEvent::new();
        response.exited = true;
        response.exit_code = code;
        if profile {
            response.timing_json = serde_json::to_string(&timing)
                .map_err(|e| Error::Other(e.to_string()))?;
        }
        stream.send(&response).await
    }

    async fn sync_clock(
        &self,
        _ctx: &TtrpcContext,
        req: SyncClockPacket,
    ) -> TtrpcResult<SyncClockPacket> {
        let mut resp = req.clone();
        let clock_id = ClockId::from_raw(nix::libc::CLOCK_REALTIME);
        match req.Delta {
            0 => {
                resp.ClientArriveTime = clock_gettime(clock_id)
                    .map_err(Error::Nix)?
                    .num_nanoseconds();
                resp.ServerSendTime = clock_gettime(clock_id)
                    .map_err(Error::Nix)?
                    .num_nanoseconds();
            }
            _ => {
                let mut clock_spce = clock_gettime(clock_id).map_err(Error::Nix)?;
                clock_spce = clock_spce.add(TimeSpec::from_duration(Duration::from_nanos(
                    req.Delta as u64,
                )));
                clock_settime(clock_id, clock_spce).map_err(Error::Nix)?;
            }
        }
        Ok(resp)
    }

    async fn get_events(&self, _ctx: &TtrpcContext, _: Empty) -> TtrpcResult<Envelope> {
        // Freeze actively cancels an idle poll, but still waits for its read
        // guard to drain before RAM can be forked. The normal timeout stays 200 ms.
        let event = self.fork_gate.wait_event(&self.rx).await?;
        if let Some((topic, event)) = event {
            debug!("received event {:?}", event);
            // Forward the full Task lifecycle. The host scopes these events to
            // the containerd namespace bound to this particular VM instance.
            let mut resp = Envelope::new();
            resp.set_timestamp(SystemTime::now().into());
            resp.set_namespace(self.namespace.to_string());
            resp.set_topic(topic);
            resp.set_event(convert_to_any(event).unwrap());
            return Ok(resp);
        }

        Err(ttrpc::Error::Others("internal".to_string()))
    }

    async fn reseed_entropy(
        &self,
        _ctx: &TtrpcContext,
        req: ReseedEntropyRequest,
    ) -> TtrpcResult<Empty> {
        write_entropy(&req.seed).await?;
        Ok(Empty::new())
    }

    async fn inject_task(
        &self,
        _ctx: &TtrpcContext,
        req: InjectTaskRequest,
    ) -> TtrpcResult<InjectTaskResponse> {
        let _fork_guard = self.fork_gate.enter()?;
        use std::time::Duration;

        use tokio::time::timeout;

        let prepare_timeout = Duration::from_millis(if req.prepare_timeout_ms > 0 {
            req.prepare_timeout_ms
        } else {
            10_000
        } as u64);
        let commit_timeout = Duration::from_millis(if req.commit_timeout_ms > 0 {
            req.commit_timeout_ms
        } else {
            5_000
        } as u64);

        // Snapshot the container PID map under a single lock before spawning concurrent futures.
        let pids: std::collections::HashMap<String, i32> = {
            let containers = self.containers.lock().await;
            containers
                .iter()
                .map(|(id, c)| (id.clone(), c.init.pid))
                .collect()
        };

        // Autonomous mode: empty task_id signals that the workload self-starts after COMMIT.
        // Skip the PREPARE/READY exchange entirely and send COMMIT immediately.
        let autonomous = req.tasks.iter().all(|t| t.task_id.is_empty());

        if autonomous {
            // ── Autonomous: CAPABILITIES → COMMIT → STARTED ────────────────────
            let auto_futs: Vec<_> = req
                .tasks
                .iter()
                .map(|task| {
                    let task = task.clone();
                    let pids = pids.clone();
                    async move { inject_autonomous_one(&task, &pids).await }
                })
                .collect();

            let auto_outcomes =
                match timeout(commit_timeout, futures::future::join_all(auto_futs)).await {
                    Ok(outcomes) => outcomes,
                    Err(_) => {
                        return Ok(InjectTaskResponse {
                            results: req
                                .tasks
                                .iter()
                                .map(|t| ContainerInjectResult {
                                    container_id: t.container_id.clone(),
                                    status: InjectStatus::INJECT_TIMEOUT_COMMIT.into(),
                                    phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
                                    message: "autonomous commit timed out".to_string(),
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        });
                    }
                };

            let results = auto_outcomes
                .into_iter()
                .map(|r| match r {
                    Ok(cid) => ContainerInjectResult {
                        container_id: cid,
                        status: InjectStatus::INJECT_STARTED.into(),
                        phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
                        ..Default::default()
                    },
                    Err(r) => r,
                })
                .collect();

            return Ok(InjectTaskResponse {
                results,
                ..Default::default()
            });
        }

        // ── Phase 1: PREPARE ───────────────────────────────────────────────────
        // Connect to each target concurrently and run CAPABILITIES → PREPARE → READY.

        let prepare_futs: Vec<_> = req
            .tasks
            .iter()
            .map(|task| {
                let task = task.clone();
                let pids = pids.clone();
                async move { inject_prepare_one(&task, &pids).await }
            })
            .collect();

        let prepare_outcomes =
            match timeout(prepare_timeout, futures::future::join_all(prepare_futs)).await {
                Ok(outcomes) => outcomes,
                Err(_) => {
                    // Global prepare timeout: build TIMEOUT result for every target.
                    return Ok(InjectTaskResponse {
                        results: req
                            .tasks
                            .iter()
                            .map(|t| ContainerInjectResult {
                                container_id: t.container_id.clone(),
                                status: InjectStatus::INJECT_TIMEOUT_PREPARE.into(),
                                phase: InjectPhase::INJECT_PHASE_PREPARE.into(),
                                message: "prepare phase timed out".to_string(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    });
                }
            };

        // Partition into ready connections and prepare failures.
        let mut ready: Vec<(String, PreparedConn)> = Vec::new();
        let mut failed_results: Vec<ContainerInjectResult> = Vec::new();

        for outcome in prepare_outcomes {
            match outcome {
                Ok((container_id, conn)) => {
                    ready.push((container_id, conn));
                }
                Err(result) => {
                    failed_results.push(result);
                }
            }
        }

        if !failed_results.is_empty() {
            // Some targets failed PREPARE — cancel all that are READY, then return.
            let cancel_futs: Vec<_> = ready
                .into_iter()
                .map(|(cid, mut conn)| async move {
                    let _ = write_framed_message(
                        &mut conn.writer,
                        &SandboxerMessage::Cancel {
                            reason: "rollback".to_string(),
                        },
                    )
                    .await;
                    ContainerInjectResult {
                        container_id: cid,
                        status: InjectStatus::INJECT_INTERNAL_ERROR.into(),
                        phase: InjectPhase::INJECT_PHASE_PREPARE.into(),
                        message: "cancelled due to peer failure".to_string(),
                        ..Default::default()
                    }
                })
                .collect();
            let cancelled: Vec<_> = futures::future::join_all(cancel_futs).await;

            let mut results = failed_results;
            results.extend(cancelled);
            return Ok(InjectTaskResponse {
                results,
                ..Default::default()
            });
        }

        // ── Phase 2: COMMIT ────────────────────────────────────────────────────
        // All targets are READY. Send COMMIT concurrently, then wait for STARTED.

        let commit_futs: Vec<_> = ready
            .into_iter()
            .map(|(container_id, conn)| async move { inject_commit_one(container_id, conn).await })
            .collect();

        let commit_outcomes =
            match timeout(commit_timeout, futures::future::join_all(commit_futs)).await {
                Ok(outcomes) => outcomes,
                Err(_) => {
                    return Ok(InjectTaskResponse {
                        results: req
                            .tasks
                            .iter()
                            .map(|t| ContainerInjectResult {
                                container_id: t.container_id.clone(),
                                status: InjectStatus::INJECT_TIMEOUT_COMMIT.into(),
                                phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
                                message: "commit phase timed out".to_string(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    });
                }
            };

        let results = commit_outcomes
            .into_iter()
            .map(|r| match r {
                Ok(cid) => ContainerInjectResult {
                    container_id: cid,
                    status: InjectStatus::INJECT_STARTED.into(),
                    phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
                    ..Default::default()
                },
                Err(r) => r,
            })
            .collect();

        Ok(InjectTaskResponse {
            results,
            ..Default::default()
        })
    }

    async fn check_inject_socket(
        &self,
        _ctx: &TtrpcContext,
        req: CheckInjectSocketRequest,
    ) -> TtrpcResult<CheckInjectSocketResponse> {
        // Snapshot container PID map.
        let pids: std::collections::HashMap<String, i32> = {
            let containers = self.containers.lock().await;
            containers
                .iter()
                .map(|(id, c)| (id.clone(), c.init.pid))
                .collect()
        };

        let check_futs: Vec<_> = req
            .targets
            .iter()
            .map(|target| {
                let target = target.clone();
                let pids = pids.clone();
                async move { check_one_target(&target, &pids).await }
            })
            .collect();

        let results = futures::future::join_all(check_futs).await;

        Ok(CheckInjectSocketResponse {
            results,
            ..Default::default()
        })
    }

    async fn adopt_container(
        &self,
        _ctx: &TtrpcContext,
        req: AdoptContainerRequest,
    ) -> TtrpcResult<AdoptContainerResponse> {
        let _fork_guard = self.fork_gate.enter()?;
        // Read PID and remove the orphan atomically under a single lock to avoid TOCTOU.
        let orphan_pid = {
            let mut containers = self.containers.lock().await;
            let pid = containers
                .get(&req.old_id)
                .map(|c| c.init.pid)
                .ok_or_else(|| {
                    ttrpc::Error::Others(format!(
                        "adopt_container: orphan '{}' not found in task service",
                        req.old_id
                    ))
                })?;
            containers.remove(&req.old_id);
            pid
        };

        // Register the pending adoption so the container factory create path can pick it up.
        self.sandbox_resources.lock().await.register_adoption(
            &req.new_id,
            orphan_pid,
            req.old_id.clone(),
        );

        debug!(
            "adopt_container: orphan '{}' (pid={}) registered for adoption as '{}'",
            req.old_id, orphan_pid, req.new_id
        );
        Ok(AdoptContainerResponse::new())
    }
}

// ── WarmFork injection helpers ────────────────────────────────────────────────

const DEFAULT_READINESS_SOCKET: &str = "/run/warmfork-readiness.sock";

/// Active connection after PREPARE has been sent and READY received.
struct PreparedConn {
    writer: tokio::io::WriteHalf<tokio::net::UnixStream>,
    reader: tokio::io::ReadHalf<tokio::net::UnixStream>,
}

/// Connect to a container's readiness socket, optionally entering its mount namespace.
///
/// If `container_id` is non-empty, the init PID is looked up from `pids` and we enter
/// its mount namespace via `setns` before connecting (then restore the original ns).
/// Empty `container_id` connects in the current namespace (single-container mode).
async fn connect_in_ns(
    container_id: &str,
    socket_path: &str,
    pids: &std::collections::HashMap<String, i32>,
) -> std::io::Result<tokio::net::UnixStream> {
    if container_id.is_empty() {
        return tokio::net::UnixStream::connect(socket_path).await;
    }
    let pid = *pids.get(container_id).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("container '{}' not found in task service", container_id),
        )
    })?;
    let socket_path = socket_path.to_string();
    // Spawn a one-shot thread so that unshare(CLONE_FS) + setns(CLONE_NEWNS)
    // do not contaminate the Tokio spawn_blocking thread pool's shared fs struct.
    // (setns requires fs->users == 1; unshare gives this thread its own fs copy.)
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = tx.send(connect_in_container_ns(pid, &socket_path));
    });
    let std_stream = rx.await.map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "ns thread exited without sending result",
        )
    })??;
    std_stream.set_nonblocking(true)?;
    tokio::net::UnixStream::from_std(std_stream)
}

fn connect_in_container_ns(
    pid: i32,
    socket_path: &str,
) -> std::io::Result<std::os::unix::net::UnixStream> {
    let original_ns = std::fs::File::open("/proc/self/ns/mnt")
        .map_err(|e| std::io::Error::new(e.kind(), format!("open self mnt ns: {}", e)))?;
    let target_ns = std::fs::File::open(format!("/proc/{}/ns/mnt", pid))
        .map_err(|e| std::io::Error::new(e.kind(), format!("open pid {} mnt ns: {}", pid, e)))?;
    nix::sched::unshare(nix::sched::CloneFlags::CLONE_FS).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "unshare CLONE_FS before setns mnt ns for pid {}: {}",
                pid, e
            ),
        )
    })?;
    nix::sched::setns(&target_ns, nix::sched::CloneFlags::CLONE_NEWNS).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("setns mnt ns for pid {}: {}", pid, e),
        )
    })?;
    let result = std::os::unix::net::UnixStream::connect(socket_path);
    // Restore original ns even on connect failure; this thread exits immediately after.
    let _ = nix::sched::setns(&original_ns, nix::sched::CloneFlags::CLONE_NEWNS);
    result
}

/// Read the CAPABILITIES message and verify the protocol version.
async fn read_and_verify_capabilities<R>(reader: &mut R) -> std::io::Result<()>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    let msg: WorkloadMessage = read_framed_message(reader, MAX_FRAME_LEN).await?;
    let caps = match msg {
        WorkloadMessage::Capabilities(c) => c,
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected CAPABILITIES, got {:?}", other),
            ))
        }
    };
    if caps.protocol_version != PROTOCOL_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "protocol version mismatch: expected {} got {}",
                PROTOCOL_VERSION, caps.protocol_version
            ),
        ));
    }
    Ok(())
}

/// Phase-1 for one target: connect → CAPABILITIES → PREPARE → READY.
/// Returns `Ok((container_id, PreparedConn))` or `Err(ContainerInjectResult)`.
async fn inject_prepare_one(
    task: &ContainerTask,
    pids: &std::collections::HashMap<String, i32>,
) -> std::result::Result<(String, PreparedConn), ContainerInjectResult> {
    let socket = if task.socket_path.is_empty() {
        DEFAULT_READINESS_SOCKET
    } else {
        task.socket_path.as_str()
    };

    // Connect (entering mount namespace if container_id is set).
    let stream = match connect_in_ns(&task.container_id, socket, pids).await {
        Ok(s) => s,
        Err(e) => {
            let status = if e.to_string().contains("not found") {
                InjectStatus::INJECT_TARGET_NOT_FOUND
            } else if e.to_string().contains("mnt ns") {
                InjectStatus::INJECT_NAMESPACE_ERROR
            } else {
                InjectStatus::INJECT_CONNECT_FAILED
            };
            return Err(ContainerInjectResult {
                container_id: task.container_id.clone(),
                status: status.into(),
                phase: InjectPhase::INJECT_PHASE_PREPARE.into(),
                message: e.to_string(),
                ..Default::default()
            });
        }
    };
    let (mut reader, mut writer) = tokio::io::split(stream);

    // CAPABILITIES
    if let Err(e) = read_and_verify_capabilities(&mut reader).await {
        return Err(ContainerInjectResult {
            container_id: task.container_id.clone(),
            status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
            phase: InjectPhase::INJECT_PHASE_PREPARE.into(),
            message: format!("capabilities: {}", e),
            ..Default::default()
        });
    }

    // PREPARE
    let payload = PreparePayload {
        task_id: task.task_id.clone(),
        env_overrides: task.env_overrides.clone(),
        context: task.context.clone(),
    };
    if let Err(e) = write_framed_message(&mut writer, &SandboxerMessage::Prepare(payload)).await {
        return Err(ContainerInjectResult {
            container_id: task.container_id.clone(),
            status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
            phase: InjectPhase::INJECT_PHASE_PREPARE.into(),
            message: format!("write PREPARE: {}", e),
            ..Default::default()
        });
    }

    // Wait for READY or REJECT
    let response: WorkloadMessage = match read_framed_message(&mut reader, MAX_FRAME_LEN).await {
        Ok(m) => m,
        Err(e) => {
            return Err(ContainerInjectResult {
                container_id: task.container_id.clone(),
                status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
                phase: InjectPhase::INJECT_PHASE_PREPARE.into(),
                message: format!("read READY/REJECT: {}", e),
                ..Default::default()
            })
        }
    };
    match response {
        WorkloadMessage::Ready => Ok((task.container_id.clone(), PreparedConn { writer, reader })),
        WorkloadMessage::Reject { reason, message } => Err(ContainerInjectResult {
            container_id: task.container_id.clone(),
            status: InjectStatus::INJECT_REJECT.into(),
            phase: InjectPhase::INJECT_PHASE_PREPARE.into(),
            message: format!("{}: {}", reason.as_str(), message),
            ..Default::default()
        }),
        other => Err(ContainerInjectResult {
            container_id: task.container_id.clone(),
            status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
            phase: InjectPhase::INJECT_PHASE_PREPARE.into(),
            message: format!("expected READY or REJECT, got {:?}", other),
            ..Default::default()
        }),
    }
}

/// Phase-2 for one target: COMMIT → STARTED.
/// Returns `Ok(container_id)` on success or `Err(ContainerInjectResult)` on failure.
async fn inject_commit_one(
    container_id: String,
    mut conn: PreparedConn,
) -> std::result::Result<String, ContainerInjectResult> {
    // Send COMMIT
    if let Err(e) = write_framed_message(&mut conn.writer, &SandboxerMessage::Commit).await {
        return Err(ContainerInjectResult {
            container_id: container_id.clone(),
            status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
            phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
            message: format!("write COMMIT: {}", e),
            ..Default::default()
        });
    }

    // Wait for STARTED
    let response: WorkloadMessage = match read_framed_message(&mut conn.reader, MAX_FRAME_LEN).await
    {
        Ok(m) => m,
        Err(e) => {
            return Err(ContainerInjectResult {
                container_id: container_id.clone(),
                status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
                phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
                message: format!("read STARTED: {}", e),
                ..Default::default()
            })
        }
    };
    match response {
        WorkloadMessage::Started => Ok(container_id),
        other => Err(ContainerInjectResult {
            container_id: container_id.clone(),
            status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
            phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
            message: format!("expected STARTED, got {:?}", other),
            ..Default::default()
        }),
    }
}

/// Autonomous mode for one target: connect → CAPABILITIES → COMMIT → STARTED.
/// The workload self-starts without receiving a task identity (task_id is empty).
/// Returns `Ok(container_id)` on success or `Err(ContainerInjectResult)` on failure.
async fn inject_autonomous_one(
    task: &ContainerTask,
    pids: &std::collections::HashMap<String, i32>,
) -> std::result::Result<String, ContainerInjectResult> {
    let socket = if task.socket_path.is_empty() {
        DEFAULT_READINESS_SOCKET
    } else {
        task.socket_path.as_str()
    };

    let stream = match connect_in_ns(&task.container_id, socket, pids).await {
        Ok(s) => s,
        Err(e) => {
            let status = if e.to_string().contains("not found") {
                InjectStatus::INJECT_TARGET_NOT_FOUND
            } else if e.to_string().contains("mnt ns") {
                InjectStatus::INJECT_NAMESPACE_ERROR
            } else {
                InjectStatus::INJECT_CONNECT_FAILED
            };
            return Err(ContainerInjectResult {
                container_id: task.container_id.clone(),
                status: status.into(),
                phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
                message: e.to_string(),
                ..Default::default()
            });
        }
    };
    let (mut reader, mut writer) = tokio::io::split(stream);

    // CAPABILITIES
    if let Err(e) = read_and_verify_capabilities(&mut reader).await {
        return Err(ContainerInjectResult {
            container_id: task.container_id.clone(),
            status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
            phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
            message: format!("capabilities: {}", e),
            ..Default::default()
        });
    }

    // COMMIT (no PREPARE/READY)
    if let Err(e) = write_framed_message(&mut writer, &SandboxerMessage::Commit).await {
        return Err(ContainerInjectResult {
            container_id: task.container_id.clone(),
            status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
            phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
            message: format!("write COMMIT: {}", e),
            ..Default::default()
        });
    }

    // STARTED
    match read_framed_message(&mut reader, MAX_FRAME_LEN).await {
        Ok(WorkloadMessage::Started) => Ok(task.container_id.clone()),
        Ok(other) => Err(ContainerInjectResult {
            container_id: task.container_id.clone(),
            status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
            phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
            message: format!("expected STARTED, got {:?}", other),
            ..Default::default()
        }),
        Err(e) => Err(ContainerInjectResult {
            container_id: task.container_id.clone(),
            status: InjectStatus::INJECT_PROTOCOL_ERROR.into(),
            phase: InjectPhase::INJECT_PHASE_COMMIT.into(),
            message: format!("read STARTED: {}", e),
            ..Default::default()
        }),
    }
}

/// Probe one target: connect → CAPABILITIES → close.
async fn check_one_target(
    target: &vmm_common::api::sandbox::InjectTarget,
    pids: &std::collections::HashMap<String, i32>,
) -> TargetCheckResult {
    let socket = if target.socket_path.is_empty() {
        DEFAULT_READINESS_SOCKET
    } else {
        target.socket_path.as_str()
    };

    let stream = match connect_in_ns(&target.container_id, socket, pids).await {
        Ok(s) => s,
        Err(e) => {
            let status = if e.to_string().contains("not found") {
                TargetCheckStatus::TARGET_NOT_FOUND
            } else if e.to_string().contains("mnt ns") {
                TargetCheckStatus::NAMESPACE_ERROR
            } else {
                TargetCheckStatus::CONNECT_FAILED
            };
            return TargetCheckResult {
                container_id: target.container_id.clone(),
                status: status.into(),
                message: e.to_string(),
                ..Default::default()
            };
        }
    };
    let mut reader = tokio::io::BufReader::new(stream);

    // Read CAPABILITIES then drop → workload loops back to accept()
    let result = read_and_verify_capabilities(&mut reader).await;
    drop(reader);

    match result {
        Ok(()) => TargetCheckResult {
            container_id: target.container_id.clone(),
            status: TargetCheckStatus::TARGET_CHECK_OK.into(),
            ..Default::default()
        },
        Err(e) => TargetCheckResult {
            container_id: target.container_id.clone(),
            status: TargetCheckStatus::PROTOCOL_ERROR.into(),
            message: e.to_string(),
            ..Default::default()
        },
    }
}

fn write_entropy_sync(seed: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    std::fs::OpenOptions::new().write(true).open("/dev/urandom")?.write_all(seed)
}

async fn write_entropy(seed: &[u8]) -> TtrpcResult<()> {
    let seed = seed.to_vec();
    // Await the actual kernel write, not an async File's buffered write. One
    // blocking job replaces separate open/write scheduling hops.
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        write_entropy_sync(&seed)
    }).await.map_err(|e| ttrpc::Error::Others(format!("reseed_entropy join: {e}")))?
      .map_err(|e| ttrpc::Error::Others(format!("reseed_entropy write: {e}")))?;
    Ok(())
}

fn process_command(command: &str, argv: &[String]) -> Result<std::process::Command> {
    if argv.is_empty() {
        let mut cmd = std::process::Command::new("/bin/bash");
        cmd.args(["-c", command]);
        return Ok(cmd);
    }
    if !command.is_empty() || argv[0].is_empty() {
        return Err(Error::Other(
            "Specify either command or a nonempty argv executable".into(),
        ));
    }
    let mut cmd = std::process::Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    Ok(cmd)
}

async fn execute_result(
    req: &ExecVMProcessRequest,
) -> Result<(String, String, i32, crate::util::ProcessTiming)> {
    let mut cmd = process_command(&req.command, &req.argv)?;
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    if !req.stdin.is_empty() {
        cmd.stdin(Stdio::piped());
    } else if req.structured_output {
        cmd.stdin(Stdio::null());
    }
    let mut timing = crate::util::ProcessTiming::default();
    let (stdout, stderr, code) = crate::util::spawn_and_wait_profile(
        cmd,
        &req.stdin,
        (req.profile || req.structured_output).then_some(&mut timing),
    )
    .await?;
    if code != 0 && !req.structured_output {
        return Err(Error::Other(format!(
            "Command failed with exit code {code}: {stderr}"
        )));
    }
    Ok((stdout, stderr, code, timing))
}

#[cfg(test)]
async fn execute_request(
    req: &ExecVMProcessRequest,
) -> Result<(String, crate::util::ProcessTiming)> {
    execute_result(req)
        .await
        .map(|(out, _, _, timing)| (out, timing))
}

#[cfg(test)]
async fn do_execute_cmd(cmd_args: &str, stdin: &[u8]) -> Result<String> {
    let req = ExecVMProcessRequest {
        command: cmd_args.into(),
        stdin: stdin.to_vec(),
        ..Default::default()
    };
    execute_request(&req).await.map(|(out, _)| out)
}

#[cfg(test)]
mod tests {
    use super::do_execute_cmd;

    #[tokio::test]
    async fn structured_exit_retains_pid_and_separate_stderr() {
        let mut req = super::ExecVMProcessRequest {
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                "printf out; printf err >&2; exit 7".into(),
            ],
            structured_output: true,
            ..Default::default()
        };
        let (out, err, code, timing) = super::execute_result(&req).await.unwrap();
        assert_eq!((out.as_str(), err.as_str(), code), ("out", "err", 7));
        assert!(timing.pid > 0);
        req.structured_output = false;
        assert!(super::execute_result(&req).await.is_err());
    }

    #[tokio::test]
    async fn direct_argv_is_literal_and_profile_covers_output() {
        let req = super::ExecVMProcessRequest {
            argv: vec!["/bin/echo".into(), "$(not-a-shell); $HOME".into()],
            profile: true,
            ..Default::default()
        };
        let (out, timing) = super::execute_request(&req).await.unwrap();
        assert_eq!(out, "$(not-a-shell); $HOME\n");
        assert!(timing.first_stdout_us > 0);
        assert!(timing.total_us >= timing.first_stdout_us);
        assert!(super::process_command("echo bad", &req.argv).is_err());
        assert!(super::process_command("", &[String::new()]).is_err());
    }

    #[tokio::test]
    async fn test_do_execute_cmd_table() {
        struct TestCase {
            name: &'static str,
            cmd: &'static str,
            stdin: &'static [u8],
            expected_out: &'static str,
            expect_err: bool,
        }

        let cases = [
            TestCase {
                name: "simple echo",
                cmd: "echo hello",
                stdin: &[],
                expected_out: "hello",
                expect_err: false,
            },
            TestCase {
                name: "cat with stdin",
                cmd: "cat",
                stdin: b"hello stdin",
                expected_out: "hello stdin",
                expect_err: false,
            },
            TestCase {
                name: "command fail",
                cmd: "exit 1",
                stdin: &[],
                expected_out: "",
                expect_err: true,
            },
            TestCase {
                name: "multi line output",
                cmd: "echo -e 'line1\nline2'",
                stdin: &[],
                expected_out: "line1\nline2",
                expect_err: false,
            },
            TestCase {
                name: "invalid utf8 output",
                cmd: "printf '\\377'",
                stdin: &[],
                expected_out: "\u{FFFD}",
                expect_err: false,
            },
            TestCase {
                name: "pipe output",
                cmd: "echo -e 'line1\nline2\nline3' | grep line2",
                stdin: &[],
                expected_out: "line2",
                expect_err: false,
            },
        ];

        for case in cases {
            let res = do_execute_cmd(case.cmd, case.stdin).await;
            if case.expect_err {
                assert!(res.is_err(), "case: {}", case.name);
            } else {
                assert!(res.is_ok(), "case: {}, err: {:?}", case.name, res.err());
                assert_eq!(
                    res.unwrap().trim(),
                    case.expected_out,
                    "case: {}",
                    case.name
                );
            }
        }
    }

    #[tokio::test]
    async fn test_do_execute_cmd_concurrency() {
        let mut futures = vec![];
        for i in 0..10 {
            futures.push(tokio::spawn(async move {
                let cmd = format!("echo hello {}", i);
                do_execute_cmd(&cmd, &[]).await
            }));
        }
        let results = futures::future::join_all(futures).await;
        for (i, res) in results.into_iter().enumerate() {
            let out = res.unwrap().expect("should success");
            assert_eq!(out.trim(), format!("hello {}", i));
        }
    }
}
