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

#[cfg(test)]
use std::process::Child;
use std::{io, path::Path};

use containerd_shim::{
    asynchronous::{
        monitor::{monitor_subscribe, monitor_unsubscribe, Subscription},
        util::read_file_to_str,
    },
    error::{Error, Result},
    monitor::{ExitEvent, Subject, Topic},
    other, other_error,
};
use log::error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{ChildStderr, ChildStdin, ChildStdout},
};
use vmm_common::{storage::Storage, Io, IO_FILE_PREFIX, STORAGE_FILE_PREFIX};

/// Verify that an orphaned container process is still alive before completing resume adoption.
/// Returns an error if `/proc/<pid>` does not exist.
pub fn verify_orphan_alive(container_id: &str, pid: i32, runtime_id: &str) -> Result<()> {
    if !std::path::Path::new("/proc").join(pid.to_string()).exists() {
        return Err(other!(
            "create {}: Resume adoption failed — orphan pid {} (runtime_id '{}') \
             is no longer alive; sandbox must be restarted in Clone mode or \
             recreated from a fresh snapshot",
            container_id,
            pid,
            runtime_id
        ));
    }
    Ok(())
}

pub struct PidMonitorGuard {
    subscription: Option<Subscription>,
}

impl PidMonitorGuard {
    pub async fn subscribe() -> Result<Self> {
        Ok(Self {
            subscription: Some(monitor_subscribe(Topic::Pid).await?),
        })
    }

    #[allow(clippy::expect_used)]
    pub async fn wait(&mut self, pid: i32) -> i32 {
        let subscription = self
            .subscription
            .take()
            .expect("pid monitor subscription should exist");
        wait_pid(pid, subscription).await
    }

    pub async fn unsubscribe(&mut self) {
        if let Some(subscription) = self.subscription.take() {
            let _ = monitor_unsubscribe(subscription.id).await;
        }
    }
}

pub async fn wait_pid(pid: i32, s: Subscription) -> i32 {
    let mut s = s;
    loop {
        if let Some(ExitEvent {
            subject: Subject::Pid(epid),
            exit_code: code,
        }) = s.rx.recv().await
        {
            if pid == epid {
                monitor_unsubscribe(s.id).await.unwrap_or_default();
                return code;
            }
        }
    }
}

#[cfg(test)]
fn spawn_test_reaper(child: &mut Option<Child>, pid: u32) {
    if let Some(mut child) = child.take() {
        tokio::spawn(async move {
            let wait_result = tokio::task::spawn_blocking(move || {
                use std::os::unix::process::ExitStatusExt;

                child.wait().map(|status| {
                    status
                        .code()
                        .unwrap_or_else(|| 128 + status.signal().unwrap_or(0))
                })
            })
            .await;
            match wait_result {
                Ok(Ok(code)) => {
                    let _ = containerd_shim::asynchronous::monitor::monitor_notify_by_pid(
                        pid as i32, code,
                    )
                    .await;
                }
                Ok(Err(e)) => {
                    error!("test reaper failed to wait child {}: {}", pid, e);
                }
                Err(e) => {
                    error!("test reaper task failed for child {}: {}", pid, e);
                }
            }
        });
    }
}

#[cfg_attr(feature = "youki", allow(dead_code))]
pub(crate) async fn read_std<T>(std: Option<T>) -> String
where
    T: AsyncRead + Unpin,
{
    let mut std = std;
    if let Some(mut std) = std.take() {
        let mut out = String::new();
        if let Err(e) = std.read_to_string(&mut out).await {
            error!("failed to read process stdio: {}", e);
        }
        return out;
    }
    "".to_string()
}

async fn read_std_bytes<T>(std: Option<T>) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin,
{
    let mut std = std;
    if let Some(mut std) = std.take() {
        let mut out = Vec::new();
        std.read_to_end(&mut out).await?;
        return Ok(out);
    }
    Ok(Vec::new())
}

#[derive(Default, Debug, serde::Serialize)]
pub struct ProcessTiming {
    pub pid: u32,
    pub subscribe_us: u128,
    pub spawn_us: u128,
    pub first_stdout_us: u128,
    pub stdout_eof_us: u128,
    pub exit_us: u128,
    pub total_us: u128,
}

pub async fn spawn_and_wait(
    cmd: std::process::Command,
    stdin: &[u8],
) -> Result<(String, String, i32)> {
    spawn_and_wait_profile(cmd, stdin, None).await
}

#[derive(Debug)]
pub enum ProcessEvent {
    Start(u32),
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
}

pub async fn spawn_and_wait_profile(
    cmd: std::process::Command,
    stdin: &[u8],
    timing: Option<&mut ProcessTiming>,
) -> Result<(String, String, i32)> {
    spawn_and_wait_stream(cmd, stdin, timing, None).await
}

pub async fn spawn_and_wait_stream(
    mut cmd: std::process::Command,
    stdin: &[u8],
    mut timing: Option<&mut ProcessTiming>,
    events: Option<&tokio::sync::mpsc::Sender<ProcessEvent>>,
) -> Result<(String, String, i32)> {
    let started = std::time::Instant::now();
    let mut monitor = PidMonitorGuard::subscribe().await?;
    if let Some(t) = timing.as_deref_mut() {
        t.subscribe_us = started.elapsed().as_micros();
    }
    let spawn_started = std::time::Instant::now();
    let child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            monitor.unsubscribe().await;
            return Err(Error::IoError {
                context: "spawn failed".to_string(),
                err: e,
            });
        }
    };

    if let Some(t) = timing.as_deref_mut() {
        t.spawn_us = spawn_started.elapsed().as_micros();
    }
    let pid = child.id();
    if let Some(t) = timing.as_deref_mut() {
        t.pid = pid;
    }
    let mut child = Some(child);
    if let Some(events) = events {
        let _ = events.send(ProcessEvent::Start(pid)).await;
    }

    let mut process_exited = false;

    let result = async {
        if !stdin.is_empty() {
            let child_stdin = child.as_mut().unwrap().stdin.take().ok_or_else(|| {
                other!("stdin pipe is missing, please ensure stdin(Stdio::piped()) is set")
            })?;

            let mut async_stdin =
                ChildStdin::from_std(child_stdin).map_err(|e| Error::IoError {
                    context: "failed to convert stdin".to_string(),
                    err: e,
                })?;

            async_stdin
                .write_all(stdin)
                .await
                .map_err(|e| Error::IoError {
                    context: "failed to write stdin".to_string(),
                    err: e,
                })?;

            async_stdin.shutdown().await.map_err(|e| Error::IoError {
                context: "failed to shutdown stdin".to_string(),
                err: e,
            })?;
        }

        let stdout_stream = child
            .as_mut()
            .unwrap()
            .stdout
            .take()
            .map(ChildStdout::from_std)
            .transpose()
            .map_err(|e| Error::IoError {
                context: "failed to convert stdout".to_string(),
                err: e,
            })?;

        let stderr_stream = child
            .as_mut()
            .unwrap()
            .stderr
            .take()
            .map(ChildStderr::from_std)
            .transpose()
            .map_err(|e| Error::IoError {
                context: "failed to convert stderr".to_string(),
                err: e,
            })?;

        #[cfg(test)]
        spawn_test_reaper(&mut child, pid);

        let profile = timing.is_some() || events.is_some();
        let stdout = async {
            if !profile {
                return (read_std_bytes(stdout_stream).await, 0, 0);
            }
            let mut out = Vec::new();
            let mut first = 0;
            if let Some(mut stream) = stdout_stream {
                let mut buf = [0u8; 4096];
                loop {
                    let n = match stream.read(&mut buf).await {
                        Ok(n) => n,
                        Err(e) => return (Err(e), first, started.elapsed().as_micros()),
                    };
                    if n == 0 {
                        break;
                    }
                    if first == 0 {
                        first = started.elapsed().as_micros();
                    }
                    if let Some(events) = events {
                        // A disconnected receiver does not orphan the process:
                        // continue draining and reaping, without retaining output.
                        let _ = events.send(ProcessEvent::Stdout(buf[..n].to_vec())).await;
                    } else {
                        out.extend_from_slice(&buf[..n]);
                    }
                }
            }
            (Ok(out), first, started.elapsed().as_micros())
        };
        let exit = async {
            let code = monitor.wait(pid as i32).await;
            (code, started.elapsed().as_micros())
        };
        let stderr = async {
            if let Some(events) = events {
                if let Some(mut stream) = stderr_stream {
                    let mut buf = [0u8; 4096];
                    loop {
                        let n = stream.read(&mut buf).await?;
                        if n == 0 { break; }
                        let _ = events.send(ProcessEvent::Stderr(buf[..n].to_vec())).await;
                    }
                }
                Ok(Vec::new())
            } else {
                read_std_bytes(stderr_stream).await
            }
        };
        let ((stdout_bytes, first, eof), stderr_bytes, (exit_code, exit_time)) =
            tokio::join!(stdout, stderr, exit);
        if let Some(t) = timing.as_deref_mut() {
            t.first_stdout_us = first;
            t.stdout_eof_us = eof;
            t.exit_us = exit_time;
            t.total_us = started.elapsed().as_micros();
        }

        process_exited = true;

        let stdout = String::from_utf8_lossy(&stdout_bytes.map_err(|e| Error::IoError {
            context: "failed to read stdout".to_string(),
            err: e,
        })?)
        .into_owned();

        let stderr = String::from_utf8_lossy(&stderr_bytes.map_err(|e| Error::IoError {
            context: "failed to read stderr".to_string(),
            err: e,
        })?)
        .into_owned();

        Ok((stdout, stderr, exit_code))
    }
    .await;

    match result {
        Ok(res) => Ok(res),
        Err(e) => {
            if !process_exited {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid as i32),
                    nix::sys::signal::Signal::SIGKILL,
                );
            }
            monitor.unsubscribe().await;
            Err(e)
        }
    }
}

pub async fn read_storages(bundle: impl AsRef<Path>, id: &str) -> Result<Vec<Storage>> {
    let storage_file_name = format!("{}-{}", STORAGE_FILE_PREFIX, id);
    let path = bundle.as_ref().join(&storage_file_name);
    let content = read_file_to_str(&path).await?;
    serde_json::from_str::<Vec<Storage>>(content.as_str()).map_err(other_error!(e, "read storage"))
}

pub async fn read_io(
    bundle: impl AsRef<Path>,
    container_id: &str,
    exec_id: Option<&str>,
) -> Result<Io> {
    let io_file_name = if let Some(eid) = exec_id {
        format!("{}-{}-{}", IO_FILE_PREFIX, container_id, eid)
    } else {
        format!("{}-{}", IO_FILE_PREFIX, container_id)
    };
    let path = bundle.as_ref().join(&io_file_name);
    let content = read_file_to_str(&path).await?;
    let io = serde_json::from_str::<Io>(content.as_str()).map_err(other_error!(e, "read io"))?;
    if exec_id.is_some() {
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(Error::IoError {
                    context: "remove exec io file".to_string(),
                    err: e,
                })
            }
        }
    }
    Ok(io)
}

#[cfg(test)]
mod tests {
    use containerd_sandbox::data::Io;
    use containerd_shim::util::write_str_to_file;
    use temp_dir::TempDir;

    use super::read_io;

    #[tokio::test]
    async fn test_read_io_cleanup_behavior() {
        struct TestCase {
            name: &'static str,
            exec_id: Option<&'static str>,
            terminal: bool,
            should_remove: bool,
        }

        let cases = [
            TestCase {
                name: "exec io is removed after read",
                exec_id: Some("test-exec"),
                terminal: false,
                should_remove: true,
            },
            TestCase {
                name: "init io is kept after read",
                exec_id: None,
                terminal: true,
                should_remove: false,
            },
        ];

        for case in cases {
            let tmp_dir = TempDir::new().unwrap();
            let bundle = tmp_dir.path();
            let io_file_name = match case.exec_id {
                Some(exec_id) => format!("io-test-container-{}", exec_id),
                None => "io-test-container".to_string(),
            };
            let io_path = bundle.join(io_file_name);
            let io = Io {
                stdin: "stdin".to_string(),
                stdout: "stdout".to_string(),
                stderr: "stderr".to_string(),
                terminal: case.terminal,
            };
            let io_str = serde_json::to_string(&io).unwrap();
            write_str_to_file(&io_path, &io_str).await.unwrap();

            let read_back = read_io(bundle, "test-container", case.exec_id)
                .await
                .unwrap();

            assert_eq!(read_back.stdin, "stdin", "case: {}", case.name);
            assert_eq!(io_path.exists(), !case.should_remove, "case: {}", case.name);
        }
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use std::process::{Command, Stdio};
    #[tokio::test]
    async fn streaming_stdout_precedes_exit_and_retains_stderr_status() {
        let directory = temp_dir::TempDir::new().unwrap();
        let release = directory.path().join("release");
        let mut cmd = Command::new("/bin/sh");
        cmd.args(["-c", "printf READY; n=0; while [ ! -f \"$1\" ] && [ $n -lt 200 ]; do sleep .01; n=$((n+1)); done; printf error >&2; exit 7", "test"])
            .arg(&release).stdout(Stdio::piped()).stderr(Stdio::piped());
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let running = tokio::spawn(async move {
            spawn_and_wait_stream(cmd, &[], None, Some(&tx)).await.unwrap()
        });
        assert!(matches!(rx.recv().await.unwrap(), ProcessEvent::Start(_)));
        match tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await.unwrap().unwrap() {
            ProcessEvent::Stdout(data) => assert_eq!(data, b"READY"),
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(!running.is_finished());
        std::fs::write(&release, b"ok").unwrap();
        let mut errors = Vec::new();
        while let Some(event) = rx.recv().await {
            if let ProcessEvent::Stderr(data) = event { errors.extend(data); }
        }
        let (out, err, code) = running.await.unwrap();
        assert!(out.is_empty() && err.is_empty());
        assert_eq!(errors, b"error");
        assert_eq!(code, 7);
    }
    #[tokio::test]
    async fn streaming_disconnected_receiver_still_drains_and_reaps() {
        let mut cmd = Command::new("/bin/sh");
        cmd.args(["-c", "head -c 1048576 /dev/zero; printf err >&2; exit 3"])
            .stdout(Stdio::piped()).stderr(Stdio::piped());
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let result = tokio::time::timeout(std::time::Duration::from_secs(3),
            spawn_and_wait_stream(cmd, &[], None, Some(&tx))).await.unwrap().unwrap();
        assert_eq!(result, (String::new(), String::new(), 3));
    }
}
