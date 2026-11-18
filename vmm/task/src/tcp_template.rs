//! Conservative empty-TCP certificate for fixed templates. Only the vsock-only
//! PID-1 agent runs while the certificate is held; all other userspace is frozen.
//! Listeners also disqualify the fast path, avoiding passive-open kernel races.
use std::{io, path::Path, sync::Arc, time::{Duration, Instant}};
use tokio::sync::{Mutex, OwnedRwLockWriteGuard, RwLock};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, FileTypeExt, MetadataExt, OpenOptionsExt};
use std::io::Write;
const GROUP: &str = "/sys/fs/cgroup/freezer/kuasar-template-tcp";

pub struct TemplateTcp {
    pub exec_gate: Arc<RwLock<()>>,
    lease: Mutex<Option<Certificate>>,
}
struct Certificate {
    owner: String,
    state: Arc<FrozenState>,
    _exec_guard: OwnedRwLockWriteGuard<()>,
}

/// Files opened and validated while certifying the frozen template.
pub struct FrozenState {
    freezer: File,
    entropy: File,
}

fn open_entropy(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new().write(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY).open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_char_device() || metadata.rdev() != libc::makedev(1, 9) {
        return Err(io::Error::other("template entropy source is not the kernel urandom device"));
    }
    Ok(file)
}

/// Only a certified, frozen template uses this bounded kernel-device write.
/// No pathname lookup or blocking-pool handoff is needed during acquisition.
pub fn reseed_frozen(state: &FrozenState, seed: &[u8]) -> io::Result<()> {
    if seed.len() != 32 { return Err(io::Error::other("invalid fork entropy length")); }
    (&state.entropy).write_all(seed)
}

/// The held file is bound to the same freezer group that was certified. Positional
/// reads avoid shared file offsets, and every claim verifies the current state.
pub fn check_frozen(state: &FrozenState) -> io::Result<()> {
    let mut buffer = [0u8; 16];
    let n = state.freezer.read_at(&mut buffer, 0)?;
    if &buffer[..n] != b"FROZEN\n" {
        return Err(io::Error::other("TCP certificate lost its frozen workload"));
    }
    Ok(())
}

impl Default for TemplateTcp {
    fn default() -> Self { Self { exec_gate: Arc::default(), lease: Mutex::new(None) } }
}
fn failed(s: impl ToString) -> ttrpc::Error { ttrpc::Error::Others(s.to_string()) }
fn group(pid: u32) -> io::Result<String> {
    let data=std::fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
    data.lines().find_map(|line| {
        let mut p=line.splitn(3, ':'); p.next()?;
        if p.next()?.split(',').any(|s| s=="freezer") {p.next().map(str::to_owned)} else {None}
    }).ok_or_else(|| io::Error::other("freezer controller absent"))
}
fn users() -> io::Result<Vec<u32>> {
    let mut result=Vec::new();
    for e in std::fs::read_dir("/proc")? {
        let e=e?;let Ok(pid)=e.file_name().to_string_lossy().parse::<u32>() else {continue};
        if pid!=std::process::id() && e.path().join("exe").read_link().is_ok() {result.push(pid);}
    }
    Ok(result)
}
pub fn ensure_frozen() -> io::Result<()> {
    if std::fs::read_to_string(format!("{GROUP}/freezer.state"))?.trim()!="FROZEN" {
        return Err(io::Error::other("TCP certificate lost its frozen workload"));
    }
    Ok(())
}
fn thaw() -> io::Result<()> {
    let path=format!("{GROUP}/freezer.state");
    if Path::new(&path).exists() {std::fs::write(path,"THAWED")?;}
    Ok(())
}
fn freeze_and_check() -> io::Result<bool> {
    // Limit this optimization to the known init/agent topology and the default
    // freezer group; do not move tasks out of a container's resource hierarchy.
    if std::process::id()!=1 || group(1)?!="/" {return Ok(false);}
    std::fs::create_dir_all(GROUP)?;
    std::fs::write(format!("{GROUP}/freezer.state"),"FROZEN")?;
    let started=Instant::now();
    loop {
        let mut moved=false;
        for pid in users()? {
            match std::fs::read_link(format!("/proc/{pid}/ns/net")) {
                Ok(ns) if ns == std::fs::read_link("/proc/self/ns/net")? => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                _ => return Ok(false),
            }
            let g=match group(pid) {Ok(g)=>g,Err(e) if e.kind()==io::ErrorKind::NotFound=>continue,Err(e)=>return Err(e)};
            if g!="/" && g!="/kuasar-template-tcp" {return Ok(false);}
            if g=="/" {
                match std::fs::write(format!("{GROUP}/cgroup.procs"),pid.to_string()) {
                    Ok(())=>moved=true,
                    Err(e) if matches!(e.raw_os_error(),Some(libc::ESRCH))=>(),
                    Err(e)=>return Err(e),
                }
            }
        }
        if !moved && ensure_frozen().is_ok() {
            // The exec write lease prevents the agent spawning new application
            // work. Frozen descendants cannot fork/connect or create listeners.
            return crate::tcp_reset::empty_all_states();
        }
        if started.elapsed()>Duration::from_secs(1) {return Ok(false);}
        std::thread::sleep(Duration::from_millis(1));
    }
}
impl TemplateTcp {
    pub async fn prepare(&self, token: &str) -> ttrpc::Result<bool> {
        if vmm_common::trace::is_enabled() { return Ok(false); }
        let mut lease = self.lease.lock().await;
        if let Some(certificate) = lease.as_ref() {
            return Ok(certificate.owner == token);
        }
        let Ok(guard) = self.exec_gate.clone().try_write_owned() else { return Ok(false) };
        // No cancellation point between freezing and committing the lease. If
        // opening its file fails, roll back the freeze before returning an error.
        let state = tokio::task::block_in_place(|| -> io::Result<Option<FrozenState>> {
            let result = freeze_and_check().and_then(|ready| {
                if ready {
                    let freezer = OpenOptions::new().read(true).write(true)
                        .open(format!("{GROUP}/freezer.state"))?;
                    let entropy = open_entropy(Path::new("/dev/urandom"))?;
                    Ok(Some(FrozenState { freezer, entropy }))
                } else { Ok(None) }
            });
            if !matches!(&result, Ok(Some(_))) { thaw()?; }
            result
        }).map_err(failed)?;
        let ready = state.is_some();
        if let Some(state) = state {
            *lease = Some(Certificate {
                owner: token.to_owned(), state: Arc::new(state), _exec_guard: guard,
            });
        }
        Ok(ready)
    }
    pub async fn certificate(&self, token: &str) -> Option<Arc<FrozenState>> {
        self.lease.lock().await.as_ref()
            .filter(|certificate| certificate.owner == token)
            .map(|certificate| certificate.state.clone())
    }
    pub async fn release(&self, token: &str) -> ttrpc::Result<()> {
        let mut lease = self.lease.lock().await;
        if let Some(certificate) = lease.as_ref() {
            if certificate.owner != token {
                return Err(failed("another template owns frozen workload"));
            }
            certificate.state.freezer.write_all_at(b"THAWED", 0).map_err(failed)?;
            // Close the certificate FD only after a successful thaw.
            lease.take();
        }
        Ok(())
    }
}

#[cfg(test)]
mod entropy_tests {
    use super::*;
    #[test]
    fn entropy_device_validation_and_ownership() {
        assert!(open_entropy(Path::new("/dev/null")).is_err());
        let directory=temp_dir::TempDir::new().unwrap();
        let path=directory.path().join("entropy");
        std::fs::write(&path,b"regular").unwrap();
        assert!(open_entropy(&path).is_err());
        std::fs::remove_file(path).unwrap();
        let count=||std::fs::read_dir("/proc/self/fd").unwrap().count();
        let before=count();
        for _ in 0..100 { drop(open_entropy(Path::new("/dev/urandom")).unwrap()); }
        assert_eq!(count(),before);
    }
}
