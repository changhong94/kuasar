// SPDX-License-Identifier: Apache-2.0
//! Private disk ownership for durable checkpoints and image-template restore.
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use serde_json::{json, Value};

async fn run(program: &str, args: &[&str], cwd: Option<&Path>) -> Result<()> {
    let started = std::time::Instant::now();
    let mut command = tokio::process::Command::new(program);
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let result = command.output().await?;
    if !result.status.success() {
        return Err(anyhow!(
            "disk checkpoint command failed: {}",
            String::from_utf8_lossy(&result.stderr)
        ));
    }
    tracing::info!(target: "checkpoint_timing", operation = args.first().copied().unwrap_or(program), program,
        elapsed_us = started.elapsed().as_micros() as u64, tool_output = %String::from_utf8_lossy(&result.stderr), "checkpoint disk command");
    Ok(())
}

/// The caller holds the guest barrier and has paused/drained block devices.
/// Block::pause persists qcow metadata before this immutable flattening step.
pub(super) async fn capture(config: &mut Value, work: &Path, published: &Path) -> Result<()> {
    capture_mode(config, work, published, false).await
}

pub(super) async fn capture_mode(config: &mut Value, work: &Path, published: &Path, local: bool) -> Result<()> {
    let disks = config["disks"]
        .as_array_mut()
        .ok_or_else(|| anyhow!("missing disk configuration"))?;
    if disks.iter().filter(|d| d["readonly"] == false).count() > 1 {
        return Err(anyhow!("checkpoint supports one writable application disk"));
    }
    for (index, disk) in disks.iter_mut().enumerate() {
        let source = Path::new(
            disk["path"]
                .as_str()
                .ok_or_else(|| anyhow!("missing disk path"))?,
        );
        let (chain, _) = disk_chain(source)?;
        let lazy = chain
            .iter()
            .map(|p| manifest_reference(p))
            .collect::<Result<Vec<_>>>()?
            .iter()
            .any(Option::is_some);
        if lazy || (local && disk["image_type"] == "Qcow2") {
            // Local checkpoint publication fsyncs every payload before rename.
            // Keep captures private until that barrier; avoid serial flushes
            // of the same files here and again in the commit phase.
            copy_chain_with_sync(&chain, work, true, !(local && batch_checkpoint_sync())).await?;
            disk["path"] = json!(published.join(source.file_name().unwrap()));
            continue;
        }

        if disk["readonly"] == true {
            // A durable snapshot must not depend on an image elsewhere on this
            // host. Reflinks preserve local CoW; chunk ingest deduplicates images.
            let source = disk["path"]
                .as_str()
                .ok_or_else(|| anyhow!("missing readonly disk path"))?;
            let name = format!("readonly-{index}.raw");
            if disk["image_type"] == "Qcow2" {
                // Generic slots may seal readonly images in QCOW2. Convert only
                // an independently owned, quiesced chain, never the live disk.
                let export = work.join(format!(".readonly-export-{index}"));
                std::fs::create_dir(&export)?;
                std::fs::set_permissions(&export, std::fs::Permissions::from_mode(0o700))?;
                let copies = copy_chain(&chain, &export, true).await?;
                let target = work.join(&name);
                run("qemu-img", &["convert", "-f", "qcow2", "-O", "raw",
                    copies[0].to_str().unwrap(), target.to_str().unwrap()], None).await?;
                std::fs::File::open(&target)?.sync_all()?;
                std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o400))?;
                std::fs::remove_dir_all(&export)?;
                disk["path"] = json!(published.join(name));
                disk["image_type"] = json!("Raw");
                disk["backing_files"] = json!(false);
                continue;
            }
            if disk["image_type"] != "Raw" {
                return Err(anyhow!("checkpoint readonly disk must be Raw or Qcow2"));
            }
            run(
                "cp",
                &[
                    "--reflink=auto",
                    "--",
                    source,
                    work.join(&name).to_str().unwrap(),
                ],
                None,
            )
            .await?;
            std::fs::set_permissions(work.join(&name), std::fs::Permissions::from_mode(0o400))?;
            disk["path"] = json!(published.join(name));
            continue;
        }
        if !matches!(disk["image_type"].as_str(), Some("Raw" | "Qcow2")) {
            return Err(anyhow!(
                "checkpoint requires explicit Raw or Qcow2 disk format"
            ));
        }
        let source = disk["path"]
            .as_str()
            .ok_or_else(|| anyhow!("missing writable disk path"))?;
        let name = format!("writable-{index}.raw");
        let target = work.join(&name);
        // VMM uses an exclusive OFD lock even when paused. Preserve that lock:
        // reflink the managed directory while IO is drained, then flatten the
        // independently owned copies. Relative backing names remain valid.
        let source = Path::new(source);
        let parent = source
            .parent()
            .ok_or_else(|| anyhow!("missing disk parent"))?;
        if parent.file_name().and_then(|s| s.to_str()) != Some("fork-disks") {
            return Err(anyhow!("writable checkpoint disk is not managed"));
        }
        let export = work.join(format!(".disk-export-{index}"));
        std::fs::create_dir(&export)?;
        std::fs::set_permissions(&export, std::fs::Permissions::from_mode(0o700))?;
        for entry in std::fs::read_dir(parent)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err(anyhow!("non-regular managed disk layer"));
            }
            run(
                "cp",
                &[
                    "--reflink=always",
                    "--",
                    entry.path().to_str().unwrap(),
                    export.join(entry.file_name()).to_str().unwrap(),
                ],
                None,
            )
            .await?;
        }
        let copy = export.join(
            source
                .file_name()
                .ok_or_else(|| anyhow!("missing disk filename"))?,
        );
        run(
            "qemu-img",
            &[
                "convert",
                "-O",
                "raw",
                copy.to_str().unwrap(),
                target.to_str().unwrap(),
            ],
            None,
        )
        .await?;
        std::fs::remove_dir_all(export)?;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o400))?;
        std::fs::File::open(&target)?.sync_all()?;
        disk["path"] = json!(published.join(name));
        disk["image_type"] = json!("Raw");
        disk["backing_files"] = json!(false);
    }
    Ok(())
}

/// Restore an independently owned writable QCOW top and private reflink history.
/// An existing private QCOW copy needs no additional empty overlay.
/// Both names live in the VM's managed fork-disks directory, so live fork can
/// hard-link history into each child's own directory before parent destruction.
pub(super) async fn stage(
    config: &mut Value,
    directory: &Path,
    label: &str,
    hypervisor_pid: u32,
) -> Result<Vec<PathBuf>> {
    tokio::fs::create_dir_all(directory).await?;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
    let mut owned = Vec::new();
    for (index, disk) in config["disks"]
        .as_array_mut()
        .ok_or_else(|| anyhow!("missing disks"))?
        .iter_mut()
        .enumerate()
    {
        let source = PathBuf::from(
            disk["path"]
                .as_str()
                .ok_or_else(|| anyhow!("missing disk base"))?,
        );
        let (chain, size) = disk_chain(&source)?;
        let readonly = disk["readonly"] == true;
        let (base_name, format) = if disk["image_type"] == "Qcow2" {
            let copied = copy_chain_with_sync(&chain, directory, false, !batch_checkpoint_sync()).await?;
            owned.extend(copied);
            if readonly {
                disk["path"] = json!(directory.join(source.file_name().unwrap()));
                continue;
            }
            (
                source.file_name().unwrap().to_str().unwrap().to_owned(),
                "qcow2",
            )
        } else if disk["image_type"] == "Raw" {
            let name = format!("{label}-{index}.raw");
            let base = directory.join(&name);
            run(
                "cp",
                &[
                    "--reflink=auto",
                    "--preserve=xattr",
                    "--",
                    source.to_str().unwrap(),
                    base.to_str().unwrap(),
                ],
                None,
            )
            .await?;
            if let Some(reference) = manifest_reference(&source)? {
                // Bind to this host's service, never a saved Unix socket path.
                std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o600))?;
                write_manifest_reference(&base, &reference)?;
            }
            std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o400))?;
            if !batch_checkpoint_sync() { sync_payload(&base, "restore_base")?; }
            owned.push(base);
            (name, "raw")
        } else {
            return Err(anyhow!("unsupported checkpoint disk format"));
        };
        let leaf_name = format!("{label}-{index}.qcow2");
        let leaf = directory.join(&leaf_name);
        if leaf.exists() {
            return Err(anyhow!("checkpoint disk generation exists"));
        }
        let reuse_private_top = batch_checkpoint_sync() && !readonly && format == "qcow2";
        if reuse_private_top {
            // copy_chain created a distinct inode (reflink or independent
            // copy), never a hard link to the checkpoint. It can be the
            // writable top directly; adding an empty overlay would cause an
            // unnecessary format/flush and often an immediate compaction.
            let private_top = directory.join(&base_name);
            use std::os::unix::fs::MetadataExt;
            let private = std::fs::symlink_metadata(&private_top)?;
            let original = std::fs::metadata(&source)?;
            if !private.is_file() || private.nlink() != 1
                || (private.dev() == original.dev() && private.ino() == original.ino()) {
                return Err(anyhow!("restore top is not an independently owned file"));
            }
            std::fs::rename(&private_top, &leaf)?;
            owned.retain(|path| path != &private_top);
        } else {
        // -u does not open the metadata-only Manifest base as if it were data.
        // The authenticated logical size is explicit; CH verifies it on open.
        run(
            "qemu-img",
            &[
                "create",
                "-u",
                "-f",
                "qcow2",
                "-F",
                format,
                "-b",
                &base_name,
                &leaf_name,
                &size.to_string(),
            ],
            Some(directory),
        )
        .await?;
        }
        std::fs::set_permissions(
            &leaf,
            std::fs::Permissions::from_mode(if readonly { 0o400 } else { 0o600 }),
        )?;
        if !batch_checkpoint_sync() { sync_payload(&leaf, "restore_leaf")?; }
        disk["path"] = json!(leaf);
        disk["image_type"] = json!("Qcow2");
        disk["backing_files"] = json!(true);
        owned.push(leaf.clone());
        // Restore has no live-fork supervisor merge job. Compact only this
        // private, unopened copy, before another restore/fork can grow its chain.
        // Use the controller's exact binary; do not widen VMM seccomp policies.
        if !readonly && chain.len() >= if reuse_private_top { 4 } else { 3 } {
            run(&format!("/proc/{hypervisor_pid}/exe"),
                &["--compact-restored-disk", leaf.to_str().unwrap()], None).await?;
        }
    }
    // A directory may contain several disks. Remove only paths copied by this
    // staging operation and no longer referenced by ANY configured disk.
    let mut live = std::collections::HashSet::new();
    for disk in config["disks"].as_array().unwrap() {
        live.extend(disk_chain(Path::new(disk["path"].as_str().unwrap()))?.0);
    }
    for path in &owned {
        if !live.contains(path) { std::fs::remove_file(path)?; }
    }
    owned.retain(|path| live.contains(path));
    if batch_checkpoint_sync() {
        // Copies and newly created QCOW metadata remain private until all
        // payloads and directory entries are durable. Reading them during
        // private compaction requires visibility, not an intermediate flush.
        let directory = directory.to_owned();
        tokio::task::spawn_blocking(move || super::checkpoint::sync_tree(&directory)).await??;
    } else {
        std::fs::File::open(directory)?.sync_all()?;
    }
    std::fs::File::open(directory.parent().unwrap())?.sync_all()?;
    Ok(owned)
}

const MANIFEST_MAGIC: &[u8; 8] = b"KSMAN001";
/// A small local descriptor is never itself uploaded as disk contents. The
/// checkpoint index directly retains the immutable disk Manifest it references.
pub(super) fn manifest_reference(path: &Path) -> Result<Option<Value>> {
    use std::os::unix::fs::FileExt;
    let file = std::fs::File::open(path)?;
    use std::os::unix::io::AsRawFd;
    let mut marker = [0u8; 1];
    // SAFETY: the fd, NUL-terminated attribute name and output buffer are valid.
    let n = unsafe {
        nix::libc::fgetxattr(
            file.as_raw_fd(),
            b"user.kuasar.manifest\0".as_ptr().cast(),
            marker.as_mut_ptr().cast(),
            1,
        )
    };
    if n < 0 {
        let e = std::io::Error::last_os_error();
        if matches!(
            e.raw_os_error(),
            Some(nix::libc::ENODATA | nix::libc::EOPNOTSUPP)
        ) {
            return Ok(None);
        }
        return Err(e.into());
    }
    if n != 1 || (marker != [1] && marker != [2]) {
        return Err(anyhow!("invalid Manifest disk marker"));
    }
    let mut head = [0; 82];
    if file.read_at(&mut head[..8], 0)? != 8 || &head[..8] != MANIFEST_MAGIC {
        return Err(anyhow!("invalid Manifest disk magic"));
    }
    file.read_exact_at(&mut head, 0)?;
    let size = u64::from_le_bytes(head[8..16].try_into().unwrap());
    let key = std::str::from_utf8(&head[16..80])?;
    if size == 0 || size % 512 != 0 || !key.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(anyhow!("invalid Manifest disk reference"));
    }
    Ok(Some(
        json!({"manifest":key,"logical_bytes":size,"kind":if marker==[2] {"manifest-disk-graph"} else {"manifest-disk"}}),
    ))
}
pub(super) fn write_manifest_reference(path: &Path, entry: &Value) -> Result<()> {
    let socket = std::env::var("KUASAR_CHECKPOINT_DISK_SOCKET")?;
    if !socket.starts_with('/') || socket.len() >= 108 || socket.contains('\0') {
        return Err(anyhow!("invalid Manifest disk socket"));
    }
    let size = entry["logical_bytes"]
        .as_u64()
        .ok_or_else(|| anyhow!("missing disk size"))?;
    let key = entry["manifest"]
        .as_str()
        .ok_or_else(|| anyhow!("missing disk Manifest"))?;
    if size == 0
        || size % 512 != 0
        || key.len() != 64
        || !key.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(anyhow!("invalid Manifest disk reference"));
    }
    let mut bytes = MANIFEST_MAGIC.to_vec();
    bytes.extend(size.to_le_bytes());
    bytes.extend(key.as_bytes());
    bytes.extend((socket.len() as u16).to_le_bytes());
    bytes.extend(socket.as_bytes());
    std::fs::write(path, bytes)?;
    use std::os::unix::io::AsRawFd;
    let file = std::fs::File::open(path)?;
    let marker = [if entry["kind"] == "manifest-disk-graph" {
        2u8
    } else {
        1u8
    }];
    // SAFETY: valid fd and input buffers; only the host can set this attribute.
    let rc = unsafe {
        nix::libc::fsetxattr(
            file.as_raw_fd(),
            b"user.kuasar.manifest\0".as_ptr().cast(),
            marker.as_ptr().cast(),
            1,
            0,
        )
    };
    if rc < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o400))?;
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

/// Only managed relative QCOW chains are accepted. No aliases, traversal or
/// missing bases: a truncated chain must never become an implicit zero disk.
fn disk_chain(path: &Path) -> Result<(Vec<PathBuf>, u64)> {
    use std::os::unix::fs::FileExt;
    let mut current = path.to_path_buf();
    let mut paths = Vec::new();
    let mut size = 0;
    loop {
        if paths.len() >= 10 || paths.contains(&current) {
            return Err(anyhow!("invalid disk dependency chain"));
        }
        let meta = std::fs::symlink_metadata(&current)?;
        if !meta.is_file() {
            return Err(anyhow!("non-regular disk dependency"));
        }
        let file = std::fs::File::open(&current)?;
        let mut head = [0; 32];
        file.read_exact_at(&mut head, 0)?;
        let qcow = &head[..4] == b"QFI\xfb";
        if paths.is_empty() {
            size = if qcow {
                u64::from_be_bytes(head[24..32].try_into().unwrap())
            } else {
                manifest_reference(&current)?
                    .and_then(|v| v["logical_bytes"].as_u64())
                    .unwrap_or(meta.len())
            };
        }
        paths.push(current.clone());
        if !qcow {
            break;
        }
        let off = u64::from_be_bytes(head[8..16].try_into().unwrap());
        let len = u32::from_be_bytes(head[16..20].try_into().unwrap()) as usize;
        if off == 0 && len == 0 {
            break;
        }
        if len == 0
            || len > 1023
            || off < 32
            || off
                .checked_add(len as u64)
                .map_or(true, |end| end > meta.len())
        {
            return Err(anyhow!("invalid QCOW backing name"));
        }
        let mut bytes = vec![0; len];
        file.read_exact_at(&mut bytes, off)?;
        let name = String::from_utf8(bytes)?;
        let name = Path::new(&name);
        if name.components().count() != 1
            || !matches!(
                name.components().next(),
                Some(std::path::Component::Normal(_))
            )
        {
            return Err(anyhow!("non-local disk backing dependency"));
        }
        current = current.parent().unwrap().join(name);
    }
    Ok((paths, size))
}
async fn copy_chain(
    paths: &[PathBuf],
    destination: &Path,
    quiesced_capture: bool,
) -> Result<Vec<PathBuf>> {
    copy_chain_with_sync(paths, destination, quiesced_capture, true).await
}

pub(super) fn batch_checkpoint_sync() -> bool {
    std::env::var("KUASAR_CHECKPOINT_BATCH_SYNC").as_deref() != Ok("0")
}

fn sync_payload(path: &Path, phase: &str) -> Result<()> {
    let started = std::time::Instant::now();
    let result = std::fs::File::open(path)?.sync_all();
    tracing::info!(target: "checkpoint_timing", phase, path = %path.display(),
        elapsed_us = started.elapsed().as_micros() as u64, ok = result.is_ok(), "checkpoint payload sync");
    result?;
    Ok(())
}

async fn copy_chain_with_sync(
    paths: &[PathBuf], destination: &Path, quiesced_capture: bool, durable: bool,
) -> Result<Vec<PathBuf>> {
    let mut owned = Vec::new();
    // One process per chain, not per dependency. Validate every destination
    // first; do not touch the source chain or publish until all copies have
    // completed and each file has crossed the existing durability barrier.
    let mut args = vec!["--reflink=auto", "--preserve=xattr", "--"];
    let mut names = std::collections::HashSet::new();
    for path in paths {
        let name = path.file_name().ok_or_else(|| anyhow!("missing disk dependency name"))?;
        if !names.insert(name) || destination.join(name).exists() {
            return Err(anyhow!("duplicate disk dependency name"));
        }
        args.push(path.to_str().ok_or_else(|| anyhow!("non-UTF8 disk dependency path"))?);
    }
    if !paths.is_empty() {
        args.push(destination.to_str().ok_or_else(|| anyhow!("non-UTF8 disk directory"))?);
        run("cp", &args, None).await?;
    }
    for path in paths {
        let target = destination.join(path.file_name().unwrap());
        if quiesced_capture {
            use std::os::unix::fs::FileExt;
            let file = std::fs::File::open(&target)?;
            let mut head = [0; 80];
            if file.read_at(&mut head, 0)? == 80
                && &head[..4] == b"QFI\xfb"
                && u32::from_be_bytes(head[4..8].try_into().unwrap()) == 3
            {
                let flags = u64::from_be_bytes(head[72..80].try_into().unwrap());
                if flags & !1 != 0 {
                    return Err(anyhow!("unsupported QCOW capture features"));
                }
                if flags & 1 != 0 {
                    // Block::pause has drained IO and durably flushed L1/L2 and
                    // data, but the live writer keeps its dirty/open bit until
                    // close. Seal only this independent immutable copy. Never
                    // clear the original writer's bit or remove its syncs.
                    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))?;
                    let file = std::fs::OpenOptions::new().write(true).open(&target)?;
                    file.write_all_at(&0u64.to_be_bytes(), 72)?;
                    if durable { sync_payload(&target, "seal_header")?; }
                }
            }
        }
        if let Some(reference) = manifest_reference(path)? {
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))?;
            write_manifest_reference(&target, &reference)?;
        }
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o400))?;
        if durable { sync_payload(&target, "seal_file")?; }
        owned.push(target);
    }
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn managed_chain_rejects_escape_and_cycles() {
        let dir = std::env::temp_dir().join(format!("disk-chain-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("top.qcow2");
        for name in ["../escape", "top.qcow2"] {
            let mut data = vec![0u8; 32 + name.len()];
            data[..4].copy_from_slice(b"QFI\xfb");
            data[8..16].copy_from_slice(&32u64.to_be_bytes());
            data[16..20].copy_from_slice(&(name.len() as u32).to_be_bytes());
            data[24..32].copy_from_slice(&512u64.to_be_bytes());
            data[32..].copy_from_slice(name.as_bytes());
            std::fs::write(&path, data).unwrap();
            assert!(disk_chain(&path).is_err());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn sealing_quiesced_copy_never_changes_live_writer() {
        let dir = std::env::temp_dir().join(format!("disk-seal-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let source = dir.join("writer.qcow2");
        let dest = dir.join("capture");
        std::fs::create_dir(&dest).unwrap();
        let mut bytes = vec![0u8; 128];
        bytes[..4].copy_from_slice(b"QFI\xfb");
        bytes[4..8].copy_from_slice(&3u32.to_be_bytes());
        bytes[72..80].copy_from_slice(&1u64.to_be_bytes());
        std::fs::write(&source, &bytes).unwrap();
        let copies = copy_chain(&[source.clone()], &dest, true).await.unwrap();
        assert_eq!(std::fs::read(&source).unwrap(), bytes);
        bytes[72..80].fill(0);
        assert_eq!(std::fs::read(&copies[0]).unwrap(), bytes);
        assert_eq!(
            std::fs::metadata(&copies[0]).unwrap().permissions().mode() & 0o222,
            0
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn remote_capture_accepts_readonly_qcow_without_changing_source() {
        let dir = std::env::temp_dir().join(format!("readonly-capture-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let base = dir.join("base.raw");
        let bytes = vec![37u8; 1024 * 1024];
        std::fs::write(&base, &bytes).unwrap();
        run("qemu-img", &["create", "-f", "qcow2", "-F", "raw", "-b", "base.raw", "leaf.qcow2"], Some(&dir)).await.unwrap();
        let leaf = dir.join("leaf.qcow2");
        let original = std::fs::read(&leaf).unwrap();
        let work = dir.join("capture");
        let published = dir.join("published");
        std::fs::create_dir(&work).unwrap();
        let mut config = json!({"disks":[{"path":leaf,"image_type":"Qcow2","readonly":true,"backing_files":true}]});
        capture_mode(&mut config, &work, &published, false).await.unwrap();
        assert_eq!(std::fs::read(work.join("readonly-0.raw")).unwrap(), bytes);
        assert_eq!(std::fs::read(&base).unwrap(), bytes);
        assert_eq!(std::fs::read(&leaf).unwrap(), original);
        assert_eq!(config["disks"][0]["image_type"], "Raw");
        assert_eq!(config["disks"][0]["backing_files"], false);
        assert_eq!(std::fs::metadata(work.join("readonly-0.raw")).unwrap().permissions().mode() & 0o222, 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn batch_copy_keeps_relative_chain_and_rejects_duplicates_before_writing() {
        let dir = std::env::temp_dir().join(format!("batch-chain-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let base = dir.join("base.raw");
        let bytes = vec![83u8; 512 * 1024];
        std::fs::write(&base, &bytes).unwrap();
        run("qemu-img", &["create", "-f", "qcow2", "-F", "raw", "-b", "base.raw", "leaf.qcow2"], Some(&dir)).await.unwrap();
        let leaf = dir.join("leaf.qcow2");
        let original = std::fs::read(&leaf).unwrap();
        let dest = dir.join("copy");
        std::fs::create_dir(&dest).unwrap();
        assert!(copy_chain(&[base.clone(), base.clone()], &dest, false).await.is_err());
        assert_eq!(std::fs::read_dir(&dest).unwrap().count(), 0);
        let copied = copy_chain(&[leaf.clone(), base.clone()], &dest, false).await.unwrap();
        assert_eq!(disk_chain(&copied[0]).unwrap().0, copied);
        assert_eq!(std::fs::read(&copied[1]).unwrap(), bytes);
        assert_eq!(std::fs::read(&leaf).unwrap(), original);
        assert_eq!(std::fs::read(&base).unwrap(), bytes);
        for path in copied {
            assert_eq!(std::fs::metadata(path).unwrap().permissions().mode() & 0o222, 0);
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn staged_chain_survives_source_removal_with_batched_barrier() {
        let dir = std::env::temp_dir().join(format!("stage-sync-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let base = dir.join("base.raw");
        let data = vec![59u8; 512 * 1024];
        std::fs::write(&base, &data).unwrap();
        run("qemu-img", &["create", "-f", "qcow2", "-F", "raw", "-b", "base.raw", "old.qcow2"], Some(&dir)).await.unwrap();
        let old = dir.join("old.qcow2");
        std::fs::set_permissions(&old, std::fs::Permissions::from_mode(0o400)).unwrap();
        let mut config = json!({"disks":[{"path":old,"readonly":false,"image_type":"Qcow2","backing_files":true}]});
        let dest = dir.join("staged");
        let owned = stage(&mut config, &dest, "restored", 0).await.unwrap();
        assert_eq!(owned.len(), 2);
        assert_eq!(std::fs::metadata(&old).unwrap().permissions().mode() & 0o222, 0);
        let top = Path::new(config["disks"][0]["path"].as_str().unwrap());
        assert_ne!(std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(top).unwrap()),
                   std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&old).unwrap()));
        assert_ne!(std::fs::metadata(top).unwrap().permissions().mode() & 0o222, 0);
        std::fs::remove_file(old).unwrap();
        std::fs::remove_file(base).unwrap();
        let output = dir.join("check.raw");
        run("qemu-img", &["convert", "-f", "qcow2", "-O", "raw", config["disks"][0]["path"].as_str().unwrap(), output.to_str().unwrap()], None).await.unwrap();
        assert_eq!(std::fs::read(output).unwrap(), data);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ordinary_disk_data_is_not_a_host_manifest_reference() {
        let path = std::env::temp_dir().join(format!("manifest-test-{}", uuid::Uuid::new_v4()));
        let mut data = vec![0u8; 512];
        data[..8].copy_from_slice(MANIFEST_MAGIC);
        std::fs::write(&path, data).unwrap();
        assert!(manifest_reference(&path).unwrap().is_none());
        std::fs::remove_file(path).unwrap();
    }
}

/// Compose immutable QCOW payloads into one logical disk block map. Layer
/// manifests remain chunk-backed; no data clusters are read to build this map.
pub(super) fn composition(path: &Path, files: &[Value]) -> Result<Value> {
    let (chain, size) = disk_chain(path)?;
    let lookup = |p: &Path| -> Result<Value> {
        let name = p
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("bad disk name"))?;
        files
            .iter()
            .find(|f| f["name"] == name)
            .cloned()
            .ok_or_else(|| anyhow!("missing disk dependency"))
    };
    let base = lookup(chain.last().ok_or_else(|| anyhow!("missing disk base"))?)?;
    if !matches!(
        base["kind"].as_str(),
        Some("manifest-disk" | "manifest-disk-graph")
    ) {
        return Err(anyhow!("disk map requires a published Manifest base"));
    }
    let layers = chain[..chain.len() - 1]
        .iter()
        .rev()
        .map(|p| lookup(p))
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({"size":size,"base":base,"layers":layers}))
}
