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

pub(super) async fn capture_mode(
    config: &mut Value,
    work: &Path,
    published: &Path,
    local: bool,
) -> Result<()> {
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
        // Restored read-only images may also be QCOW wrappers. Preserve their
        // immutable backing chain in remote snapshots instead of requiring Raw.
        if lazy || ((local || disk["readonly"] == true) && disk["image_type"] == "Qcow2") {
            copy_chain(&chain, work, true).await?;
            disk["path"] = json!(published.join(source.file_name().unwrap()));
            continue;
        }

        if disk["readonly"] == true {
            // A durable snapshot must not depend on an image elsewhere on this
            // host. Reflinks preserve local CoW; chunk ingest deduplicates images.
            let source = disk["path"]
                .as_str()
                .ok_or_else(|| anyhow!("missing readonly disk path"))?;
            if disk["image_type"] != "Raw" {
                return Err(anyhow!("checkpoint readonly disk must be Raw"));
            }
            let name = format!("readonly-{index}.raw");
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
            if let Some(reference) = stored_reference(Path::new(source))? {
                write_stored_reference(&work.join(&name), &reference)?;
            }
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

/// Restore a new private qcow leaf with a private reflink of immutable history.
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
            let copied = copy_chain(&chain, directory, false).await?;
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
            if let Some(reference) = stored_reference(&source)? {
                write_stored_reference(&base, &reference)?;
            }
            std::fs::File::open(&base)?.sync_all()?;
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
        std::fs::set_permissions(
            &leaf,
            std::fs::Permissions::from_mode(if readonly { 0o400 } else { 0o600 }),
        )?;
        std::fs::File::open(&leaf)?.sync_all()?;
        disk["path"] = json!(leaf);
        disk["image_type"] = json!("Qcow2");
        disk["backing_files"] = json!(true);
        owned.push(leaf.clone());
        // Restore has no live-fork supervisor merge job. Compact only this
        // private, unopened copy, before another restore/fork can grow its chain.
        // Use the controller's exact binary; do not widen VMM seccomp policies.
        if !readonly && chain.len() >= 3 {
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
    std::fs::File::open(directory)?.sync_all()?;
    std::fs::File::open(directory.parent().unwrap())?.sync_all()?;
    Ok(owned)
}

const MANIFEST_MAGIC: &[u8; 8] = b"KSMAN001";
/// A small local descriptor is never itself uploaded as disk contents. The
/// checkpoint index directly retains the immutable disk Manifest it references.
// A host-only receipt for a fully materialized, immutable disk. This is NOT
// the lazy descriptor marker: guest disk bytes remain ordinary disk bytes.
// Size/mtime and mode reject subsequent writes or writable copies. Reflink
// copies get a new receipt only after verifying the immutable source receipt.
pub(super) fn stored_reference(path: &Path) -> Result<Option<Value>> {
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
    let file = std::fs::File::open(path)?;
    let meta = file.metadata()?;
    if meta.mode() & 0o222 != 0 {
        return Ok(None);
    }
    let mut bytes = [0u8; 4096];
    // SAFETY: valid descriptor, NUL-terminated name, and writable output buffer.
    let n = unsafe {
        nix::libc::fgetxattr(
            file.as_raw_fd(),
            b"user.kuasar.stored\0".as_ptr().cast(),
            bytes.as_mut_ptr().cast(),
            bytes.len(),
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
    let receipt: Value = serde_json::from_slice(&bytes[..n as usize])?;
    if receipt["size"] != meta.len()
        || receipt["mtime"] != meta.mtime()
        || receipt["mtime_nsec"] != meta.mtime_nsec()
    {
        return Ok(None);
    }
    let reference = &receipt["reference"];
    if reference["manifest"]
        .as_str()
        .is_none_or(|k| k.len() != 64 || !k.bytes().all(|b| b.is_ascii_hexdigit()))
        || reference["logical_bytes"] != meta.len()
    {
        return Err(anyhow!("Invalid immutable disk receipt"));
    }
    Ok(Some(reference.clone()))
}

pub(super) fn write_stored_reference(path: &Path, reference: &Value) -> Result<()> {
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
    let file = std::fs::File::open(path)?;
    let meta = file.metadata()?;
    if meta.mode() & 0o222 != 0 || reference["logical_bytes"] != meta.len() {
        return Err(anyhow!(
            "Stored disk receipt requires an immutable exact-size payload"
        ));
    }
    let reference = json!({"manifest":reference["manifest"],"logical_bytes":reference["logical_bytes"],"kind":reference["kind"].as_str().unwrap_or("raw")});
    let bytes = serde_json::to_vec(
        &json!({"reference":reference,"size":meta.len(),"mtime":meta.mtime(),"mtime_nsec":meta.mtime_nsec()}),
    )?;
    // Only called for newly materialized/copied files before publication.
    // Setting a user xattr requires owner-write permission even with an open fd.
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    // SAFETY: descriptor, NUL-terminated name and serialized input are valid.
    let rc = unsafe {
        nix::libc::fsetxattr(
            file.as_raw_fd(),
            b"user.kuasar.stored\0".as_ptr().cast(),
            bytes.as_ptr().cast(),
            bytes.len(),
            0,
        )
    };
    let error = if rc < 0 {
        Some(std::io::Error::last_os_error())
    } else {
        None
    };
    file.set_permissions(std::fs::Permissions::from_mode(0o400))?;
    if let Some(error) = error {
        return Err(error.into());
    }
    file.sync_all()?;
    Ok(())
}

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
    let mut owned = Vec::new();
    for path in paths {
        let target = destination.join(path.file_name().unwrap());
        if target.exists() {
            return Err(anyhow!("duplicate disk dependency name"));
        }
        run(
            "cp",
            &[
                "--reflink=auto",
                "--preserve=xattr",
                "--",
                path.to_str().unwrap(),
                target.to_str().unwrap(),
            ],
            None,
        )
        .await?;
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
                    file.sync_all()?;
                }
            }
        }
        if let Some(reference) = manifest_reference(path)? {
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))?;
            write_manifest_reference(&target, &reference)?;
        }
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o400))?;
        if let Some(reference) = stored_reference(path)? {
            write_stored_reference(&target, &reference)?;
        }
        std::fs::File::open(&target)?.sync_all()?;
        owned.push(target);
    }
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn materialized_receipt_survives_sealed_copy_but_rejects_writes() {
        let dir = temp_dir::TempDir::new().unwrap();
        let source = dir.path().join("base.raw");
        let destination = dir.path().join("copy");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(&source, vec![0x55; 4096]).unwrap();
        let reference = json!({"manifest":"a".repeat(64),"logical_bytes":4096,"kind":"raw"});
        assert!(write_stored_reference(&source, &reference).is_err());
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o400)).unwrap();
        write_stored_reference(&source, &reference).unwrap();
        assert_eq!(stored_reference(&source).unwrap(), Some(reference.clone()));
        let copies = copy_chain(&[source.clone()], &destination, false)
            .await
            .unwrap();
        assert_eq!(
            stored_reference(&copies[0]).unwrap(),
            Some(reference.clone())
        );
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(stored_reference(&source).unwrap().is_none());
        std::fs::write(&source, vec![0x99; 4096]).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&source)
            .unwrap()
            .set_modified(std::time::SystemTime::UNIX_EPOCH)
            .unwrap();
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o400)).unwrap();
        assert!(stored_reference(&source).unwrap().is_none());
        assert_eq!(stored_reference(&copies[0]).unwrap(), Some(reference));
        assert_eq!(std::fs::read(&copies[0]).unwrap(), vec![0x55; 4096]);
    }

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
    async fn remote_capture_preserves_readonly_qcow_dependencies() {
        let dir = std::env::temp_dir().join(format!("readonly-capture-{}", uuid::Uuid::new_v4()));
        let work = dir.join("capture");
        std::fs::create_dir_all(&work).unwrap();
        let base = dir.join("base.raw");
        let source = dir.join("image.qcow2");
        std::fs::write(&base, vec![0x55; 4096]).unwrap();
        let mut bytes = vec![0; 128];
        bytes[..4].copy_from_slice(b"QFI\xfb");
        bytes[4..8].copy_from_slice(&3u32.to_be_bytes());
        bytes[8..16].copy_from_slice(&104u64.to_be_bytes());
        bytes[16..20].copy_from_slice(&8u32.to_be_bytes());
        bytes[24..32].copy_from_slice(&4096u64.to_be_bytes());
        bytes[104..112].copy_from_slice(b"base.raw");
        std::fs::write(&source, &bytes).unwrap();
        let published = dir.join("published");
        let mut config = json!({"disks":[{"path":source,"image_type":"Qcow2","readonly":true,"backing_files":true}]});
        capture_mode(&mut config, &work, &published, false).await.unwrap();
        assert_eq!(config["disks"][0]["path"], json!(published.join("image.qcow2")));
        assert_eq!(std::fs::read(work.join("image.qcow2")).unwrap(), bytes);
        assert_eq!(std::fs::read(work.join("base.raw")).unwrap(), vec![0x55; 4096]);
        let spec = composition(&work.join("image.qcow2"), &[
            json!({"name":"base.raw","manifest":"raw-content","logical_bytes":4096}),
            json!({"name":"image.qcow2","manifest":"qcow-content","logical_bytes":128}),
        ]).unwrap();
        assert_eq!(spec["base"]["kind"], "raw");
        assert_eq!(spec["base"]["manifest"], "raw-content");
        assert_eq!(spec["layers"].as_array().unwrap().len(), 1);
        std::fs::write(&base, vec![0x99; 4096]).unwrap();
        assert_eq!(std::fs::read(work.join("base.raw")).unwrap(), vec![0x55; 4096]);
        std::fs::remove_dir_all(&dir).unwrap();
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
    let base_path = chain.last().ok_or_else(|| anyhow!("missing disk base"))?;
    let mut base = lookup(base_path)?;
    // A fully materialized raw backing file is ingested by Store::publish as
    // an ordinary authenticated Manifest. It has no lazy-disk kind marker.
    if base["kind"].is_null() {
        use std::io::Read;
        let mut magic = [0u8; 4];
        std::fs::File::open(base_path)?.read_exact(&mut magic)?;
        if &magic == b"QFI\xfb" {
            return Err(anyhow!("QCOW root without a raw backing file is unsupported"));
        }
        base["kind"] = json!("raw");
    }
    if !matches!(
        base["kind"].as_str(),
        Some("raw" | "manifest-disk" | "manifest-disk-graph")
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
