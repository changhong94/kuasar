// SPDX-License-Identifier: Apache-2.0
//! Private disk ownership for durable checkpoints and image-template restore.
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::{os::unix::fs::PermissionsExt, path::{Path, PathBuf}};

async fn run(program: &str, args: &[&str], cwd: Option<&Path>) -> Result<()> {
    let mut command = tokio::process::Command::new(program);
    command.args(args);
    if let Some(cwd) = cwd { command.current_dir(cwd); }
    let result = command.output().await?;
    if !result.status.success() {
        return Err(anyhow!("disk checkpoint command failed: {}", String::from_utf8_lossy(&result.stderr)));
    }
    Ok(())
}

/// The caller holds the guest barrier and has paused/drained block devices.
/// Block::pause persists qcow metadata before this immutable flattening step.
pub(super) async fn capture(config: &mut Value, work: &Path, published: &Path) -> Result<()> {
    let disks = config["disks"].as_array_mut().ok_or_else(|| anyhow!("missing disk configuration"))?;
    if disks.iter().filter(|d| d["readonly"] == false).count() > 1 {
        return Err(anyhow!("checkpoint supports one writable application disk"));
    }
    for (index, disk) in disks.iter_mut().enumerate() {
        if disk["readonly"] == true { continue; }
        if !matches!(disk["image_type"].as_str(), Some("Raw" | "Qcow2")) {
            return Err(anyhow!("checkpoint requires explicit Raw or Qcow2 disk format"));
        }
        let source = disk["path"].as_str().ok_or_else(|| anyhow!("missing writable disk path"))?;
        let name = format!("writable-{index}.raw");
        let target = work.join(&name);
        // VMM uses an exclusive OFD lock even when paused. Preserve that lock:
        // reflink the managed directory while IO is drained, then flatten the
        // independently owned copies. Relative backing names remain valid.
        let source = Path::new(source);
        let parent = source.parent().ok_or_else(|| anyhow!("missing disk parent"))?;
        if parent.file_name().and_then(|s| s.to_str()) != Some("fork-disks") {
            return Err(anyhow!("writable checkpoint disk is not managed"));
        }
        let export = work.join(format!(".disk-export-{index}"));
        std::fs::create_dir(&export)?;
        std::fs::set_permissions(&export, std::fs::Permissions::from_mode(0o700))?;
        for entry in std::fs::read_dir(parent)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() { return Err(anyhow!("non-regular managed disk layer")); }
            run("cp", &["--reflink=always", "--", entry.path().to_str().unwrap(),
                export.join(entry.file_name()).to_str().unwrap()], None).await?;
        }
        let copy = export.join(source.file_name().ok_or_else(|| anyhow!("missing disk filename"))?);
        run("qemu-img", &["convert", "-O", "raw", copy.to_str().unwrap(), target.to_str().unwrap()], None).await?;
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
pub(super) async fn stage(config: &mut Value, directory: &Path, label: &str) -> Result<Vec<PathBuf>> {
    tokio::fs::create_dir_all(directory).await?;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
    let mut owned = Vec::new();
    for (index, disk) in config["disks"].as_array_mut().ok_or_else(|| anyhow!("missing disks"))?.iter_mut().enumerate() {
        if disk["readonly"] == true { continue; }
        if disk["image_type"] != "Raw" { return Err(anyhow!("checkpoint writable base is not Raw")); }
        let source = disk["path"].as_str().ok_or_else(|| anyhow!("missing writable base"))?;
        let base_name = format!("{label}-{index}.raw");
        let leaf_name = format!("{label}-{index}.qcow2");
        let base = directory.join(&base_name);
        let leaf = directory.join(&leaf_name);
        if base.exists() || leaf.exists() { return Err(anyhow!("checkpoint disk generation already exists")); }
        run("cp", &["--reflink=always", "--", source, base.to_str().unwrap()], None).await?;
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o400))?;
        run("qemu-img", &["create", "-f", "qcow2", "-F", "raw", "-b", &base_name, &leaf_name], Some(directory)).await?;
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o600))?;
        for path in [&base, &leaf] { std::fs::File::open(path)?.sync_all()?; }
        disk["path"] = json!(leaf);
        disk["image_type"] = json!("Qcow2");
        disk["backing_files"] = json!(true);
        owned.extend([base, leaf]);
    }
    std::fs::File::open(directory)?.sync_all()?;
    std::fs::File::open(directory.parent().unwrap())?.sync_all()?;
    Ok(owned)
}
