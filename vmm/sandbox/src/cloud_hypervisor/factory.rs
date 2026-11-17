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

use anyhow::anyhow;
use containerd_sandbox::SandboxOption;

use crate::{
    cloud_hypervisor::{
        config::{CloudHypervisorVMConfig, ContainerStorageBackend},
        devices::{console::Console, fs::Fs, pmem::Pmem, rng::Rng, vsock::Vsock},
        CloudHypervisorVM,
    },
    utils::get_netns,
    vm::{ContainerStoragePolicy, VMFactory, CONSOLE_LOG_FILENAME, TASK_VSOCK_FILENAME},
};

pub struct CloudHypervisorVMFactory {
    vm_config: CloudHypervisorVMConfig,
}

// Reserve CIDs across concurrent factory calls and include persisted sandboxes
// after a sandboxer restart. Backend socket paths remain the isolation boundary.
fn reserve_fork_cid(base_dir: &str) -> containerd_sandbox::error::Result<u32> {
    use std::{
        collections::HashSet,
        sync::{Mutex, OnceLock},
    };
    static CIDS: OnceLock<Mutex<HashSet<u32>>> = OnceLock::new();
    let mut used = CIDS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap();
    if let Some(parent) = std::path::Path::new(base_dir).parent() {
        if let Ok(entries) = std::fs::read_dir(parent) {
            for entry in entries.flatten() {
                if let Ok(bytes) = std::fs::read(entry.path().join("sandbox.json")) {
                    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                        if let Some(cid) = value["vm"]["fork_cid"].as_u64() {
                            used.insert(cid as u32);
                        }
                    }
                }
            }
        }
    }
    for _ in 0..128 {
        let cid = rand::random::<u32>();
        if cid > 2 && cid < u32::MAX && used.insert(cid) {
            return Ok(cid);
        }
    }
    Err(anyhow!("unable to reserve a unique fork CID").into())
}

#[cfg(test)]
mod tests {
    use containerd_sandbox::{data::SandboxData, SandboxOption};

    use super::CloudHypervisorVMFactory;
    use crate::{
        cloud_hypervisor::config::{CloudHypervisorVMConfig, ContainerStorageBackend},
        vm::VMFactory,
    };

    #[tokio::test]
    async fn test_live_fork_agent_has_independent_transport() {
        use crate::vm::VM;
        let mut config = CloudHypervisorVMConfig::default();
        config.live_fork = true;
        config.live_fork_agent = true;
        config.hugepages = false;
        config.container_storage_backend = ContainerStorageBackend::VirtioBlk;
        config.common.image_path.clear();
        let factory = CloudHypervisorVMFactory::new(config);
        let first = factory
            .create_vm(
                "first",
                &SandboxOption {
                    base_dir: "/tmp/fork-first".into(),
                    sandbox: SandboxData::default(),
                },
            )
            .await
            .unwrap();
        let second = factory
            .create_vm(
                "second",
                &SandboxOption {
                    base_dir: "/tmp/fork-second".into(),
                    sandbox: SandboxData::default(),
                },
            )
            .await
            .unwrap();
        assert!(first.live_fork_agent());
        assert_ne!(first.fork_cid, second.fork_cid);
        assert_ne!(first.agent_socket, second.agent_socket);
        assert!(first.agent_socket.contains("fork-first/task.vsock"));
    }

    #[tokio::test]
    async fn test_writable_fork_seed_cannot_be_booted_after_sealing() {
        use std::os::unix::fs::PermissionsExt;

        use crate::vm::VM;
        let root = temp_dir::TempDir::new().unwrap();
        let state = root.path().join("state");
        std::fs::create_dir(&state).unwrap();
        let image = root.path().join("seed.raw");
        std::fs::write(&image, [0; 4096]).unwrap();
        let mut config = CloudHypervisorVMConfig::default();
        config.live_fork = true;
        config.live_fork_writable_disk = true;
        config.hugepages = false;
        config.container_storage_backend = ContainerStorageBackend::VirtioBlk;
        config.common.image_path = image.to_str().unwrap().into();
        let option = SandboxOption {
            base_dir: state.join("child").to_str().unwrap().into(),
            sandbox: SandboxData::default(),
        };
        std::fs::set_permissions(&image, std::fs::Permissions::from_mode(0o444)).unwrap();
        // Slots can still be created for descendants; only cold boot consumes
        // the seed. Root must also reject sealed seeds despite DAC override.
        let mut vm = CloudHypervisorVMFactory::new(config.clone())
            .create_vm("child", &option)
            .await
            .unwrap();
        assert!(vm
            .start()
            .await
            .unwrap_err()
            .to_string()
            .contains("sealed seeds"));
        config.common.image_path.clear();
        assert!(CloudHypervisorVMFactory::new(config)
            .create_vm("child", &option)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_live_fork_profile_constraints() {
        use crate::vm::VM;
        assert!(!CloudHypervisorVMConfig::default().live_fork);
        let mut config = CloudHypervisorVMConfig::default();
        config.live_fork = true;
        config.hugepages = false;
        config.container_storage_backend = ContainerStorageBackend::VirtioBlk;
        config.common.image_path.clear();
        let mut option = SandboxOption {
            base_dir: "/tmp/live-fork-test".into(),
            sandbox: SandboxData::default(),
        };
        let vm = CloudHypervisorVMFactory::new(config.clone())
            .create_vm("test", &option)
            .await
            .unwrap();
        assert!(vm.live_fork_profile());
        assert!(!vm.config.memory.shared);
        assert!(vm.devices.is_empty()); // no vsock, virtiofs, RNG or console device
        option.sandbox.netns = "/proc/self/ns/net".into();
        assert!(CloudHypervisorVMFactory::new(config.clone())
            .create_vm("test", &option)
            .await
            .is_ok());
        option.sandbox.netns.clear();
        config.hugepages = true;
        assert!(CloudHypervisorVMFactory::new(config.clone())
            .create_vm("test", &option)
            .await
            .is_err());
        config.hugepages = false;
        config.container_storage_backend = ContainerStorageBackend::Virtiofs;
        assert!(CloudHypervisorVMFactory::new(config)
            .create_vm("test", &option)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_create_vm_virtiofs_with_empty_virtiofsd_path_fails() {
        let mut config = CloudHypervisorVMConfig::default();
        config.container_storage_backend = ContainerStorageBackend::Virtiofs;
        config.virtiofsd.path = String::new();

        let factory = CloudHypervisorVMFactory::new(config);
        let s = SandboxOption {
            base_dir: "/tmp/test-sandbox".to_string(),
            sandbox: SandboxData::default(),
        };

        let err = factory.create_vm("test-id", &s).await.err().unwrap();
        assert!(
            err.to_string().contains("virtiofsd.path is not configured"),
            "expected virtiofsd.path error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_create_vm_virtio_blk_with_empty_virtiofsd_path_passes_validation() {
        let mut config = CloudHypervisorVMConfig::default();
        config.container_storage_backend = ContainerStorageBackend::VirtioBlk;
        config.virtiofsd.path = String::new();
        config.common.image_path = String::new();

        let factory = CloudHypervisorVMFactory::new(config);
        let s = SandboxOption {
            base_dir: "/tmp/test-sandbox".to_string(),
            sandbox: SandboxData::default(),
        };

        let result = factory.create_vm("test-id", &s).await;
        if let Err(ref e) = result {
            assert!(
                !e.to_string().contains("virtiofsd"),
                "virtio-blk mode must not fail virtiofsd path check, got: {}",
                e
            );
        }
    }
}

#[async_trait::async_trait]
impl VMFactory for CloudHypervisorVMFactory {
    type VM = CloudHypervisorVM;
    type Config = CloudHypervisorVMConfig;

    fn new(config: Self::Config) -> Self {
        Self { vm_config: config }
    }

    fn image_path(&self) -> &str {
        &self.vm_config.common.image_path
    }

    fn kernel_path(&self) -> &str {
        &self.vm_config.common.kernel_path
    }

    fn vcpus(&self) -> u32 {
        self.vm_config.common.vcpus
    }

    fn memory_mb(&self) -> u32 {
        self.vm_config.common.memory_in_mb
    }

    fn kernel_params(&self) -> &str {
        &self.vm_config.common.kernel_params
    }

    fn storage_policy(&self) -> ContainerStoragePolicy {
        ContainerStoragePolicy {
            storage_backend: self
                .vm_config
                .container_storage_backend
                .as_str()
                .to_string(),
            allow_large_bind_mount: self.vm_config.virtio_blk.allow_large_bind_mount,
            enable_reflink_cow: self.vm_config.virtio_blk.enable_reflink_cow,
            block_image_size_overhead_percent: self
                .vm_config
                .virtio_blk
                .block_image_size_overhead_percent,
            small_dir_max_files: self.vm_config.virtio_blk.small_dir_max_files,
            small_dir_max_bytes: self.vm_config.virtio_blk.small_dir_max_bytes,
            overlay_image_fallback_size_mb: self
                .vm_config
                .virtio_blk
                .overlay_image_fallback_size_mb,
            bind_image_fallback_size_mb: self.vm_config.virtio_blk.bind_image_fallback_size_mb,
        }
    }

    fn with_resources(&self, vcpus: u32, memory_mb: u32) -> Self {
        let mut config = self.vm_config.clone();
        config.common.vcpus = vcpus;
        config.common.memory_in_mb = memory_mb;
        Self { vm_config: config }
    }

    async fn create_vm(
        &self,
        id: &str,
        s: &SandboxOption,
    ) -> containerd_sandbox::error::Result<Self::VM> {
        if self.vm_config.container_storage_backend == ContainerStorageBackend::Virtiofs
            && self.vm_config.virtiofsd.path.is_empty()
        {
            return Err(anyhow!(
                "container_storage_backend is virtiofs but virtiofsd.path is not configured"
            )
            .into());
        }

        if self.vm_config.live_fork_writable_disk
            && (!self.vm_config.live_fork || self.vm_config.common.image_path.is_empty())
        {
            return Err(anyhow!(
                "live_fork_writable_disk requires live_fork and a dedicated raw image_path"
            )
            .into());
        }
        if self.vm_config.live_fork_agent && !self.vm_config.live_fork {
            return Err(anyhow!("live_fork_agent requires live_fork=true").into());
        }
        let netns = get_netns(&s.sandbox);
        let mut vm = CloudHypervisorVM::new(id, &netns, &s.base_dir, &self.vm_config);
        if self.vm_config.live_fork {
            if self.vm_config.hugepages
                || self.vm_config.container_storage_backend != ContainerStorageBackend::VirtioBlk
            {
                return Err(anyhow!("live_fork requires virtio-blk and private RAM").into());
            }
            vm.config.memory.shared = false;
            vm.config.memory.thp = Some(false);
            vm.config.cmdline = self.vm_config.common.kernel_params.clone();
            if !self.vm_config.common.image_path.is_empty() {
                let image = std::fs::canonicalize(&self.vm_config.common.image_path)?;
                let state_dir = std::fs::canonicalize(
                    std::path::Path::new(&s.base_dir)
                        .parent()
                        .ok_or_else(|| anyhow!("sandbox base directory has no parent"))?,
                )?;
                if image.starts_with(state_dir) {
                    return Err(anyhow!(
                        "live_fork disk must be outside sandbox state directories"
                    )
                    .into());
                }
                vm.add_device(
                    crate::cloud_hypervisor::devices::block::Disk::new(
                        "rootfs",
                        &self.vm_config.common.image_path,
                        !self.vm_config.live_fork_writable_disk,
                        // Avoid a second buffered writeback queue on the host
                        // for writable guest disks. QCOW metadata barriers remain.
                        self.vm_config.live_fork_writable_disk,
                    )
                    .raw(),
                );
            }
            let cid = if self.vm_config.live_fork_agent {
                Some(reserve_fork_cid(&s.base_dir)?)
            } else {
                None
            };
            if let Some(root) = std::env::var_os("KUASAR_FORK_RUNTIME_DIR") {
                vm.configure_fork_runtime(std::path::Path::new(&root))
                    .await?;
            }
            if let Some(cid) = cid {
                let socket = format!(
                    "{}/{}",
                    vm.fork_runtime_dir.as_deref().unwrap_or(&s.base_dir),
                    TASK_VSOCK_FILENAME
                );
                vm.add_device(Vsock::new(cid, &socket, "vsock"));
                vm.agent_socket = format!("hvsock://{}:1024", socket);
                vm.fork_cid = Some(cid);
                vm.config
                    .cmdline
                    .push_str(" task.sharefs_type=none task.container_storage_backend=virtio-blk");
            }
            return Ok(vm);
        }
        // add image as a disk
        if !self.vm_config.common.image_path.is_empty() {
            let rootfs_device = Pmem::new("rootfs", &self.vm_config.common.image_path, true);
            vm.add_device(rootfs_device);
        }

        // add virtio-rng device
        if !self.vm_config.entropy_source.is_empty() {
            let rng = Rng::new("rng", &self.vm_config.entropy_source);
            vm.add_device(rng);
        }

        // add vsock device
        // set guest cid
        // cid seems not important for cloud hypervisor
        let guest_socket_path = format!("{}/{}", s.base_dir, TASK_VSOCK_FILENAME);
        let vsock = Vsock::new(3, &guest_socket_path, "vsock");
        vm.add_device(vsock);
        vm.agent_socket = format!("hvsock://{}:1024", guest_socket_path);

        // add console device
        // TODO add log path parameter
        let console_path = format!("/tmp/{}-{}", id, CONSOLE_LOG_FILENAME);
        let console = Console::new(&console_path, "console");
        vm.add_device(console);

        // add virtio-fs device
        if !vm.virtiofsd_config.socket_path.is_empty() {
            let fs = Fs::new("fs", &vm.virtiofsd_config.socket_path, "kuasar");
            vm.add_device(fs);
        }

        Ok(vm)
    }
}
