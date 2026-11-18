# Host reboot recovery validation on 113

The physical `10.213.17.113` host was **not rebooted**. Its boot ID stayed unchanged;
108 code was not touched. With user approval, an isolated nested KVM Linux host
ran the Kuasar/Cloud Hypervisor stack and real Linux sandbox guests. It had a
separate root disk, a reflink-enabled persistent XFS data disk, private SSH keys,
and a loopback-only forwarded SSH port. The fixture was powered off after all
leased test sandboxes were destroyed. Disk images are retained outside Git.

## Evidence

- `reboot1.json`: normal Linux reboot; original parent/child PID, in-memory random
  secret and counter, and independent durable files recovered; parent destroyed
  first and child continued. Initial validation exposed the distinction between
  device-config MAC and runtime guest/CNI MAC. The guard rejected resume without
  consuming the checkpoint. After fixing the identity comparison and rebooting
  the fixture again, that original checkpoint passed all continuation checks.
- `reboot2.json`: QMP forced reset, without a normal Linux shutdown, after both
  pause acknowledgments. Both guests recovered; child destroyed first, parent
  continued; a new child forked from the recovered parent retained in-memory
  state and independent writable storage, and was independently destroyed.
- `reboot3.json`: final build with restored-controller lifecycle monitoring;
  another Linux reboot recovered both guests. The test SIGKILLed only its own
  child controller through a pidfd, observed normal Stopped lifecycle state,
  deleted it, and verified the parent kept running before deletion.
- `physical-113-smoke.json`: final build deployed to the owned physical-113 fork
  service; running-parent fork, repeated durable pause/resume, independent
  files, parent deletion while child paused, and paused-child deletion passed.
- `reboot-summary.json`: boot IDs, deployment binary hashes and test counts.
  Python suite: 108 run, 101 passed, 7 existing skips, no failures. SQLite's
  escaped-ID/scalar failure is fixed; both PERSIST and WAL round trips are tested.

## Reproduction and boundaries

These scripts are a host-specific fixture, not a production installer. Paths
refer to `/home/hong/src/research/fork-features-113/nested` and the existing
`python-latest-113` artifacts. Review disk paths and port ownership before reuse.
Generate a new private SSH key and generic initrd; `build_root.py` creates the
separate root/data images, and `populate.py` installs the existing assets and an
owned service. Install the newly built sandboxer before enabling that service.
The fixture's SSH wrapper must target only `root@127.0.0.1:22239`; no SSH key is
included in Git. `launch.sh` runs only the owned QEMU host. `reset_nested.py`
checks that host's PID/name/disk before issuing QMP reset.

The bootstrap reconciles only its private seed CNI lease. Persistent data must
be mounted before sandboxer startup. Host /run namespaces and sockets are
reconstructed, not persisted or trusted across kernel boots. Boot ID + PID
start time protect VMM ownership, and new controllers join normal exit monitoring.

Run `test_reboot_guest.py prepare rebootN` inside the isolated host and wait for
`CHECKPOINTS_DURABLE`. Reboot **that isolated host only**, then run the verify
phase (or `wait_verify.py` on 113). The tests use the unchanged pinned Python
image and generate fresh process secrets; they do not recreate the workload
process after reboot. The timeout/readiness polling is functional-test setup,
not a startup-performance benchmark.

Recovery is from a successfully persisted, paused version 2 checkpoint, not
unsaved running RAM. Resume consumes the paused checkpoint. This does not claim
continuous checkpointing, exactly-once external effects, CRI Pod recovery,
version 1 cold recovery, or physical power-loss certification. All persistence
barriers and the original running-parent CoW fork path remain enabled.
