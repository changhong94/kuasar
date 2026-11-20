# Fork Lab — standalone LAN console

The dedicated deployment on 108 is available at `http://10.213.17.108:18088`.
This is separate from benchmark deployments. Do not stop or reconfigure benchmark
services when operating this console. Development starts on `sandbox_fork_snapshot`; promotion to staging and
`sandbox_fork` follows explicit user approval.

## Deployment

- Application: `/srv/forklab/app` (pinned copy, including E2B helpers).
- VMM and sandboxer: `/srv/forklab/bin` (pinned tested release binaries).
- Kernel/initramfs/CNI: dedicated copies under `/srv/forklab`.
- Data: `/srv/forklab/data`, dedicated 160 GiB sparse XFS loop volume with reflink.
- State catalog: `data/console.sqlite`, SQLite WAL with FULL synchronization.
- Immutable images: `data/images`; image metadata: `data/registry`.
- Local durable snapshots: `data/templates`; running VM state: `data/state`.
- Network: bridge `forklab0`, subnet `10.208.108.0/24`, namespace `fl-sb-*`,
  independent host-local IPAM under `data/ipam`.
- Runtime sockets: `/run/forklab`; no runtime admin socket is exposed via TCP.
- `forklab-web.service`, `forklab-sandboxer.service` and
  `srv-forklab-data.mount` are enabled at boot. Unit/config copies are in `deploy/`.

Read status without disturbing VMs:

```sh
systemctl status forklab-web forklab-sandboxer srv-forklab-data.mount
journalctl -u forklab-web -u forklab-sandboxer
```

Restarting only `forklab-web` preserves VMs and the catalog. Restarting the
sandboxer may interrupt VMs; do not do it while colleagues are using them.
Snapshots and the catalog are persistent. A host reboot is not equivalent to
resuming every unsaved process: explicitly restore a durable snapshot as needed.

## Semantics

Five image references are resolved to linux/amd64 digests. Python uses the
existing comparison digest `sha256:2fe5997d249a808b8eeea52c58a1dbffbba28754dc11699ef5c029f2d818ce79`.
The four requested additional references are in `import_images.py`. Imports use
skopeo and umoci; the catalog records the selected digest, OCI configuration,
compressed size, logical disk size, allocated bytes and preparation time.
No language-specific warmup is performed. Image ENV, WORKDIR and USER are applied
to the sandbox's persistent idle process; short-lived image CMD/ENTRYPOINT is
not automatically run. The sandbox remains available for subsequent work.

Each sandbox has one vCPU, 2 GiB private guest RAM, an 8 GiB writable runtime disk,
and an immutable image disk. Application overlay upper/work directories live
on the writable guest disk (`/lower`), not on a host temporary directory.
The service allows six concurrent sandboxes by default; destroy unused instances
through the UI. The cap is local to this standalone service.

Pause/resume use the VMM lifecycle API. Live snapshot uses
`sandbox-snapshot-live`: the source resumes after its safety and persistence
barriers, then saving finishes in the background. Only durable snapshots can be
restored. Pause duration and request-to-durable duration are runtime measurements,
not browser polling latency. This deployment selects local persistence.

Display names are independent of filesystem IDs. New instances use
`sandbox-<image repository name>-<five random digits>` and new snapshots use
`snapshot-<image repository name>-<five random digits>`. Registry/organization
and tag/digest are omitted. The server regenerates collisions under the same
lock as reservation, including names of deleted objects. Existing names and
IDs remain unchanged. No snapshot-name input is shown. Instances and saved
snapshots are shown in separate Sandbox and Snapshot tables. Restore on a
saved snapshot creates a new independent instance without deleting the snapshot.
The demo does not expose VM pause/resume controls.
Only running sandboxes can capture a snapshot; both UI and server enforce this.
Base images cannot be deleted through the API. Deleting a snapshot hides it and
prevents new restores, while its backing files remain pinned until existing
restored children are destroyed. Existing sandboxes continue independently.

This is a shared internal LAN console without per-user accounts. Anyone with
network access to this URL can manage its listed sandboxes. Requests from foreign
browser origins are rejected; the API exposes only the fixed catalog and
lifecycle actions, not arbitrary host shell commands or runtime socket forwarding.

## Validation

`test_console.py` covers duplicate names, paused-state rejection, publication
races, deletion with live descendants and the absence of an image-delete API.
The installed E2B helpers are needed for imports. Browser and real-guest evidence
is stored outside the repository under `/home/huawei/codex-work/fork-console-108`.
The tests exercise pause/resume, UI disabling, process continuation and restore,
file isolation, deletion of a referenced snapshot and independent destruction.

No PDF, binary, imported image or generated benchmark result belongs in this
source directory. Do not commit or push without a new user instruction.
