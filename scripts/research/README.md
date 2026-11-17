# Same-host live sandbox fork (experimental)

This path connects Kuasar's registered sandbox lifecycle to the Cloud Hypervisor
`sandbox_fork` branch's `vm.fork` HTTP API. It does not invoke snapshot/restore,
write `memory.img`, or copy guest RAM in Kuasar. CH captures vCPU/device state,
quiesces its host threads, forks private anonymous RAM, and restores both VMMs.
Kuasar reconnects to their API sockets and resumes both VMs before reporting success.

## Supported profile

Set `[hypervisor] live_fork = true`, `container_storage_backend = "virtio-blk"`,
`hugepages = false`. This defaults to **off**. Use the locally built CH fork binary.
RAM is private with THP disabled. By default the optional root image is **raw and read-only**;
it must live outside the sandbox state directory and remain available and unchanged
while any descendant exists. Kernel parameters must explicitly select `console=ttyS0`
and an appropriate root/init. Serial is exposed at `<sandbox-directory>/serial.sock`.

By default this remains the serial-only VM profile. Set
`[hypervisor] live_fork_agent = true` to enable **vsock and the Kuasar guest agent**.
The seed guest must run `vmm-task`, listening on `vsock://-1:1024`. Both parent and
child then advertise a real `ttrpc+hvsock://.../task.vsock:1024` task address and
support Kuasar's normal Controller task-resource updates and the agent Task API.

Each sandbox has a reserved, distinct CID and its own Unix backend path. CH sends
virtio-vsock TRANSPORT_RESET on restore, so Linux re-reads the child CID, aborts
connected sockets and retains wildcard listeners. The backend does not inherit
host connections, and rejects packets with another source CID. Kuasar reconnects
both agents, resumes event forwarding and injects fresh entropy into each guest.
CID is routing metadata; separate backend sockets/VMs provide the host connection
isolation. Clients must reconnect existing Task/serial connections after fork.

A source may contain running containers and exec processes. The guest kernel,
runc state and agent Task objects are inherited in RAM; the child does not run
Task Create/Start, restore a container checkpoint, or copy its rootfs. Kuasar clones
container/exec records and the Controller `extensions.tasks` resource list, writes
small metadata files in the child's own bundle directory, and preserves IDs.
Device hotplug, virtiofs, arbitrary host volumes and external/terminal IO remain
unsupported. The NIC and writable-disk extensions below support independent CNI
networking and a managed CoW disk. Container rootfs must already be guest-resident
(for example, in initramfs or on the managed guest disk). The fixture populates it only when creating the source container.
This supports real OCI containers but is not yet a general CRI image/rootfs path.
Guest application identities beyond the CID and entropy are not automatically rewritten.
Snapshot pools cannot be combined with this profile. Admin-created sandboxes do not
add host cgroup resource limits. Only trusted host administrators should access the
admin socket.

## Admin API

Use Kuasar's existing `--admin-listen /path/admin.sock`. Send one JSON object plus
newline per Unix socket connection; the response is one JSON line. IDs must have
12–64 alphanumeric, underscore or hyphen characters.

```json
{"action":"sandbox-boot","sandbox_id":"seed00000001"}
{"action":"sandbox-fork","source_id":"seed00000001","sandbox_id":"child0000001"}
{"action":"sandbox-get","sandbox_id":"child0000001"}
{"action":"sandbox-destroy","sandbox_id":"seed00000001"}
{"action":"sandbox-destroy","sandbox_id":"child0000001"}
```

`boot` starts a new seed; `fork` derives an independently registered sandbox from a
running source. Both return `live_fork` metadata with PID, API/serial sockets and,
for a child, CH operation timings. Reconnect serial clients after a fork. Children
can be forked again, including after the original parent's destruction.

The sandboxer never calls `fork()` itself. Its Tokio and gRPC workers keep running.
CH alone owns the single-thread fork boundary. A child VMM is not necessarily a
child process of the sandboxer: pidfd readiness monitors exit and pidfd signaling
terminates it without relying on `waitpid` or accidentally signaling a reused PID.
Persisted process start times are checked on adoption. Descendant exit codes are
currently reported as zero (exit time and stopped state are available).

Duplicate IDs and pre-existing directories are rejected. Fork mutations are never
retried blindly after a disconnected HTTP response: operation history is reconciled,
excluding operations that predated this attempt. A failed request rolls back the
child and resumes the source. If CH's outcome cannot be established, the sandbox
record is retained so `sandbox-destroy` can reconcile it later. Full crash-atomic
transactions across a sandboxer crash during fork are not implemented or validated.

## Real Linux validation

Prerequisites: working `/dev/kvm`, Rust stable/nightly tools, a static C compiler,
`cpio`, Python 3, the sibling CH checkout and built debug binary, and a Linux kernel
with initramfs, serial and virtio-blk support built in.

```sh
scripts/research/run-live-fork.sh /path/to/vmlinuz /tmp/ku-live-fork-test
```

Use a fresh short output path (Unix socket pathname limits apply). The runner builds
Kuasar and a Linux PID 1 fixture from the sibling CH repository. It drives only the
Kuasar admin API and serial sockets. It checks:

- Two concurrent forks to one ID yield one success and one rejection.
- A 2-vCPU, 256-MiB Linux guest initializes a 32-MiB buffer to 10; after fork each
  guest independently observes 10 and writes 20 or 30, then keeps reporting its value.
- Restored virtio-blk state reads the same read-only disk from both guests.
- Parent-first and descendant-first destruction preserve the surviving guest.
- A surviving child can fork again; unexpected exit is detected through pidfd.
- Independent sandbox metadata, failed-source rollback, existing-directory protection.
- No `memory.img`; CH's fork-boundary combined guest-RAM PSS equals the baseline,
  providing evidence that RAM was shared rather than copied eagerly.

Results, guest logs and VMM boundary measurements are written to the output directory.
The measured fork latency includes Kuasar's orchestration and both VM resumes; it is
an individual development run, not a benchmark distribution.

## Guest agent and container validation

This checkout patches its shim dependency to the sibling
`kuasar-io/rust-extensions` checkout, also on `sandbox_fork`, based on
`1127b78fd29f55853f63a1146748ad33b25ca6b7`. Keep that checkout alongside Kuasar.
It fixes a cgroup-v1 OOM monitor ownership bug: wrapping `EventFd::as_raw_fd()`
in another owning File double-closed the descriptor when a container was deleted,
occasionally aborting the guest agent. Ownership now transfers through `OwnedFd`
without unsafe duplication. Its eventfd teardown regression test and repeated real
container deletions validate the fix.

The runner below additionally needs host `busybox` (static), `bash`, `runc`, `ldd`,
`strip`, and matching Linux virtio-vsock modules. It packages the actual Kuasar
`vmm-task` binary using its runc backend (`--no-default-features`) into initramfs.
The fixture does not use mock Task services or launch the test containers directly
with guest shell commands. Shell RPCs prepare the guest-resident bundle; actual
container creation/start/kill/wait/delete use the agent's normal Task service.

```sh
scripts/research/run-agent-fork.sh /tmp/ch-fork-vmlinuz 6.8.0-138-generic /tmp/ku-agent-test
```

This runs a 2-vCPU, 512-MiB Linux guest and verifies:

- Kuasar Controller Status advertises each sandbox's own task address.
- Controller Update prepares metadata/spec; real Task Create/Start runs runc containers.
- Three generations use the same container ID, with independent values and heartbeats.
- Guest-observed CIDs differ; the source CID remains unchanged after each fork.
- An established pre-fork RPC connection fails after transport reset.
- Missing/reserved/duplicate child CIDs are rejected without disrupting the source agent.
- After each Task Delete the agent must still answer RPC, with no abort/panic
  in captured guest serial logs. Killing a descendant container/VM leaves its source live; destroying the original
  parent leaves the child's container and agent operational.
- A resident 32-MiB probe inherited across fork observes 10 in every branch, then
  independently writes 20/30/40; no `memory.img` is created.
- The first two-process fork preserves combined RAM PSS exactly at the boundary.
  For later forks with a living ancestor, new-parent+child PSS is **not** directly
  comparable to the source's previous PSS: shared pages change from a half-share
  to two third-shares. These later measurements are retained without asserting equality.

The full test output, host log, configuration and `results.json` are saved in the
output directory. This validates an empty seed with post-fork container creation;
sandboxer crash recovery during an in-flight fork is still outside the validation.

## Forking running containers

Use the same `sandbox-fork` request against a source containing registered Task
containers. Before CH pauses the VM, Kuasar holds both sandbox locks and asks the
agent to acquire a token-owned lifecycle barrier. Existing Task operations drain
(up to 10 seconds), new ones fail with `FAILED_PRECONDITION`, and the agent reports
its container/exec inventory. Kuasar rejects mismatches rather than inheriting
partly registered tasks. `Wait` is exempt because it is read-only and may remain
pending for the lifetime of a process. Guest applications continue executing until
CH pauses their vCPUs; natural exits and exit-monitor state are inherited too.

After each VM resumes, Kuasar reconnects its agent and releases that VM's copy of
the barrier. Errors release the source barrier and clean up the child. Transport
reset terminates old RPCs, including pending Wait calls: reconnect and issue State
or Wait on the desired sandbox's advertised task socket. Applications and their
open guest-resident files keep their execution state. No SetupSandbox, Task Create
or Task Start is issued for inherited containers.

Container IDs are scoped by sandbox/task socket. By default the child's event
namespace is `kuasar-fork-<child-sandbox-id>`. The containerd integration below binds
a concrete namespace and registers the inherited containers and tasks there.
Registration does not rewrite the guest's runc IDs or process state. The direct
agent fixture in this section tests the lower-level Kuasar path; the containerd
fixture additionally tests the real daemon and normal management APIs.

Run the additional real Linux test (same prerequisites as the agent test):

```sh
FORK_TEST_SCRIPT=test-running-container-fork.py scripts/research/run-agent-fork.sh \
  /tmp/ch-fork-vmlinuz 6.8.0-138-generic /tmp/ku-running-test
```

It starts one real runc container and one exec process in the source, each owning
16 MiB initialized to 10 and an open log file, then forks twice while they run.
It checks identical guest PIDs, `/proc` start times, random startup nonces and
startup count 1 in all branches, followed by advancing counters and independent
RAM/file values 20/21, 30/31 and 40/41. Each process checks its entire buffer before
changing it. It also checks independent host metadata, normal Controller removal,
Task Kill/Wait/Delete, parent-first and descendant-first destruction, distinct CIDs,
connection reset, pending Wait and failed-fork cleanup. The first two-sharer fork
must preserve combined guest-RAM PSS exactly; no `memory.img` may appear.

## containerd management of inherited tasks

The sibling `kuasar-io/containerd` repository is on `sandbox_fork`, based on Kuasar's
`v0.2.0-kuasar` tag (`21eaa26be101e0802609621c657b92d2cdbff581`). Use its matching
`containerd` and `ctr` binaries and the newly built Kuasar host/agent. The stock
system daemon is not replaced. Configure a `sandbox` proxy plugin named `vmm`
pointing to Kuasar's gRPC socket, and set `KUASAR_CONTAINERD_TTRPC_ADDRESS` on the
sandboxer to that daemon's ttrpc event socket (default remains
`/run/containerd/containerd.sock.ttrpc`). The sandbox controller gRPC plugin needs
the CRI runtime/podsandbox dependencies enabled, even when not using the CRI API.
The isolated fixture disables only the CRI gRPC service.

For an existing live-fork seed with registered running containers:

```sh
ctr --address /path/containerd.sock --namespace parent sandboxes import-running \
  --admin-address /path/kuasar-admin.sock seed00000001
ctr --address /path/containerd.sock --namespace parent sandboxes fork \
  --admin-address /path/kuasar-admin.sock --target-namespace child \
  seed00000001 child0000001
ctr --address /path/containerd.sock --namespace child containers list
ctr --address /path/containerd.sock --namespace child tasks list
ctr --address /path/containerd.sock --namespace child tasks exec \
  --null-io --exec-id check samecontainer /bin/sh -c 'echo child > /result'
ctr --address /path/containerd.sock --namespace child tasks kill \
  --signal SIGKILL samecontainer
ctr --address /path/containerd.sock --namespace child tasks delete samecontainer
ctr --address /path/containerd.sock --namespace child containers delete samecontainer
ctr --address /path/containerd.sock --namespace child sandboxes remove child0000001
```

In this CLI flow, parent and child must use different namespaces because the guest
IDs are preserved. The CRI flow below instead assigns distinct external IDs in
`k8s.io`.
If omitted, the child's namespace defaults to `kuasar-fork-<child-id>`. Conflicting
sandbox/container IDs are rejected before forking. A sandbox binds to one namespace;
it cannot be imported into another namespace. Existing registration can be retried.
Containerd namespaces provide logical identity scoping, not authorization for clients
with unrestricted access to its socket.

The import command writes sandbox/container records through containerd APIs and
registers each inherited task through the runtime manager. The OCI annotation
`io.kuasar.fork.adopt=true` selects a guarded registration path: verify sandbox
namespace, advertised endpoint, empty external IO and inherited task inventory,
then query State. It never calls guest Create/Start. Runtime bundles persist the
sandbox ID and bootstrap endpoint, so containerd restart reloads the registrations.
Existing exec IDs are available through normal Task APIs as well as new execs.

Sandboxed hvsock Task RPCs open fresh connections, allowing the next normal call to
work after VM fork resets vsock. Mutations are never automatically replayed after
an ambiguous transport failure. Task and exec deletion update the sandbox resource
list and Kuasar's metadata; failures preserve records needed for reconciliation.
The agent now publishes init/exec TaskExit and forwards all Task lifecycle events.
Event polls have bounded lifetimes and participate in the fork barrier, preventing
old connections from inheriting a waiting event consumer. The host routes events
to the bound namespace. In-flight clients still need to handle transient RPC errors.

The CLI flow above uses containerd's sandbox/container/task services directly.
For CRI PodSandbox/Container records managed by kubelet, use the integration below. Rootfs and IO restrictions above still
apply. `--null-io` runs exec without host FIFO/TTY transport. Remove exec processes,
then the task/container, before removing their sandbox. Full crash-atomic
registration across Kuasar and containerd is not provided: on partial registration
the child is retained and the error names the namespace/ID for retrying
`import-running`; it is not destroyed underneath registered tasks.

Run real Linux validation with an isolated daemon (requires root/KVM and the build
tools described above):

```sh
scripts/research/run-containerd-fork.sh \
  /tmp/ch-fork-vmlinuz 6.8.0-138-generic /tmp/ku-containerd-test
# Repeat with descendant-first destruction:
DELETE_CHILD_FIRST=1 scripts/research/run-containerd-fork.sh \
  /tmp/ch-fork-vmlinuz 6.8.0-138-generic /tmp/ku-containerd-child-first
```

The fixture creates source processes once, imports the parent, forks through `ctr`,
and uses normal `ctr containers/tasks/sandboxes` commands for both branches. It
checks namespace and ID collision rejection, scoped exit events, inherited PIDs,
independent 16-MiB init/exec buffers, task registration after containerd restart,
exec after peer destruction, and complete metadata/VM cleanup. It also asserts no
`memory.img` and unchanged combined guest-RAM PSS at the two-process fork boundary.

## Experimental CRI / Kubernetes Pod fork

The CRI integration uses the same VM fork, with no memory image and no eager RAM
copy. `RunPodSandbox` allocates a new sandbox ID and stores the **new** Pod UID,
name and Kubernetes namespace. It records a plan of inherited containers.
`CreateContainer` allocates distinct CRI/containerd IDs in `k8s.io`, and
`StartContainer` adopts the existing guest task without creating or starting a
replacement process. Task RPCs translate the external ID to the inherited guest
ID; task events translate back. Exec IDs stay unchanged. Pending read-only Wait
calls reconnect after the fork transport reset; mutating RPCs are not replayed.
Normal CRI stop/remove and kubelet garbage collection own subsequent teardown.

Enable the isolated containerd daemon with `KUASAR_FORK_ADMIN_ADDRESS` pointing
to the sandboxer's admin socket. Initial non-CRI seed IDs must be listed in
`KUASAR_FORK_TEMPLATE_IDS` (comma separated). A source Pod must be Ready, in the
same Kubernetes namespace, and have `io.kuasar.fork.allow: "true"`. These checks
are a prototype opt-in policy, not per-user Kubernetes authorization: control who
can create Pods using this RuntimeClass and these annotations through admission.

Configure runtime `kuasar-vmm` with `sandboxer = "vmm"`, runtime type
`io.containerd.kuasar-vmm.v1` and `io_type = "streaming"`, plus the `vmm` sandbox
proxy plugin. Keep the CRI service enabled. Create RuntimeClass `kuasar-fork` with
handler `kuasar-vmm`, then submit a Pod like:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: fork-child
  annotations:
    io.kuasar.fork.source: "<source-Pod-UID>"
    io.kuasar.fork.allow: "true"
spec:
  runtimeClassName: kuasar-fork
  nodeName: "<same-node-as-source>"
  hostNetwork: true
  automountServiceAccountToken: false
  enableServiceLinks: false
  restartPolicy: Never
  containers:
  - name: samecontainer
    image: fork-workload:latest
    imagePullPolicy: Never
    command: ["/bin/fork-workload", "init"]
```

Container names and application configuration must match the inherited inventory.
Use `kubectl get pods`, `kubectl exec`, and `kubectl delete pod` normally. The new
Pod UID and container ID identify host management records; already-running guest
processes retain their PIDs, environment, open files, and rootfs contents.

This remains a deliberately restricted research runtime. The validated fixture is
one root container in a guest-resident rootfs, empty inherited main IO, no arbitrary
host volumes, passthrough devices, resource limits or changed credentials. The
NIC and writable-disk extensions are described below.
New exec supports CRI streaming stdout/stderr. Output is drained after process
exit, and streaming channels are released only after the consumer reaches EOF. Main-process logs remain inherited
(the fixture uses `/dev/null`), and Kubernetes log/termination-file collection is
not implemented. Kubelet's `/etc/hosts` and termination-log mounts are accepted as
metadata but not installed into the inherited rootfs. No new downward API or
service-account files are injected. `hostNetwork: true` skips CNI; it does **not**
attach the guest to the host network. Network semantics, arbitrary Pod security
contexts, probes needing networking, in-place container restart (attempt > 0),
init containers, and full crash-atomic cross-service registration are outside this
profile. Recreate the whole Pod to obtain another inherited instance.

Run on an isolated root/KVM test host, with K3s `v1.31.13+k3s1` installed as
`../../tools/k3s-v1.31` relative to this repository, and the Rust/Go toolchains above:

```sh
scripts/research/run-kubernetes-fork.sh /tmp/ch-fork-vmlinuz 6.8.0-138-generic /tmp/k8fork1
DELETE_CHILD_FIRST=1 scripts/research/run-kubernetes-fork.sh /tmp/ch-fork-vmlinuz 6.8.0-138-generic /tmp/k8fork2
```

Use fresh short output paths (Unix socket length limit). This starts its own K3s
server, kubelet, containerd and sandboxer; it does not replace the system daemon.
The fixture creates its real runc workload once in a Linux guest, imports a
metadata-only OCI image for that preloaded rootfs, then creates parent and child
Pods through Kubernetes. It checks distinct Pod UIDs/CRI IDs, inherited startup
nonce, independent 16-MiB memory values via streaming `kubectl exec`, survivor
execution after peer deletion, and eventual sandbox cleanup by kubelet. The seed
is destroyed before the second fork so both boundaries have exactly two RAM
sharers: combined guest-RAM PSS must equal its pre-fork value. No `memory.img` may
appear. Logs, observed Pod records and `results.json` are saved in the output.


## Network-enabled forks and E2B SDK

The optional `FORK_NETWORK=1` fixture now creates a bridge-CNI network and runs
with independent guest NICs, TAPs, MACs and Pod IPs. The earlier hostNetwork-only
profile above remains available for regression testing. `FORK_E2B=1` exercises
the same lifecycle through an unmodified E2B SDK. See
[the E2B integration documentation](../../integrations/e2b/README.md) for API setup,
network reset semantics, supported scope and reproducible validation.

## Writable ext4 disk inheritance

Set `[hypervisor] live_fork_writable_disk = true` together with `live_fork = true`
and a dedicated raw `image_path`. The default stays read-only. The image must live
outside sandbox state directories, on the same host filesystem, and be exclusively
owned by this seed VM. The guest mounts it normally; containers must already use
rootfs on that guest disk before fork. HostPath, PVC and arbitrary CRI image/volume
provisioning are not added by this feature.

Kuasar supplies `<parent>/fork-disks` and `<child>/fork-disks` to CH. CH quiesces
block I/O and creates private qcow2 leaves over the sealed old disk without copying
the disk or RAM. Each sandbox owns hard links to all backing ancestors. Normal
sandbox/Pod/E2B deletion removes only that sandbox's links. The surviving branch
continues to read its own chain after the source VM and its state directory are gone.

The initial raw image becomes read-only after first fork and is consumed as a seed;
Kuasar rejects another cold boot of a sealed seed, including when running as root.
Creating descendant slots remains permitted. Keep this configured seed file in
place while using the factory; it is caller-owned and not automatically deleted.
Do not modify sealed layers, use external host writers or move individual backing
files. One buffered writable disk is supported. Generation count is no longer
limited: CH automatically merges allocated deltas when a chain reaches four files,
keeping each branch at three or four files including the raw base. Cross-filesystem
hard links, shared guest filesystems and persistent restart/crash recovery remain
outside this profile.

To build and run the complete writable-disk SDK test on the existing research host:

```sh
sudo -E env FORK_WRITABLE_DISK=1 FORK_NETWORK=1 FORK_E2B=1 \
  MAX_EXEC_SECONDS=2 KUASAR_RPC_TIMING=1 \
  scripts/research/run-kubernetes-fork.sh /tmp/ch-fork-vmlinuz \
  6.8.0-138-generic /tmp/writable-sdk
```

Use a fresh output directory/seed per run. Add `DELETE_CHILD_FIRST=1
LATENCY_PARALLEL=1` for the reverse deletion order and concurrent commands. Omit
`FORK_E2B` for the native kubectl test. The fixture builds a sparse 512 MiB ext4
seed, moves the agent's existing state files onto it before creating the seed
container, and creates all container rootfs data there. `writable-disk.json` records
ext4 mount evidence, leaf allocation sizes, inode/hard-link identity and sealed
layer SHA256 hashes. Tests write durable and un-fsynced data before fork, then
independently overwrite/create/unlink/rename/truncate files, sync and evict guest
page cache, re-read ancestor data and execute binaries after peer destruction.
`results.json` additionally verifies inherited running-process identity, separate
Pod UIDs/container IDs/MACs/IPs, normal exec, bidirectional networking, lifecycle
cleanup, no `memory.img`, and unchanged combined guest-RAM PSS at the fork boundary.

## Continuous generations, compaction and reclamation

No extra API parameter is needed. The normal Kuasar fork, CRI Pod annotation and
E2B SDK fork paths use CH's bounded disk-chain compaction automatically. Shared
ancestors stay immutable; fresh merged layers replace only the current branch's
history. Per-VM hard links retain history needed by other branches. Unreachable
local layer links are reclaimed after a successful pivot, and normal sandbox
removal releases the rest. This bounds chain depth rather than total storage for
live historical snapshots. Compaction of sealed ancestors runs after resume in a
background worker; the live leaf continues receiving writes. The next fork joins and atomically publishes the
candidate before pausing the guest. Chain length is at most four files, with at
most one extra unpublished `.merge-*` candidate per VM. It does not create
`memory.img` or copy RAM. The caller-owned raw seed remains required by the
configured Kuasar factory.

The containerd sandbox Task `Wait` path tolerates temporary missing/refused/reset
vsock endpoints and the guest's explicit fork-frozen response. A successful read-only
State probe resets its reconnect window, so later forks do not inherit an already
expired timer. Transport reconstruction must not publish a false process-exit event.
Cancellation and the existing bounded unavailability window remain in effect;
mutations are still never replayed automatically.

Run on a fresh short output directory with the existing real-guest dependencies:

```sh
sudo -E env CH_FORK_VERIFY_PAGES=1 FORK_WRITABLE_DISK=1 FORK_NETWORK=1 \
  FORK_E2B=1 FORK_GENERATIONS=24 FORK_DELETE_SEED=83 MAX_EXEC_SECONDS=2 \
  scripts/research/run-kubernetes-fork.sh /tmp/ch-fork-vmlinuz \
  6.8.0-138-generic /tmp/multigen-sdk
```

Omit `FORK_E2B` to use native Kubernetes/CRI operations. `FORK_GENERATIONS` must be
at least 12. Change `FORK_DELETE_SEED` to exercise another reproducible permutation.
The fixture retains all generations while a separate older branch writes during
forks. Each generation verifies inherited file contents, independent modifications,
cache-cold ancestor reads and the running process nonce. It checks live chains and
owned directory file counts, immutable inode hashes, distinct Pod/container/IP
identities, randomized deletion and another fork from a middle generation after
its ancestor directories have actually been reclaimed. All disk directories and
CNI allocations must disappear after final deletion. `chain-samples.json`,
`multigen-timings.json` and `results.json` contain the evidence; the host log records
compaction input files/bytes/time, removed link counts and pre-restore RAM PFN identity.
Pairwise PSS is recorded for context but is not an equality assertion when older
ancestors remain alive; its sharing denominator changes with the number of VMs.

### Background compaction and concurrent direct I/O

Add `FORK_IO_STRESS=1` to the multi-generation fixture (and rebuild the guest
initramfs) to enable the inherited workload's direct-I/O disk checker. Each branch
repeatedly writes/fsyncs 64 KiB patterns across qcow2 cluster boundaries in an
8 MiB file, reads other slots with `O_DIRECT`, and checks their contents against
its inherited in-memory expectations. Branch-specific commands change the patterns.
Progress must continue through fork, merge, cache drops and ancestor destruction;
read/write/check failures exit the workload and fail the fixture. Older branches
also execute writes concurrently with the newest branch's forks.

The result records parent/child `pause_ms`, background publication count and
`merge_wait_before_pause_ms`, in addition to PFN equality, chain/candidate bounds,
sealed inode hashes, exec timings and deletion cleanup. Compare a preserved
foreground binary and the new binary with the same freshly built guest, fresh
512 MiB raw seeds, generation count and deletion seed. Pause spans `vm.pause` to
successful explicit `vm.resume`; the background join wait happens before that span.
Do not run builds or other fixtures concurrently with timing measurements.

### Pause-stage profiling

`CH_FORK_TIMING=1` enables CH's operation/PID-specific monotonic phase timeline.
Run the same multi-generation/direct-I/O fixture with that variable and use:

```sh
python3 scripts/research/analyze-fork-stages.py BEFORE/host.log AFTER/host.log
python3 scripts/research/compare-fork-pauses.py BEFORE/host.log AFTER/host.log
```

The phase analyzer keeps parent and child restore times separate, reports the
critical path until both are ready, and treats API binding as concurrent work.
Each distribution includes its sample count; missing/interleaved records from
older diagnostic binaries are not invented or treated as zero. Total pause uses
the later of the two explicit resumes. Keep full PFN verification and direct I/O
enabled for both sides of a comparison. The current status reconciliation polls
at 1 ms for 250 ms and then 20 ms; mutation replay and the timeout policy are unchanged.

For reproducible old/new comparisons, `FORK_SANDBOXER_BIN=/absolute/path/to/binary`
overrides the fixture's sandboxer binary without replacing the current build.
Select CH independently with `hypervisor.path` in the fixture input TOML. Keep
kernel, initramfs, vCPU/RAM sizes, fresh disk size and test variables identical.


### Teardown and tail-latency comparisons

`analyze-fork-stages.py` now includes VMGenID slot removal, device/CPU/memory
object release, retained KVM VM destruction, and the post-preparation join wait.
`compare-fork-pauses.py` reports nearest-rank P99 in addition to median/P95/max.
Always retain sample counts: with fewer than 100 forks, P99 is the observed max,
not a reliable population tail estimate. Compare equal generation counts and
workloads, and retain failed attempts separately rather than folding recovery
pauses into successful fork latencies. The complete fixture continues to check
concurrent direct I/O, inherited live process state, RAM PFNs, independent Pod
networks, shuffled deletion, and another fork after ancestor reclamation.

### FD ownership during long fork lineages

Set `FORK_FD_CENSUS=1` with `FORK_GENERATIONS=100` to record guest process
FD targets/limits and each host process's FD table after every generation.
`fd_census.py` writes `fd-census/*-guest.txt` and `*-host.json`; every 25
generations also samples the earliest live ancestor. Sampling runs outside the
VM pause interval. Keep RLIMIT_NOFILE unchanged, and distinguish per-VM growth
from the expected aggregate growth when retaining 100 live Pods.

The lifecycle fix spans the local containerd, ttrpc-rust and rust-extensions
patches: dedicated CRI IO clients close on remote EOF/error or IO cancellation,
while stdin retains send-side half-close semantics; disconnected inherited
vsock sockets are woken through the server's lifetime-protected connection
tracker; streaming handlers cancel with their request and return unsent output
for reconnection; runc pipes have one owner per FD and idempotent cleanup.
The tracker never closes a raw FD or consumes stream data. It uses nonblocking
MSG_PEEK because Linux vsock getpeername may still return an old address after
reset while recv returns ENOTCONN. Preserve both functional checks and the FD
curve when evaluating changes to this path.

For FD/lifecycle stress runs, `FORK_REPORT_EXEC_SLO=1` records any
`MAX_EXEC_SECONDS` misses in `results.json.exec_slo` separately from functional
success. The default remains a hard assertion. Do not discard slow samples or
call the latency SLO passed when its recorded result is FAIL; 100 simultaneously
active I/O workloads need their own performance distribution.
