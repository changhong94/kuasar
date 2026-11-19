# Incremental memory checkpoints over Manifest/chunk

Remote checkpoints restored through the Manifest UFFD pager retain an immutable
memory baseline. Subsequent remote checkpoints export changed 4 KiB pages and a
range map rather than reading every configured RAM byte. A first checkpoint, or
an ordinary native/local snapshot without this baseline, still uses the existing
full capture path. `KUASAR_INCREMENTAL_MEMORY=0` disables the new path when the
sandboxer starts; configure it before creating/restoring the VM.

## Correctness and publication

* KVM logging starts before restored vCPUs run. VMM memory bitmaps preserve
  device writes, including writes made while restoring devices. Snapshotting
  collects both sources under the existing paused-VM/device/guest barriers.
* Consumed memory dirty bits accumulate for the lifetime of the baseline. They
  are serialized in the in-memory fork state, so a write before fork is retained
  by both children. Each branch then tracks its own writes.
* A failed capture or publication does not discard accumulated dirty pages.
  A successful checkpoint publishes durable chunk data and an authenticated root
  before committing the checkpoint directory and releasing guest RAM. Restoring
  that committed checkpoint establishes a new baseline and dirty epoch.
* `memory-ranges.delta` is a sparse temporary carrier. `memory-delta.json`
  enumerates writes, including zero writes. A hole **inside** a declared dirty
  range replaces old bytes with zero; holes outside those ranges inherit the
  baseline. Generic disk-hole fallthrough rules must not be used here.
* `compose-memory` reads baseline/delta metadata, not baseline RAM payload. It
  emits a flat `manifest-memory-graph` with raw Manifest references and explicit
  zero extents. The checkpoint root retains every referenced Manifest. Restore
  uses `serve-memory` to fetch bytes on demand through the ordinary chunk layer.
* Snapshot export does not perform background whole-RAM prefetch. No language
  runtime is prestarted or specially warmed.

RAM stays private and retains host-fork CoW semantics. Fixed private RAM is
required; ballooning, memory hotplug and passthrough configurations are excluded
by the restore capability check. The current live-fork thread-lifecycle path
still resolves outstanding UFFD pages before terminating the pager and doing
host fork. Thus **snapshot save is incremental, but live fork of a partially
faulted-in VM is not yet fully lazy**. Native export remains a full export.

## Diagnostics and interpretation

The memory file entry in `chunk_snapshot.files` records:

* `logical_bytes`: configured snapshot address-space length, not bytes fetched.
* `capture_read_bytes`: RAM bytes scanned by incremental capture.
* `capture_allocated_bytes`: nonzero bytes written to the sparse delta carrier.
* `stats.StoredBytes`: new memory delta/map bytes accepted by the store after
  compression and deduplication; this excludes other checkpoint files.

On disconnect, the pager emits `MANIFEST_READ requests=... bytes=...`. These are
logical bytes served to the VMM over the pager's entire lifetime. They are **not**
physical storage-device reads and do not isolate block-cache hits. In particular,
zero ranges can be synthesized without fetching chunks.

For a baseline that refuses snapshot while background prefetch is in progress,
measure from the first save request through the successful response, including
that required wait. Do not label this whole interval as uninterrupted guest
pause. Separately measure restore request to successful response, and the entire
restore / modify / save sequence. Retain warmup separately from measured runs.

## Validation

The integration workload uses a real Linux guest with 2 GiB RAM and a persistent
process holding 128 MiB of initialized nonzero pseudorandom memory. Each measured
cycle restores a chunk snapshot, writes four pages, then saves. Kernel and guest
agent activity also dirties pages and is included in measured bytes.

Correctness checks cover multiple incremental generations, nonzero-to-zero
replacement, writes on both sides of a live fork, writes preceding fork,
publication failure after dirty-log consumption followed by another write and
retry, and independent destruction. A guest `O_DIRECT` read from `/dev/vda` into
an existing process buffer validates virtio device writes across save/restore;
compare that buffer's SHA-256 before and after using `/proc/PID/mem`.

Manifest unit tests additionally reject overlapping/misaligned/out-of-bounds
ranges and use an unreadable two-GiB payload to prove composition only consults
metadata. Relevant commands:

```
# accelerator
 go test ./cmd/manifest-raw
# Cloud Hypervisor
 cargo test -p vmm --features kvm --lib sparse::
 cargo test -p vmm --features kvm --lib uffd::
# Kuasar
 cargo test -p vmm-sandboxer --lib checkpoint
```

## Adaptive background compaction and collection

Set `KUASAR_MEMORY_MAINTENANCE` to a private durable directory to enable
maintenance. By default, a memory map queues compaction at 8 distinct referenced
payload Manifests, 8192 extents, or 50% overlay coverage. Override these budgets
with `KUASAR_MEMORY_MAX_OBJECTS`, `KUASAR_MEMORY_MAX_EXTENTS`, and
`KUASAR_MEMORY_MAX_PERCENT`. These are trigger thresholds, not hard instantaneous
limits: foreground saves can continue while one background worker is busy.

Foreground capture **continues to export only dirty pages**, including when a
budget is exceeded. A background worker streams the immutable graph from Store,
preserves zero/hole metadata, and ingests a new raw Manifest baseline. It does not
access the VM's RAM or UFFD pager. After Store durability acknowledgement, it
atomically publishes and fsyncs the completed-baseline marker. Subsequent saves
use that baseline. A later generation can also adopt a completed ancestor:
comparing the two range maps produces the necessary overrides without reading
payload. This comparison handles divergent siblings and zero writes as well.

One worker runs per maintenance directory. Jobs on the same raw baseline are
coalesced. Queued jobs survive controller restarts; the controller restarts the
worker on startup. Failed compactions retry three times with bounded backoff,
then leave the durable job queued for the next maintenance invocation. Publication
failure never changes the existing baseline. `KUASAR_MEMORY_FAIL_ONCE=/private/path`
is an isolated-test fault: when that file exists, the worker removes it and fails
once after ingest but before publishing completion.

Collection is separately opt-in. It currently supports an **exclusive filesystem
checkpoint Store**, initialized empty; it does not delete from a shared Store or
an object-store backend. Before the first ingest, write an owner request containing
canonical absolute `store` and `state` directory paths, then run:

```
KUASAR_MEMORY_MAINTENANCE=/private/maintenance manifest-raw \
  --manifest-config /private/manifest.yaml memory-owner-init /private/owner-request.json
```

The configured Store must actually use that filesystem root. Every writer must
use the matching Kuasar/manifest-raw maintenance protocol. Configure the state
root to encompass every checkpoint/template owner. Publication outside the
registered state tree is rejected. Enabling this collector on an existing,
unregistered Store is refused rather than guessing which roots remain live.

Kuasar fsyncs `memory-roots.json` in each owner directory. It preserves both the
running VM's baseline and the published checkpoint, copies these references into
a forked child before it runs, and removes them only after VM destruction. GC
marks those roots, pending compactions, completed bases needed by live graphs,
and explicit retained roots under the maintenance directory. Operators exporting
snapshots outside the state tree must register their memory references before
removing the original owner; an unregistered copied Manifest key is not a GC pin.

Shared transaction locks cover publication, root transitions, fork and destroy.
The collector takes an exclusive lock and validates the entire reachability scan
before deleting any object. Corrupt roots abort collection. Memory-owned
Manifests and chunks can be reclaimed only when unreachable. Disk/config/state
Manifests are preserved; objects shared between memory and disk stay protected.
This is **memory-data GC**, not a general-purpose historical checkpoint metadata
collector. A live parent, retained snapshot or child can intentionally keep an
old base alive.

GC runs after compaction, controller startup and sandbox destruction. Completion
markers, root updates and their directories are fsynced before old data becomes
eligible. Removing unreachable immutable objects is idempotent across an
interrupted sweep. Administrative retry commands are:

```
manifest-raw --manifest-config /private/manifest.yaml maintain-memory run
manifest-raw --manifest-config /private/manifest.yaml collect-memory now
```

Use the same `KUASAR_MEMORY_MAINTENANCE` environment for these commands and the
sandboxer. `MEMORY_COMPACT` reports source logical data bytes (not physical disk
reads), newly stored bytes after compression/deduplication, and elapsed time.
`MEMORY_GC` reports removed object counts/bytes and elapsed time. Background CPU,
I/O and metadata-lock contention can increase foreground latency; measure both
rather than treating background work as free. There is no synchronous whole-RAM
fallback on the save path.

## Foreground/background cost controls

Sparse segments advertise their exact remaining length to the chunker. Its
input buffer is bounded by that length (with a one-byte minimum for EOF), instead
of allocating a dense-stream buffer for every isolated dirty page. Chunk sizes,
CDC boundaries, zero handling and independent callback payloads are unchanged.
This applies to both foreground ingestion and background compaction; it does not
read missing guest pages or change the snapshot format.

Background compaction uses two ingestion workers and at most 16 authenticated
Manifest readers. `KUASAR_MEMORY_MERGE_WORKERS` can set the background worker
count independently to 1–4; invalid values fail the job without publishing a
completion marker. Foreground pagers retain the four-reader bound.
A separate `io.lock` schedules background source reads: foreground Store
publications share the gate, and compaction checks for them before each read of
at most 64 KiB. The background releases the gate **before** fetching data, so a
foreground request never waits for a background Store RPC while acquiring it.
Already admitted reads and bounded queued writes may overlap a new publication;
this is cooperative scheduling, not an I/O exclusion or durability barrier.
`KUASAR_MEMORY_IO_GATE=0` disables this scheduling for diagnostics. Sustained
foreground traffic can delay compaction and reclamation; monitor queue progress
and storage peaks. Transaction/GC locking and all persistence barriers remain
independent and unchanged. Neither bounded concurrency nor scheduling defers
durability acknowledgements or drops GC roots. Completion time and peak storage
must still be measured: a slower worker can temporarily retain more old chunks.

Successful publication already installs a durable `published` memory root. The
controller no longer writes an identical `checkpoint` root a second time. On
publication, a legacy duplicate is removed in the same atomic registry update;
the active baseline and fork-inherited roots remain protected. File sync, atomic
rename and directory sync remain required. `KUASAR_MEMORY_TUNING=0` is a diagnostic
switch for the previous duplicate pin and background reader/worker limits; it
does not undo the chunker's bounded-buffer optimization.

Set `RUST_LOG=checkpoint_profile=info` to log `CHECKPOINT_STAGE` durations from
transaction-lock acquisition through RAM release, and `CHECKPOINT_STORE` CLI
publication durations. Stage values are individual elapsed intervals, not
cumulative timestamps. Store command `maintenance.sync_us` counts the total
metadata atomic-publication operation, including writes, rename and sync; it is
**not** a pure kernel fsync measurement. `MEMORY_COMPACT.metadata_opens` counts
actual reader opens. The same record includes `workers`, `lock_us`, `ingest_us`,
`source_read_us`, `commit_us`, and `io_wait_us`. The last measures background
waiting at the scheduling gate, including lock syscall overhead. `source_read_us` is time inside source reads
(including fetch/decrypt/decompress), a subset of ingestion; reads and writer
workers overlap, so these fields must not be added together. `commit_us` covers
root ownership and completion/catalog publication, while chunk durability remains
inside `ingest_us`. Client request timing remains the authoritative save cost.

## References

* [Firecracker snapshot support](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md): private memory mappings and differential snapshots with dirty tracking.
* [SnapFaaS](https://arxiv.org/html/2109.13319): shared base snapshots with per-function differences. The implementation here adds Manifest/chunk publication and existing live-fork lifecycle requirements; it is not a direct port.
