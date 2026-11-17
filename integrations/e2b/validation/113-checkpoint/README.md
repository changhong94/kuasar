# 113 durable checkpoint validation

Host fixture: `hong@10.213.17.113`, namespace `py113-fork`, SDK virtualenv
`/home/hong/deploy/comparison/venv`. These scripts intentionally address that
isolated deployment; review paths before reuse. They read its key file without
printing credentials and record every test-owned sandbox for cleanup.

Source work area: `/home/hong/src/research/fork-features-113`. Run scripts as root
inside `nsenter --net=/run/netns/py113-fork` using the SDK virtualenv Python.
The crash test deliberately SIGKILLs **only its own child VMM**, after checking
its PID, executable and disk FD. It proves guest fsync survives a VMM crash,
not a host power failure. Do not run it against shared production workloads.

- `test_checkpoint_storage_guest.py`: live Python process/secret continuation,
  three resume cycles, CoW file isolation, parent deletion while child paused,
  actual guest RAM mapping release, and deletion of paused children.
- `test_checkpoint_languages.py`: Node/Ruby real execution, parent and child
  pause/resume, independent disk contents and independent destruction.
- `test_disk_crash.py`: fsync file + parent directory, owned VMM crash,
  independent reflink export, e2fsck and recovered file digest, live parent.
- `test_checkpoint_metrics.py`: five sequential lifecycle measurements,
  fresh Python interpreter/print receipt on resume, deterministic 32 MiB IO.
- `storage-tools/retained.py`: original report paired storage workload adapted
  to the newly supported pause/resume API; same/different bytes, three rounds
  each. It takes the existing benchmark lock and runs fork only. FIEMAP includes
  the product's data and control filesystems and deduplicates shared extents.

This is a new feature-validation batch with pool capacity 2, not report batch A
(pool 64). Existing benchmark/report artifacts were not overwritten. Startup
numbers here are not a new six-product ranking. No language-specific warming
was added. Immutable image digests and deployed binary hashes are in summary.

The 62 targeted adapter tests pass. The full suite has 104 tests, 7 skips and
one SQLite 3.42 escaped-field failure; the same failure is reproduced from the
unmodified base revision (`baseline-sqlite-test.log`). Do not claim a fully
passing suite. Host reboot/controller-loss recovery remains unsupported.
