# SPDX-License-Identifier: Apache-2.0
"""SQLite is authoritative; JSON is an atomic compatibility view rebuilt on startup.
Keep the .sqlite3 and its journal/WAL sidecars when moving the state. PERSIST
is the default rollback journal. E2B_SQLITE_JOURNAL=WAL opts into WAL; both modes
retain synchronous=EXTRA, including a durable commit before acknowledgment.
The JSON projection alone is not a backup of the authoritative database.
Records are stored per sandbox to avoid rewriting the entire pool on one lease.
The old single-row schema is migrated atomically; state is now a read-only view.
"""
import json
import os
from pathlib import Path
import sqlite3
import time
import threading
import uuid


class DurableState:
    def __init__(self, path):
        if sqlite3.sqlite_version_info < (3, 38, 0):
            raise RuntimeError("DurableState requires SQLite 3.38+ JSON operators")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(str(self.path) + '.sqlite3')
        existed = self.db_path.exists()
        if self.db_path.is_symlink() and not existed:
            raise RuntimeError("Authoritative SQLite target is unavailable")
        control_root = os.environ.get('E2B_DURABLE_SQLITE_ROOT')
        if control_root and not os.path.lexists(self.db_path):
            # Each state generation gets its own target. Archiving the logical
            # state directory preserves the link; the next generation cannot
            # overwrite its database. Existing databases are never relocated.
            root = Path(control_root).resolve(strict=True)
            if not root.is_dir():
                raise ValueError('SQLite control root must be a persistent directory')
            target = root / (uuid.uuid4().hex + '.sqlite3')
            fd = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            os.symlink(target, self.db_path)
        # Use the canonical path so SQLite's WAL/SHM are colocated with the DB.
        # The durable link remains authoritative even if the option is unset.
        self.db_path = self.db_path.resolve(strict=False)
        self.db = sqlite3.connect(self.db_path, isolation_level=None,
                                  check_same_thread=False, timeout=30)
        os.chmod(self.db_path, 0o600)
        mode = os.environ.get('E2B_SQLITE_JOURNAL', 'PERSIST').upper()
        if mode not in ('PERSIST', 'WAL'):
            raise ValueError('Unsupported durable journal mode')
        actual = self.db.execute('PRAGMA journal_mode='+mode).fetchone()[0]
        if actual.upper() != mode:
            raise RuntimeError('Requested durable journal mode unavailable')
        self.db.execute('PRAGMA synchronous=EXTRA')
        self.db.execute('PRAGMA locking_mode=EXCLUSIVE')
        kind = self.db.execute("SELECT type FROM sqlite_master WHERE name='state'").fetchone()
        if kind is None or kind[0] == 'table':
            row = self.db.execute('SELECT value FROM state WHERE id=1').fetchone() if kind else None
            initial = json.loads(row[0]) if row else (json.loads(self.path.read_text()) if self.path.exists() else {})
            # Migrate in one durable transaction. The compatibility view remains
            # readable by old tooling, but rejects old whole-document writes.
            self.db.execute('BEGIN IMMEDIATE')
            try:
                self.db.execute('CREATE TABLE state_records (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
                self.db.executemany('INSERT INTO state_records VALUES(?, ?)',
                                    [(k, json.dumps(v, separators=(',', ':'))) for k, v in initial.items()])
                if kind:
                    self.db.execute('DROP TABLE state')
                self.db.execute("CREATE VIEW state AS SELECT 1 AS id, json_group_object(id,json(value)) AS value FROM state_records")
                self.db.execute('COMMIT')
            except BaseException:
                if self.db.in_transaction:
                    self.db.execute('ROLLBACK')
                raise
        if not existed:
            for parent in {self.path.parent, self.db_path.parent}:
                fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        self.project(self.load())

    def load(self):
        row = self.db.execute('SELECT value FROM state WHERE id=1').fetchone()
        if row is None:
            raise RuntimeError('durable state has no committed record')
        return json.loads(row[0])

    def save(self, records, *, only=None):
        started_ns = time.perf_counter_ns()
        if not isinstance(records, dict) or any(not isinstance(k, str) for k in records):
            raise ValueError('Durable state must map string sandbox IDs to records')
        selected = records if only is None else {sid: records[sid] for sid in only}
        encoded = json.dumps(selected, separators=(',', ':'))
        serialized_ns = time.perf_counter_ns()
        self.db.execute('BEGIN IMMEDIATE')
        begun_ns = time.perf_counter_ns()
        try:
            # Parse the one serialized document in SQLite, then write only
            # changed records. json_each already provides object/array JSON;
            # avoid rescanning the whole document for every sandbox. -> is
            # needed only for scalar records to retain types/large integer text.
            before = self.db.total_changes
            self.db.execute("""INSERT INTO state_records(id,value)
                SELECT key, CASE WHEN type IN ('object','array') THEN value
                    ELSE ? -> fullkey END FROM json_each(?) WHERE true
                ON CONFLICT(id) DO UPDATE SET value=excluded.value
                WHERE state_records.value != excluded.value""", (encoded, encoded))
            changed_count = self.db.total_changes - before
            before = self.db.total_changes
            if only is None:
                self.db.execute('DELETE FROM state_records WHERE id NOT IN (SELECT key FROM json_each(?))', (encoded,))
            removed_count = self.db.total_changes - before
            updated_ns = time.perf_counter_ns()
            self.db.execute('COMMIT')
        except BaseException:
            if self.db.in_transaction:
                self.db.execute('ROLLBACK')
            raise
        # The durable transaction must finish before acknowledgment. Loss of this
        # derived view on crash is repaired before serving the next request.
        committed_ns = time.perf_counter_ns()
        if only is not None:
            # The projection comes from committed rows, not other in-memory
            # records that a caller may still be modifying. No Python cache.
            encoded = self.db.execute("SELECT value FROM state WHERE id=1").fetchone()[0]
        self.project(encoded)
        if os.environ.get('E2B_TRACE_TIMING'):
            ended_ns = time.perf_counter_ns()
            print('E2B_TIMING '+json.dumps(dict(
                stage='durable_save', started_ns=started_ns, ended_ns=ended_ns,
                elapsed_ms=(ended_ns-started_ns)/1e6,
                commit_ms=(committed_ns-started_ns)/1e6,
                serialize_ms=(serialized_ns-started_ns)/1e6,
                begin_ms=(begun_ns-serialized_ns)/1e6,
                update_ms=(updated_ns-begun_ns)/1e6,
                barrier_ms=(committed_ns-updated_ns)/1e6,
                projection_ms=(ended_ns-committed_ns)/1e6,
                thread=threading.get_native_id(),
                record_count=len(records), changed_records=changed_count, partial=only is not None,
                removed_records=removed_count, encoded_bytes=len(encoded.encode()))), flush=True)

    def project(self, records):
        tmp = self.path.with_suffix('.tmp')
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as f:
            # save() reuses the exact committed JSON; startup may pass a mapping.
            f.write(records if isinstance(records, str) else json.dumps(records, separators=(',', ':')))
        os.replace(tmp, self.path)

    def close(self):
        self.db.close()
