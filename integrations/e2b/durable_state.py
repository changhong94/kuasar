# SPDX-License-Identifier: Apache-2.0
"""SQLite is authoritative; JSON is an atomic compatibility view rebuilt on startup.
Keep the .sqlite3 and -journal files when moving the state. PERSIST reuses the
rollback journal; EXTRA retains SQLite's strongest synchronous commit setting.
"""
import json
import os
from pathlib import Path
import sqlite3
import time
import threading


class DurableState:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(str(self.path) + '.sqlite3')
        existed = self.db_path.exists()
        self.db = sqlite3.connect(self.db_path, isolation_level=None,
                                  check_same_thread=False, timeout=30)
        os.chmod(self.db_path, 0o600)
        self.db.execute('PRAGMA journal_mode=PERSIST')
        self.db.execute('PRAGMA synchronous=EXTRA')
        self.db.execute('PRAGMA locking_mode=EXCLUSIVE')
        self.db.execute('CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)')
        row = self.db.execute('SELECT value FROM state WHERE id=1').fetchone()
        if row is None:
            self.save(json.loads(self.path.read_text()) if self.path.exists() else {})
        if not existed:
            fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
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

    def save(self, records):
        started_ns = time.perf_counter_ns()
        encoded = json.dumps(records, separators=(',', ':'))
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.db.execute('INSERT INTO state VALUES(1, ?) ON CONFLICT(id) DO UPDATE SET value=excluded.value', (encoded,))
            self.db.execute('COMMIT')
        except BaseException:
            if self.db.in_transaction:
                self.db.execute('ROLLBACK')
            raise
        # The durable transaction must finish before acknowledgment. Loss of this
        # derived view on crash is repaired before serving the next request.
        committed_ns = time.perf_counter_ns()
        self.project(encoded)
        if os.environ.get('E2B_TRACE_TIMING'):
            ended_ns = time.perf_counter_ns()
            print('E2B_TIMING '+json.dumps(dict(
                stage='durable_save', started_ns=started_ns, ended_ns=ended_ns,
                elapsed_ms=(ended_ns-started_ns)/1e6,
                commit_ms=(committed_ns-started_ns)/1e6,
                projection_ms=(ended_ns-committed_ns)/1e6,
                thread=threading.get_native_id(),
                record_count=len(records))), flush=True)

    def project(self, records):
        tmp = self.path.with_suffix('.tmp')
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as f:
            # save() reuses the exact committed JSON; startup may pass a mapping.
            f.write(records if isinstance(records, str) else json.dumps(records, separators=(',', ':')))
        os.replace(tmp, self.path)

    def close(self):
        self.db.close()
