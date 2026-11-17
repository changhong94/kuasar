# SPDX-License-Identifier: Apache-2.0
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from durable_state import DurableState


class DurableStateTests(unittest.TestCase):
    def test_projection_reuses_exact_committed_serialization(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state.json'
            store = DurableState(p)
            records = {'slot': {'metadata': {'unicode': '测试', 'line': 'a\nb'}, 'token': 'new'}}
            with patch('durable_state.json.dumps', wraps=json.dumps) as dumps:
                store.save(records)
                self.assertEqual(dumps.call_count, 1)
            committed = store.db.execute('SELECT value FROM state WHERE id=1').fetchone()[0]
            self.assertEqual(p.read_text(), committed)
            self.assertEqual(store.load(), records)
            store.close()

    def test_migration_and_stale_projection(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state.json'
            p.write_text('{"old": {"status": "running"}}')
            store = DurableState(p)
            self.assertEqual(store.db.execute('PRAGMA synchronous').fetchone()[0], 3)
            self.assertEqual(store.db.execute('PRAGMA journal_mode').fetchone()[0], 'persist')
            self.assertIn('old', store.load())
            store.save({'new': {'status': 'deleting'}})
            store.close()
            p.write_text('{"stale": 1}')
            store = DurableState(p)
            self.assertEqual(store.load(), {'new': {'status': 'deleting'}})
            self.assertEqual(json.loads(p.read_text()), store.load())
            store.close()

    def test_kill_after_commit_before_json_projection(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state.json'
            store = DurableState(p)
            store.save({'parent': 'running'})
            store.close()
            code = "from durable_state import DurableState; import os,signal,sys; s=DurableState(sys.argv[1]); s.project=lambda value: os.kill(os.getpid(),signal.SIGKILL); s.save({'parent':'running','child':'creating'})"
            done = subprocess.run([sys.executable, '-c', code, str(p)], cwd=Path(__file__).parent)
            self.assertEqual(done.returncode, -signal.SIGKILL)
            self.assertEqual(json.loads(p.read_text()), {'parent': 'running'})
            store = DurableState(p)
            self.assertEqual(store.load(), {'parent':'running','child':'creating'})
            self.assertEqual(json.loads(p.read_text()), store.load())
            store.close()

    def test_kill_uncommitted_transaction_rolls_back(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state.json'
            store = DurableState(p)
            store.save({'parent':'running'})
            store.close()
            code = "from durable_state import DurableState; import os,signal,sys; s=DurableState(sys.argv[1]); s.db.execute('PRAGMA cache_size=5'); s.db.execute('BEGIN IMMEDIATE'); s.db.execute('UPDATE state SET value=?', ('x'*2097152,)); os.kill(os.getpid(),signal.SIGKILL)"
            done = subprocess.run([sys.executable, '-c', code, str(p)], cwd=Path(__file__).parent)
            self.assertEqual(done.returncode, -signal.SIGKILL)
            store = DurableState(p)
            self.assertEqual(store.load(), {'parent':'running'})
            store.close()

    def test_failed_commit_does_not_publish_json(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state.json'
            store = DurableState(p)
            store.save({'parent':'running'})
            store.db.execute('PRAGMA query_only=ON')
            with self.assertRaises(Exception):
                store.save({'wrong':'state'})
            self.assertEqual(json.loads(p.read_text()), {'parent':'running'})
            self.assertEqual(store.load(), {'parent':'running'})
            store.close()
