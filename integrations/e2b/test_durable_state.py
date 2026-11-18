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
    def test_projection_matches_committed_records(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state.json'
            store = DurableState(p)
            records = {'slot': {'metadata': {'unicode': '测试', 'line': 'a\nb'}, 'token': 'new'}}
            store.save(records)
            committed = store.db.execute('SELECT value FROM state WHERE id=1').fetchone()[0]
            self.assertEqual(json.loads(p.read_text()), json.loads(committed))
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
            code = "from durable_state import DurableState; import os,signal,sys; s=DurableState(sys.argv[1]); s.db.execute('PRAGMA cache_size=5'); s.db.execute('BEGIN IMMEDIATE'); s.db.execute('UPDATE state_records SET value=?', ('x'*2097152,)); os.kill(os.getpid(),signal.SIGKILL)"
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

    def test_updates_only_changed_slots_and_removes_deleted_slots(self):
        with tempfile.TemporaryDirectory() as d:
            store = DurableState(Path(d)/'state')
            records = {str(i): {'status': 'ready', 'nested': {'value': i}} for i in range(64)}
            store.save(records)
            before = store.db.total_changes
            records['3']['nested']['value'] = 999
            store.save(records)
            self.assertEqual(store.db.total_changes - before, 1)
            before = store.db.total_changes
            store.save(records)
            self.assertEqual(store.db.total_changes, before)
            del records['4']
            store.save(records)
            self.assertEqual(store.load(), records)
            # Legacy readers see current data; legacy writers cannot corrupt it.
            self.assertEqual(json.loads(store.db.execute('SELECT value FROM state WHERE id=1').fetchone()[0]), records)
            with self.assertRaises(Exception):
                store.db.execute("UPDATE state SET value='{}'")
            store.close()
            store = DurableState(Path(d)/'state')
            self.assertEqual(store.load(), records)
            store.close()

    def test_json_types_and_escaped_ids_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            store = DurableState(Path(d)/'state')
            records = {'a.b': True, 'quote"': False, 'nil': None, 'string': '测试',
                       'huge': 123456789012345678901234567890,
                       'array': [1, False, None, {'n': 2}], 'obj': {'s': 'a\nb'}}
            store.save(records)
            self.assertEqual(store.load(), records)
            before = store.db.total_changes
            store.save(records)
            self.assertEqual(store.db.total_changes, before)
            store.close()

    def test_separate_sqlite_root_keeps_generation_and_reopens_without_option(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)/'control'; root.mkdir()
            p = Path(d)/'state'
            with patch.dict(os.environ, {'E2B_DURABLE_SQLITE_ROOT': str(root)}):
                store = DurableState(p); store.save({'old': 'running'}); store.close()
            logical = Path(str(p)+'.sqlite3')
            self.assertTrue(logical.is_symlink())
            old_target = logical.resolve()
            store = DurableState(p)
            self.assertEqual(store.load(), {'old': 'running'}); store.close()
            archive = Path(d)/'old-state.sqlite3'
            logical.rename(archive); p.unlink()
            with patch.dict(os.environ, {'E2B_DURABLE_SQLITE_ROOT': str(root)}):
                store = DurableState(p); store.save({'new': 'running'}); store.close()
            self.assertNotEqual(logical.resolve(), old_target)
            import sqlite3
            db = sqlite3.connect(archive)
            self.assertEqual(json.loads(db.execute('SELECT value FROM state').fetchone()[0]), {'old':'running'})
            db.close()

    def test_missing_external_database_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state'; p.write_text('{"stale": 1}')
            Path(str(p)+'.sqlite3').symlink_to(Path(d)/'missing')
            with self.assertRaisesRegex(RuntimeError, 'unavailable'):
                DurableState(p)
            self.assertFalse((Path(d)/'missing').exists())

    def test_partial_commit_preserves_other_committed_records(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state'; store = DurableState(p)
            records = {'parent': {'n': 1}, 'child': {'n': 2}}
            store.save(records)
            records['parent']['n'] = 999  # not part of the committed change
            records['child']['n'] = 3
            store.save(records, only=('child',))
            expected = {'parent': {'n': 1}, 'child': {'n': 3}}
            self.assertEqual(store.load(), expected)
            self.assertEqual(json.loads(p.read_text()), expected)
            store.db.execute('PRAGMA query_only=ON')
            records['child']['n'] = 4
            with self.assertRaises(Exception): store.save(records, only=('child',))
            self.assertEqual(store.load(), expected)
            self.assertEqual(json.loads(p.read_text()), expected)
            store.close()

    def test_partial_commit_survives_kill_before_projection(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state'; store = DurableState(p)
            store.save({'parent': 'running'}); store.close()
            code = "from durable_state import DurableState; import os,signal,sys; s=DurableState(sys.argv[1]); s.project=lambda value: os.kill(os.getpid(),signal.SIGKILL); s.save({'child':'creating'},only=('child',))"
            done = subprocess.run([sys.executable, '-c', code, str(p)], cwd=Path(__file__).parent)
            self.assertEqual(done.returncode, -signal.SIGKILL)
            store = DurableState(p)
            self.assertEqual(store.load(), {'parent':'running','child':'creating'})
            self.assertEqual(json.loads(p.read_text()), store.load()); store.close()

    def test_legacy_sqlite_migration_survives_crash(self):
        import sqlite3
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state'
            db = sqlite3.connect(str(p)+'.sqlite3')
            db.execute('CREATE TABLE state (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)')
            original = {'parent': {'state': 'running'}, 'child': {'state': 'creating'}}
            db.execute('INSERT INTO state VALUES(1, ?)', (json.dumps(original),))
            db.commit(); db.close()
            # Kill inside migration, after copying rows but before replacing the
            # old schema. SQLite must recover the entire original transaction.
            code = """import sqlite3,os,signal,sys
from durable_state import DurableState
connect=sqlite3.connect
def traced(*a,**kw):
 c=connect(*a,**kw)
 c.set_trace_callback(lambda sql: os.kill(os.getpid(),signal.SIGKILL) if sql.startswith('DROP TABLE state') else None)
 return c
sqlite3.connect=traced
DurableState(sys.argv[1])
"""
            done = subprocess.run([sys.executable, '-c', code, str(p)], cwd=Path(__file__).parent)
            self.assertEqual(done.returncode, -signal.SIGKILL)
            store = DurableState(p)
            self.assertEqual(store.load(), original)
            store.save({'child': {'state': 'running'}})
            store.close()
            store = DurableState(p)
            self.assertEqual(store.load(), {'child': {'state': 'running'}})
            store.close()


class WALDurableStateTests(unittest.TestCase):
    def test_wal_keeps_extra_and_recovers_committed_and_uncommitted_crashes(self):
        from unittest.mock import patch
        with patch.dict(os.environ, {'E2B_SQLITE_JOURNAL': 'WAL'}):
            with tempfile.TemporaryDirectory() as d:
                store=DurableState(Path(d)/'state')
                self.assertEqual(store.db.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
                self.assertEqual(store.db.execute('PRAGMA synchronous').fetchone()[0], 3)
                store.close()
            cases=DurableStateTests()
            cases.test_kill_after_commit_before_json_projection()
            cases.test_kill_uncommitted_transaction_rolls_back()
            cases.test_failed_commit_does_not_publish_json()
            cases.test_legacy_sqlite_migration_survives_crash()


class ExternalWALCrashTests(unittest.TestCase):
    def test_external_wal_crash_recovery(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, {'E2B_SQLITE_JOURNAL':'WAL', 'E2B_DURABLE_SQLITE_ROOT':root}):
                cases = DurableStateTests()
                cases.test_kill_after_commit_before_json_projection()
                cases.test_kill_uncommitted_transaction_rolls_back()
                cases.test_failed_commit_does_not_publish_json()
                cases.test_partial_commit_survives_kill_before_projection()


class BackgroundWALTests(unittest.TestCase):
    def test_background_preserves_crash_durability(self):
        from unittest.mock import patch
        with patch.dict(os.environ, {'E2B_SQLITE_CHECKPOINT': 'background'}):
            WALDurableStateTests().test_wal_keeps_extra_and_recovers_committed_and_uncommitted_crashes()
            ExternalWALCrashTests().test_external_wal_crash_recovery()

    def test_checkpoint_debt_blocks_until_reader_releases_and_thread_closes(self):
        import sqlite3
        import threading
        import time
        from unittest.mock import patch
        with patch.dict(os.environ, {'E2B_SQLITE_JOURNAL':'WAL', 'E2B_SQLITE_CHECKPOINT':'background'}):
            with tempfile.TemporaryDirectory() as d:
                p=Path(d)/'state';store=DurableState(p)
                self.assertEqual(store.db.execute('PRAGMA wal_autocheckpoint').fetchone()[0],0)
                self.assertEqual(store.db.execute('PRAGMA synchronous').fetchone()[0],3)
                store.save({'generation':0})
                reader=sqlite3.connect(store.db_path, isolation_level=None)
                reader.execute('BEGIN');reader.execute('SELECT * FROM state_records').fetchall()
                cp=store.checkpointer;cp.MAX_PENDING=2;cp.WAKE_COMMITS=1
                store.save({'generation':1})
                end=time.monotonic()+5
                while cp.last_result is None:
                    self.assertLess(time.monotonic(),end);time.sleep(.01)
                passed=threading.Event();errors=[]
                def waiter():
                    try:cp.before_commit();passed.set()
                    except Exception as e:errors.append(e)
                t=threading.Thread(target=waiter);t.start()
                self.assertFalse(passed.wait(.1))
                reader.execute('COMMIT');reader.close();cp.wake.set()
                t.join(5);self.assertFalse(t.is_alive());self.assertFalse(errors);self.assertTrue(passed.is_set())
                store.save({'generation':2});store.close()
                self.assertFalse(cp.worker.is_alive())
                reopened=DurableState(p);self.assertEqual(reopened.load(),{'generation':2});reopened.close()

    def test_background_retains_single_adapter_ownership(self):
        from unittest.mock import patch
        with patch.dict(os.environ, {'E2B_SQLITE_JOURNAL':'WAL', 'E2B_SQLITE_CHECKPOINT':'background'}):
            with tempfile.TemporaryDirectory() as d:
                p=Path(d)/'state';store=DurableState(p)
                with self.assertRaises(BlockingIOError):DurableState(p)
                self.assertFalse(os.get_inheritable(store.owner.fileno()))
                store.close()
                replacement=DurableState(p);replacement.close()

    def test_worker_failure_rejects_future_commits(self):
        from unittest.mock import patch
        import time
        from durable_state import WALCheckpointer
        with patch('durable_state.sqlite3.connect',side_effect=OSError('injected storage failure')):
            cp=WALCheckpointer('/not-opened')
            cp.worker.join(5)
        with self.assertRaisesRegex(RuntimeError,'checkpoint failed'):cp.before_commit()
        with self.assertRaisesRegex(RuntimeError,'checkpoint failed'):cp.close()
