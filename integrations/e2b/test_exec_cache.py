# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import exec_cache


class ExecCacheTests(unittest.TestCase):
    def test_failed_preparation_does_not_publish_success(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory)/'report.json'
            with patch.object(exec_cache.vm_rpc, 'execute', return_value={
                    'exit_code': 1, 'stdout': b''}):
                with self.assertRaises(RuntimeError):
                    exec_cache.prepare('/unused', report)
            self.assertFalse(report.exists())

    def test_reports_only_completed_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory)/'report.json'
            with patch.object(exec_cache.vm_rpc, 'execute', return_value={
                    'exit_code': 0, 'stdout': b'/bin/bash\n/etc/profile\n'}):
                result = exec_cache.prepare('/unused', report)
            self.assertEqual(result['files'], ['/bin/bash', '/etc/profile'])
            self.assertFalse(result['user_command_executed'])
            self.assertTrue(report.exists())


class ImageStartupCacheTests(unittest.TestCase):
    def test_rejects_unbounded_or_escaping_paths(self):
        for files in ['python', ['/a/../b'], ['/x\ncommand'], ['relative'], ['/x']*33]:
            with self.assertRaises(ValueError):
                exec_cache.validate_startup_files(files)

    def test_empty_hints_do_not_contact_guest(self):
        with patch.object(exec_cache.vm_rpc, 'execute') as execute:
            self.assertEqual(exec_cache.prepare_image('/socket', '/root', [])['bytes'], 0)
        execute.assert_not_called()

    def test_reads_paths_as_arguments_without_launching_interpreter(self):
        files = ['/usr/local/bin/python3', '/lib/data with spaces']
        with patch.object(exec_cache.vm_rpc, 'execute', return_value={
                'exit_code': 0, 'stdout': b'10\t/usr/local/bin/python3\n20\t/lib/data with spaces\n'}) as execute:
            result = exec_cache.prepare_image('/socket', '/run/image-root', files)
        self.assertEqual(result['bytes'], 30)
        self.assertFalse(result['application_executed'])
        argv = execute.call_args.args[1]
        self.assertEqual(argv[:5], ['/bin/busybox', 'chroot', '/run/image-root', '/.__kuasar/busybox', 'sh'])
        self.assertEqual(argv[-2:], files)

    def test_failed_read_never_reports_success(self):
        with patch.object(exec_cache.vm_rpc, 'execute', return_value={'exit_code': 1, 'stdout': b''}):
            with self.assertRaises(RuntimeError):
                exec_cache.prepare_image('/socket', '/root', ['/file'])
