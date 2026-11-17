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
