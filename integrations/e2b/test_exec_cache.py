# SPDX-License-Identifier: Apache-2.0
import unittest
from unittest.mock import patch
from pathlib import Path
import exec_cache

class MetadataTests(unittest.TestCase):
    def test_no_regular_file_reads(self):
        with patch('exec_cache.vm_rpc.execute', return_value={'exit_code':0,'stdout':b'{"regular_file_bytes_read":0}'}) as rpc:
            report=exec_cache.prepare_metadata('/socket','/root')
        self.assertEqual(report['regular_file_bytes_read'],0)
        rpc.assert_called_once_with(Path('/socket'), ['/usr/bin/image-metadata','/root','0'])
    def test_preparation_failure_is_not_ignored(self):
        with patch('exec_cache.vm_rpc.execute',return_value={'exit_code':1}):
            with self.assertRaises(RuntimeError):exec_cache.prepare_metadata('/socket','/root')

    def test_uniform_budget_is_forwarded(self):
        with patch('exec_cache.vm_rpc.execute', return_value={'exit_code':0,'stdout':b'{"regular_file_bytes_read":128,"application_executed":false}'}) as rpc:
            report=exec_cache.prepare_metadata('/socket','/root',128)
        self.assertFalse(report['application_executed'])
        rpc.assert_called_once_with(Path('/socket'), ['/usr/bin/image-metadata','/root','128'])

    def test_budget_is_bounded_before_guest_rpc(self):
        for value in [-1, exec_cache.MAX_READ_BYTES+1, '128']:
            with self.subTest(value=value), patch('exec_cache.vm_rpc.execute') as rpc:
                with self.assertRaises(ValueError):
                    exec_cache.prepare_metadata('/socket','/root',value)
                rpc.assert_not_called()
