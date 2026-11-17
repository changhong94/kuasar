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
