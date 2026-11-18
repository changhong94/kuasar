# SPDX-License-Identifier: Apache-2.0
"""Exercise the deployed native metadata helper against a private fixture tree."""
import json,os,subprocess,tempfile,unittest
from pathlib import Path

@unittest.skipUnless(os.environ.get('IMAGE_METADATA_TEST_BINARY'),
                     'requires native IMAGE_METADATA_TEST_BINARY')
class UniformCacheTests(unittest.TestCase):
    def test_budget_and_file_type_boundaries_without_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            outer=Path(directory);root=outer/'image';root.mkdir()
            (root/'data').write_bytes(b'x'*4096)
            (root/'nested').mkdir();(root/'nested/data').write_bytes(b'y'*4096)
            outside=outer/'outside';outside.write_bytes(b'z'*16384)
            (root/'symlink').symlink_to(outside);os.mkfifo(root/'fifo')
            marker=outer/'executed'
            program=('#!/bin/sh\ntouch '+str(marker)+'\n').encode()
            app=root/'application';app.write_bytes(program);app.chmod(0o755)
            for budget,expected in [(0,0),(128,128),(1048576,8192+len(program))]:
                with self.subTest(budget=budget):
                    run=subprocess.run([os.environ['IMAGE_METADATA_TEST_BINARY'],str(root),str(budget)],
                                       check=True,capture_output=True,text=True,timeout=5)
                    report=json.loads(run.stdout)
                    self.assertEqual(report['regular_file_bytes_read'],expected)
                    self.assertFalse(report['application_executed'])
                    self.assertFalse(marker.exists())
                    self.assertEqual(report['errors'],0)
    def test_invalid_budgets_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            for budget in ['-1','536870913','garbage']:
                with self.subTest(budget=budget):
                    run=subprocess.run([os.environ['IMAGE_METADATA_TEST_BINARY'],root,budget],
                                       capture_output=True,timeout=5)
                    self.assertNotEqual(run.returncode,0)

if __name__=='__main__':unittest.main()
