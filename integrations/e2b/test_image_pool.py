# SPDX-License-Identifier: Apache-2.0
import hashlib
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
from server import APIError
from image_pool import ImagePoolBackend
from test_template_pool import FakeBackend
from template_pool import TemplatePool


class FakeImages(ImagePoolBackend, FakeBackend):
    def __init__(self, state):
        FakeBackend.__init__(self, state)
        self.operations = {}
        self.fixed_template = None
        self.image_catalog = {}
        self.image_stats = {}
        self.calls = []
        for name in ('base','second'):
            p=state.parent/(name+'.disk');p.write_bytes(name.encode());p.chmod(0o444)
            st=p.stat();self.image_stats[name]=(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns)
            self.image_catalog[name]=dict(disk=str(p),sha256=hashlib.sha256(name.encode()).hexdigest(),manifest='sha256:'+name)
    def admin(self, action, **kw):
        self.calls.append((action,kw))
        return dict(live_fork=dict(api_socket='test-api',vsock_socket='test-vsock'))


class ImagePoolTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.backend=FakeImages(Path(self.tmp.name)/'state')
        self.pool=TemplatePool(self.backend,2,refill_delay=0)
        self.backend.template_pool=self.pool
    def tearDown(self):
        self.pool.close();self.tmp.cleanup()
    def test_one_pool_binds_two_images_only_after_claim(self):
        self.assertTrue(all('template_id' not in r for r in self.backend.records.values()))
        attached=[]
        def attach(socket,path):
            attached.append(path)
            binding=[sid for sid,r in self.backend.records.items() if r.get('pool_state')=='binding']
            self.assertEqual(len(binding),1)
            with self.assertRaises(APIError):self.backend.get_public(binding[0])
        with patch('image_pool.add_image_disk',side_effect=attach),patch('image_pool.vm_rpc.execute',return_value={'exit_code':0}):
            a=self.backend.create({'templateID':'base'})
            b=self.backend.create({'templateID':'second'})
        self.assertNotEqual(a['sandboxID'],b['sandboxID'])
        self.assertEqual(a['template_id'],'base');self.assertEqual(b['template_id'],'second')
        self.assertEqual(self.backend.created,2)
        self.assertEqual(self.pool.status()['assigned'],2)
        self.assertEqual(set(attached),{x['disk'] for x in self.backend.image_catalog.values()})
        for sid in [a['sandboxID'],b['sandboxID']]:
            self.assertEqual(self.backend.committed[sid]['exec_root'],'/run/image-root')
    def test_failed_attachment_is_not_published_or_reused(self):
        ready=set(self.backend.records)
        with patch('image_pool.add_image_disk',side_effect=RuntimeError('attach failed')):
            with self.assertRaises(RuntimeError):self.backend.create({})
        self.assertTrue(any(sid not in self.backend.records for sid in ready))
        self.assertFalse(any(r.get('pool_state')=='leased' for r in self.backend.records.values()))
    def test_changed_or_unknown_image_does_not_consume_slot(self):
        with self.assertRaises(APIError):self.backend.create({'templateID':'unknown'})
        p=Path(self.backend.image_catalog['base']['disk']);p.chmod(0o644);p.write_bytes(b'changed')
        with self.assertRaises(APIError):self.backend.create({})
        self.assertEqual(self.pool.status()['ready'],2)
    def test_live_parent_rejects_switching_image(self):
        with patch('image_pool.add_image_disk'),patch('image_pool.vm_rpc.execute',return_value={'exit_code':0}):
            parent=self.backend.create({'templateID':'second'})['sandboxID']
        with self.assertRaises(APIError):self.backend.create({'templateID':'base'},parent)
        ready=self.pool.status()['ready']
        self.backend.create({},parent)
        self.assertEqual(self.pool.status()['ready'],ready)
        self.assertEqual(self.backend.parents[-1],parent)

    def test_failed_publish_commit_destroys_bound_slot(self):
        self.backend.fail_assignment = True
        with patch('image_pool.add_image_disk'),patch('image_pool.vm_rpc.execute',return_value={'exit_code':0}):
            with self.assertRaises(OSError):self.backend.create({'templateID':'second'})
        self.assertFalse(any(r.get('pool_state')=='leased' for r in self.backend.records.values()))
        self.assertFalse(any(r.get('exec_root') for r in self.backend.committed.values()))

    def test_mount_failure_destroys_attached_slot(self):
        with patch('image_pool.add_image_disk'),patch('image_pool.vm_rpc.execute',return_value={'exit_code':1,'stderr':b'mount failed'}):
            with self.assertRaisesRegex(RuntimeError,'mount failed'):self.backend.create({})
        self.assertFalse(any(r.get('pool_state')=='leased' for r in self.backend.records.values()))

    def test_oci_entrypoint_launch_and_durable_metadata(self):
        from oci_config import normalize
        config=normalize(dict(Cmd=['/native'],User='1234:1235',WorkingDir='/work',Env=['A=b']))
        self.backend.image_catalog['base']['oci_config']=config
        with patch('image_pool.add_image_disk'),patch('image_pool.vm_rpc.execute',side_effect=[{'exit_code':0},{'exit_code':0,'stdout':b'123\n'}]) as execute:
            sid=self.backend.create({})['sandboxID']
        self.assertEqual(self.backend.committed[sid]['oci_config'],config)
        self.assertEqual(self.backend.committed[sid]['entrypoint_pid'],123)
        self.assertEqual(execute.call_args.args[1][-1],'/native')

    def test_failed_oci_exec_destroys_private_binding(self):
        from oci_config import normalize
        self.backend.image_catalog['base']['oci_config']=normalize(dict(Cmd=['/missing']))
        with patch('image_pool.add_image_disk'),patch('image_pool.vm_rpc.execute',side_effect=[{'exit_code':0},{'exit_code':126,'stderr':b'no executable'}]):
            with self.assertRaisesRegex(RuntimeError,'no executable'):self.backend.create({})
        self.assertFalse(any(r.get('pool_state')=='leased' for r in self.backend.records.values()))

    def test_entrypoint_output_framing_empty_and_nonempty_streams(self):
        self.backend.records['live']={'status':'running','entrypoint_pid':123}
        for raw,out,err in [(b'running\n41 42\n__STDERR__\n', 'AB',''),(b'7\n__STDERR__\n65 72 72\n','','err')]:
            with patch.object(self.backend,'raw_execute',return_value={'exit_code':0,'stdout':raw}):
                r=self.backend.entrypoint('live')
            self.assertEqual((r['stdout'],r['stderr']),(out,err))

    def test_hot_add_does_not_rebuild_pool_or_rebind_ids(self):
        import json, os, hashlib
        image=self.backend.state.parent/'third.disk';image.write_bytes(b'third');image.chmod(0o444)
        catalog=self.backend.state.parent/'catalog.json'
        new=dict(disk=str(image),sha256=hashlib.sha256(b'third').hexdigest(),manifest='sha256:third')
        catalog.write_text(json.dumps({'third':new}))
        original=set(self.backend.records)
        with patch.dict(os.environ,{'E2B_IMAGE_CATALOG':str(catalog)}):
            result=self.backend.reload_images()
            self.assertEqual(result['added'],['third']);self.assertEqual(set(self.backend.records),original)
            catalog.write_text(json.dumps({'third':{**new,'manifest':'changed'}}))
            with self.assertRaises(APIError):self.backend.reload_images()
        self.assertEqual(self.backend.image_catalog['third']['manifest'],'sha256:third')
