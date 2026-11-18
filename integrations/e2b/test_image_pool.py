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
            self.image_catalog[name]=dict(disk=str(p),sha256=hashlib.sha256(name.encode()).hexdigest(),manifest='sha256:'+name,oci_config={'Env':[], 'Entrypoint':[], 'Cmd':['/bin/sleep','infinity'], 'WorkingDir':'/', 'User':'0'})
    def admin(self, action, **kw):
        self.calls.append((action,kw))
        return dict(live_fork=dict(api_socket='test-api',vsock_socket='test-vsock'))


class ImagePoolTests(unittest.TestCase):
    def test_metadata_staging_withdraws_ready_slots_without_creating_vms(self):
        original = set(self.backend.records)
        created = self.backend.created
        def stage(action, **kw):
            self.assertEqual(action, 'sandbox-snapshot-stage')
            self.assertEqual(self.backend.records[kw['sandbox_id']]['pool_state'], 'preparing')
            self.assertEqual(self.backend.committed[kw['sandbox_id']]['pool_state'], 'preparing')
            return {'ok': True}
        with patch.object(self.backend, 'admin', side_effect=stage):
            result = self.backend.stage_snapshot_slots({'key':'image-test'})
            second = self.backend.stage_snapshot_slots({'key':'image-second'})
        self.assertEqual(result['metadata_staged_slots'], 2)
        self.assertEqual(second['metadata_staged_slots'], 2)
        self.assertEqual(set(self.backend.records), original)
        self.assertEqual(self.backend.created, created)
        self.assertTrue(all(r['pool_state']=='ready' for r in self.backend.records.values()))

    def test_failed_metadata_stage_is_quarantined_before_destruction(self):
        self.pool.close()  # keep the failed-slot observation deterministic
        destroyed = []
        original = self.backend.destroy
        def destroy(sid):
            self.assertEqual(self.backend.records[sid]['pool_state'], 'quarantined')
            self.assertEqual(self.backend.committed[sid]['pool_state'], 'quarantined')
            destroyed.append(sid)
            original(sid)
        with patch.object(self.backend, 'admin', side_effect=OSError('stage sync failed')):
            with patch.object(self.backend, 'destroy', side_effect=destroy):
                with self.assertRaisesRegex(OSError, 'stage sync failed'):
                    self.backend.stage_snapshot_slots({'key':'image-test'})
        self.assertEqual(len(destroyed), 1)
        self.assertNotIn(destroyed[0], self.backend.records)

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.backend=FakeImages(Path(self.tmp.name)/'state')
        self.pool=TemplatePool(self.backend,2,refill_delay=0)
        self.backend.template_pool=self.pool
        for r in self.backend.records.values():r['vsock_socket']='test-vsock'
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
        with patch('image_pool.add_image_disk',side_effect=attach),patch('image_pool.vm_rpc.execute',return_value={'exit_code':0,'stdout':b'123'}):
            a=self.backend.create({'templateID':'base'})
            b=self.backend.create({'templateID':'second'})
        self.assertNotEqual(a['sandboxID'],b['sandboxID'])
        self.assertEqual(a['template_id'],'base');self.assertEqual(b['template_id'],'second')
        self.assertEqual(self.backend.created,2)
        self.assertEqual(self.pool.status()['assigned'],2)
        self.assertEqual(set(attached),{x['disk'] for x in self.backend.image_catalog.values()})
        for sid in [a['sandboxID'],b['sandboxID']]:
            self.assertEqual(self.backend.committed[sid]['exec_root'],'/run/image-root')
    def test_image_identity_committed_before_first_vm_mutation(self):
        def attach(socket, path):
            records=[r for r in self.backend.committed.values() if r.get('pool_state')=='binding']
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]['image_disk'], path)
            self.assertEqual(records[0]['template_id'], 'second')
            self.assertNotIn('exec_root', records[0])
        with patch('image_pool.add_image_disk', side_effect=attach), patch('image_pool.vm_rpc.execute',return_value={'exit_code':0,'stdout':b'123'}):
            self.backend.create({'templateID':'second'})

    def test_binding_commit_failure_never_attaches_image(self):
        original=self.backend.save
        failed=[]
        def save():
            original()
            if not failed and any(r.get('pool_state')=='binding' for r in self.backend.records.values()):
                failed.append(True)
                raise OSError('ambiguous assignment commit')
        with patch.object(self.backend,'save',side_effect=save), patch('image_pool.add_image_disk') as attach:
            with self.assertRaises(OSError):self.backend.create({'templateID':'second'})
            attach.assert_not_called()
        self.assertTrue(any(r.get('pool_state')=='quarantined' for r in self.backend.committed.values()))

    def test_failed_attachment_is_not_published_or_reused(self):
        ready=set(self.backend.records)
        with patch('image_pool.add_image_disk',side_effect=RuntimeError('attach failed')):
            with self.assertRaises(RuntimeError):self.backend.create({})
        self.assertTrue(any(sid not in self.backend.records for sid in ready))
        self.assertFalse(any(r.get('pool_state')=='leased' for r in self.backend.records.values()))
    def test_prepared_device_binding_is_private_durable_and_failure_is_not_published(self):
        from unittest.mock import Mock
        self.backend.slot_disks = Mock()
        for sid, record in self.backend.records.items():
            record['slot_disk'] = '/private/' + sid
        self.backend.save()
        def fail_bind(path, image):
            committed = [r for r in self.backend.committed.values() if r.get('pool_state') == 'binding']
            self.assertEqual(len(committed), 1)
            self.assertEqual(committed[0]['slot_disk'], path)
            self.assertEqual(committed[0]['image_sha256'], image['sha256'])
            raise OSError('disk fsync failure')
        self.backend.slot_disks.bind.side_effect = fail_bind
        with patch('image_pool.add_image_disk') as attach, patch('image_pool.vm_rpc.execute') as mount:
            with self.assertRaises(OSError): self.backend.create({})
            attach.assert_not_called(); mount.assert_not_called()
        self.assertFalse(any(r.get('pool_state') == 'leased' for r in self.backend.records.values()))

    def test_changed_or_unknown_image_does_not_consume_slot(self):
        with self.assertRaises(APIError):self.backend.create({'templateID':'unknown'})
        p=Path(self.backend.image_catalog['base']['disk']);p.chmod(0o644);p.write_bytes(b'changed')
        with self.assertRaises(APIError):self.backend.create({})
        self.assertEqual(self.pool.status()['ready'],2)
    def test_live_parent_rejects_switching_image(self):
        with patch('image_pool.add_image_disk'),patch('image_pool.vm_rpc.execute',return_value={'exit_code':0,'stdout':b'123'}):
            parent=self.backend.create({'templateID':'second'})['sandboxID']
        with self.assertRaises(APIError):self.backend.create({'templateID':'base'},parent)
        ready=self.pool.status()['ready']
        self.backend.create({},parent)
        self.assertEqual(self.pool.status()['ready'],ready)
        self.assertEqual(self.backend.parents[-1],parent)

    def test_failed_publish_commit_destroys_bound_slot(self):
        self.backend.fail_assignment = True
        with patch('image_pool.add_image_disk'),patch('image_pool.vm_rpc.execute',return_value={'exit_code':0,'stdout':b'123'}):
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
        new=dict(disk=str(image),sha256=hashlib.sha256(b'third').hexdigest(),manifest='sha256:third',oci_config=self.backend.image_catalog['base']['oci_config'])
        catalog.write_text(json.dumps({'third':new}))
        original=set(self.backend.records)
        from unittest.mock import Mock
        cache = Mock()
        def prepare(images):
            if 'third' not in self.backend.image_catalog:
                self.assertEqual(images, [new])
        with patch.dict(os.environ,{'E2B_IMAGE_CATALOG':str(catalog)}), \
             patch.object(self.backend, 'slot_disks', cache, create=True):
            cache.prepare_images.side_effect = OSError('cache sync failed')
            with self.assertRaisesRegex(OSError, 'cache sync failed'):
                self.backend.reload_images()
            self.assertNotIn('third', self.backend.image_catalog)
            self.assertEqual(set(self.backend.records), original)
            cache.prepare_images.side_effect = prepare
            result=self.backend.reload_images()
            self.assertEqual(result['added'],['third']);self.assertEqual(set(self.backend.records),original)
            catalog.write_text(json.dumps({'third':{**new,'manifest':'changed'}}))
            with self.assertRaises(APIError):self.backend.reload_images()
        self.assertEqual(self.backend.image_catalog['third']['manifest'],'sha256:third')

    def test_template_builder_never_starts_entrypoint(self):
        sid=next(iter(self.backend.records))
        with patch.object(self.backend,'_create_live',return_value=sid), \
             patch.object(self.backend,'response',return_value={'sandboxID':sid}), \
             patch.object(self.backend,'prepare_slot'), \
             patch('image_pool.add_image_disk'), \
             patch('image_pool.vm_rpc.execute',return_value={'exit_code':0}) as rpc, \
             patch.object(self.backend,'start_entrypoint') as start:
            self.backend._create_image({'templateID':'base'},_snapshot_builder=True)
        start.assert_not_called()
        self.assertEqual(rpc.call_count,1)
        self.assertNotIn('entrypoint_pid',self.backend.records[sid])

    def test_entrypoint_failure_is_never_published(self):
        before=set(self.backend.records)
        with patch('image_pool.add_image_disk'), patch('image_pool.vm_rpc.execute',side_effect=[{'exit_code':0},{'exit_code':126,'stderr':b'exec failed'}]):
            with self.assertRaisesRegex(RuntimeError,'OCI entrypoint failed'):
                self.backend.create({'templateID':'base'})
        self.assertTrue(before-set(self.backend.records))
        self.assertFalse(any(r.get('pool_state')=='leased' for r in self.backend.records.values()))

    def test_missing_oci_and_application_read_hints_rejected(self):
        with self.assertRaisesRegex(ValueError,'OCI configuration'):
            self.backend.validate_image_config({})
        with self.assertRaisesRegex(ValueError,'startup reads'):
            self.backend.validate_image_config({'startup_read_files':['/usr/local/bin/python3']})

    def test_only_discarded_snapshot_guest_skips_device_discovery(self):
        import os
        from unittest.mock import Mock
        sid = next(iter(self.backend.records))
        disks = Mock(path=self.backend.state.parent)
        self.backend.snapshot_templates = {}
        with patch.object(self.backend, 'slot_disks', disks, create=True), \
             patch('image_pool.add_image_disk') as attach, \
             patch('image_pool.vm_rpc.execute', return_value={'exit_code': 0}) as execute, \
             patch.dict(os.environ, {'E2B_SNAPSHOT_POOL': '1', 'E2B_IMAGE_MOUNT_HELPER': '/helper'}):
            self.backend.prepare_slot(sid)
            attach.assert_called_once()
            execute.assert_not_called()
            self.backend.prepare_slot(sid, keep_running=True)
            execute.assert_called_once_with('test-vsock', ['/helper', 'prepare'])
            execute.reset_mock()
            with patch.dict(os.environ, {'E2B_SNAPSHOT_POOL': '0'}):
                self.backend.prepare_slot(sid)
            execute.assert_called_once_with('test-vsock', ['/helper', 'prepare'])

    def test_direct_slot_only_stages_metadata_without_guest_or_disk(self):
        sid = next(iter(self.backend.records))
        self.backend.records[sid]['direct_prepared'] = True
        self.backend.snapshot_templates = {'base': {'key': 'generic-test'}}
        with patch('image_pool.add_image_disk') as attach, \
             patch('image_pool.vm_rpc.execute') as execute:
            self.backend.prepare_slot_device(sid)
            self.backend.prepare_slot(sid, device_prepared=True)
        attach.assert_not_called()
        execute.assert_not_called()
        self.assertEqual(self.backend.calls, [('sandbox-snapshot-stage',
            {'sandbox_id': sid, 'template': 'generic-test'})])
