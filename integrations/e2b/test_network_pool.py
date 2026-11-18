# SPDX-License-Identifier: Apache-2.0
import copy
import json
import sys
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from network_pool import NetworkPool, FIELDS


class NetworkPoolTests(unittest.TestCase):
    def setUp(self):
        self.record = dict(namespace='owned', netns='/owned/ns', holder_pid=123,
                           holder_start='10', holder_inode=42, ip='10.71.0.2',
                           cni_owner='original', network_slot='net-owned', network_generation=4, network_routes=['route replace 0.0.0.0/0 dev eth0 via 10.71.0.1'])
        self.backend = SimpleNamespace(cni={'type':'bridge', 'bridge':'ownedbr',
            'ipam':{'type':'host-local','subnet':'10.71.0.0/16'}}, lock=threading.RLock(),
            records={}, holders={}, template_pool=SimpleNamespace(enabled=True))
        b = self.backend
        b.get = lambda sid: b.records[sid]
        b.save = lambda: setattr(b, 'committed', copy.deepcopy(b.records))
        with patch.dict('os.environ', {'E2B_CONNTRACK_BIN': sys.executable}):
            self.pool = NetworkPool(b)

    def test_rejects_unknown_cni_policy_and_nat(self):
        for field,value in [('ipMasq',True),('capabilities',{}),('plugins',[])]:
            with patch.dict(self.backend.cni,{field:value}):
                with self.assertRaises(ValueError):NetworkPool(self.backend)

    def test_claim_transfers_generation_but_never_credentials(self):
        b = self.backend
        b.records['net-owned'] = dict(self.record, network_only=True,status='network_idle',token='old')
        b.records['new'] = dict(token='fresh',pool_state='preparing')
        with patch.object(self.pool,'owned'),patch.object(self.pool,'run') as run,patch.object(self.pool,'clear_host_conntrack'):
            self.assertEqual(self.pool.claim('new'),'/owned/ns')
            self.assertEqual(run.call_args.args[0][:6],['ip','link','set','dev','eth0','address'])
            self.assertNotIn('down',run.call_args.args[0])
        self.assertNotIn('net-owned',b.committed)
        self.assertEqual(b.committed['new']['network_generation'],5)
        self.assertEqual(b.committed['new']['cni_owner'],'original')
        self.assertEqual(b.committed['new']['token'],'fresh')
        self.assertNotIn('network_only',b.committed['new'])

    def test_failed_claim_commit_never_activates_namespace(self):
        b=self.backend;b.records['net-owned']=dict(self.record,network_only=True,status='network_idle')
        b.records['new']=dict(token='fresh',pool_state='preparing')
        b.save=lambda: (_ for _ in ()).throw(OSError('commit failed'))
        with patch.object(self.pool,'owned'),patch.object(self.pool,'run') as run:
            with self.assertRaises(OSError):self.pool.claim('new')
            run.assert_not_called()
        self.assertNotIn('net-owned',b.records)
        self.assertEqual(b.records['new']['pool_state'],'preparing')

    def test_failed_reset_cannot_publish_idle_network(self):
        b=self.backend;b.records['old']=dict(self.record,pool_origin=True,direct_prepared=True,status='deleting')
        with patch.object(self.pool,'reset',side_effect=RuntimeError('reset failed')):
            with self.assertRaises(RuntimeError):self.pool.retire('old')
        self.assertEqual(set(b.records),{'old'})
        self.assertEqual(b.records['old']['status'],'deleting')

    def test_live_fork_child_is_never_recycled_as_pool_network(self):
        self.backend.records['child']=dict(self.record,status='deleting')
        with patch.object(self.pool,'reset') as reset:
            self.assertIsNone(self.pool.retire('child'))
            reset.assert_not_called()

    def test_reset_only_removes_owned_host_conntrack(self):
        links=[{'ifname':'lo'}, {'ifname':'eth0','ifindex':2,'flags':['UP'],'linkinfo':{'info_kind':'veth'}},
               {'ifname':'tap_kua_2'}]
        def run(args,r=None,**kwargs):
            return SimpleNamespace(stdout=json.dumps(links if args[:4]==['ip','-j','-d','link'] else []))
        with patch.object(self.pool,'owned'),patch.object(self.pool,'run',side_effect=run) as local, \
             patch('conntrack_netlink.delete_ip',side_effect=OSError(95,'unsupported')), \
             patch('network_pool.subprocess.run',return_value=SimpleNamespace(returncode=0)) as host:
            self.pool.reset(self.record)
        self.assertEqual(host.call_count,2)
        for call in host.call_args_list:
            argv=call.args[0]
            self.assertIn(self.record['ip'],argv)
            self.assertNotIn('-F',argv)
        flush=[c for c in local.call_args_list if '-F' in c.args[0]]
        self.assertEqual(len(flush),1)
        self.assertEqual(flush[0].args[1]['netns'],'/owned/ns')

    def test_capture_preserves_default_and_custom_cni_routes(self):
        b=self.backend;b.records['new']=dict(self.record)
        routes=[dict(dst='default',gateway='10.71.0.1',dev='eth0'),
                dict(dst='10.71.0.0/16',protocol='kernel',scope='link'),
                dict(dst='192.0.2.0/24',gateway='10.71.0.2',metric=42)]
        with patch.object(self.pool,'run',return_value=SimpleNamespace(stdout=json.dumps(routes))):
            self.pool.capture_routes('new')
        self.assertEqual(b.committed['new']['network_routes'],[
            'route replace 0.0.0.0/0 dev eth0 via 10.71.0.1',
            'route replace 192.0.2.0/24 dev eth0 via 10.71.0.2 metric 42'])

    def test_parallel_failure_joins_remaining_cleanup(self):
        finished=threading.Event()
        def fail():
            raise RuntimeError('cleanup failed')
        def complete():
            import time
            time.sleep(.02)
            finished.set()
        with self.assertRaisesRegex(RuntimeError,'cleanup failed'):
            self.pool.parallel([fail,complete])
        self.assertTrue(finished.is_set())

    def test_private_preparation_overlaps_cleanup_but_exit_joins_it(self):
        b=self.backend;b.records['net-owned']=dict(self.record,network_only=True,status='network_idle')
        b.records['new']=dict(token='fresh',pool_state='preparing')
        started=threading.Event();release=threading.Event();finished=threading.Event()
        def cleanup(_):
            started.set()
            if not release.wait(2):raise RuntimeError('test did not release cleanup')
            finished.set()
        with patch.object(self.pool,'owned'),patch.object(self.pool,'run'),patch.object(self.pool,'clear_host_conntrack',side_effect=cleanup):
            with self.pool.claim_preparing('new') as ns:
                self.assertEqual(ns,'/owned/ns')
                self.assertTrue(started.wait(1))
                self.assertFalse(finished.is_set())
                self.assertEqual(b.records['new']['pool_state'],'preparing')
                release.set()
            self.assertTrue(finished.is_set())

    def test_preparation_failure_joins_cleanup_before_destroy(self):
        b=self.backend;b.records['net-owned']=dict(self.record,network_only=True,status='network_idle')
        b.records['new']=dict(token='fresh',pool_state='preparing')
        finished=threading.Event()
        def cleanup(_):
            import time
            time.sleep(.02)
            finished.set()
        with patch.object(self.pool,'owned'),patch.object(self.pool,'run'),patch.object(self.pool,'clear_host_conntrack',side_effect=cleanup):
            with self.assertRaisesRegex(RuntimeError,'KVM failed'):
                with self.pool.claim_preparing('new'):
                    raise RuntimeError('KVM failed')
            self.assertTrue(finished.is_set())
            self.assertEqual(b.records['new']['pool_state'],'preparing')

    def test_cleanup_failure_prevents_scope_success(self):
        b=self.backend;b.records['net-owned']=dict(self.record,network_only=True,status='network_idle')
        b.records['new']=dict(token='fresh',pool_state='preparing')
        published=False
        with patch.object(self.pool,'owned'),patch.object(self.pool,'run'),patch.object(self.pool,'clear_host_conntrack',side_effect=RuntimeError('cleanup failed')):
            with self.assertRaisesRegex(RuntimeError,'cleanup failed'):
                with self.pool.claim_preparing('new'):
                    pass
                published=True
        self.assertFalse(published)

    def test_native_identity_still_joins_cleanup_without_cli_mutation(self):
        b=self.backend;b.records['net-owned']=dict(self.record,network_only=True,status='network_idle')
        b.records['new']=dict(token='fresh',pool_state='preparing',status='creating')
        with patch.object(self.pool,'owned'),patch.object(self.pool,'run') as run,patch.object(self.pool,'clear_host_conntrack') as clear:
            with self.pool.claim_preparing('new', native_identity=True) as ns:
                self.assertEqual(ns, self.record['netns'])
            run.assert_not_called()
            clear.assert_called_once()

    def test_reservation_commits_creation_and_transfer_once_before_side_effects(self):
        b=self.backend;b.records['net-owned']=dict(self.record,network_only=True,status='network_idle')
        b.records['new']=dict(token='fresh',pool_state='preparing',status='creating')
        with patch.object(b,'save',wraps=b.save) as commit,patch.object(self.pool,'owned'),patch.object(self.pool,'run') as run,patch.object(self.pool,'clear_host_conntrack') as clear:
            reservation=self.pool.reserve('new')
            self.assertEqual(commit.call_count,1)
            run.assert_not_called();clear.assert_not_called()
            self.assertNotIn('net-owned',b.committed)
            self.assertEqual(b.committed['new']['network_generation'],5)
            self.assertEqual(b.committed['new']['token'],'fresh')
            with self.pool.claim_preparing('new',reservation):
                pass
            self.assertEqual(commit.call_count,1)
            self.assertEqual(run.call_count,1)
            self.assertEqual(clear.call_count,1)
