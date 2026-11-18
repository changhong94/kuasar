# SPDX-License-Identifier: Apache-2.0
"""Owned bridge-network slots. No VM, credentials or guest state is recycled."""
import ipaddress
import concurrent.futures
import json
import os
import secrets
import shutil
import subprocess
import time


FIELDS = ('namespace', 'netns', 'holder_pid', 'holder_start', 'holder_inode',
          'ip', 'cni_owner', 'network_slot', 'network_generation', 'network_routes')


class NetworkPool:
    def __init__(self, backend):
        self.backend = backend
        c = backend.cni
        allowed = {'cniVersion', 'name', 'type', 'bridge', 'isGateway', 'ipMasq', 'ipam'}
        if (set(c)-allowed or c.get('type') != 'bridge' or c.get('ipMasq', False)
                or c.get('ipam', {}).get('type') != 'host-local'
                or 'ranges' in c.get('ipam', {})):
            raise ValueError('network reuse requires a single IPv4 bridge/host-local config without NAT or extensions')
        self.subnet = ipaddress.ip_network(c['ipam']['subnet'])
        if self.subnet.version != 4:
            raise ValueError('network reuse currently requires IPv4')
        self.conntrack = os.environ.get('E2B_CONNTRACK_BIN') or shutil.which('conntrack')
        if not self.conntrack or not os.access(self.conntrack, os.X_OK):
            raise ValueError('network reuse requires conntrack; set E2B_CONNTRACK_BIN')

    def owned(self, r):
        if (os.stat(r['netns']).st_ino != r['holder_inode'] or
                self.backend.process_start(r['holder_pid']) != r['holder_start']):
            raise RuntimeError('network slot namespace ownership changed')
        if ipaddress.ip_address(r['ip']) not in self.subnet:
            raise RuntimeError('network slot address is outside the owned CNI subnet')

    def run(self, args, r=None, input=None):
        prefix = ['nsenter', '--net='+r['netns']] if r else []
        return subprocess.run(prefix+args, input=input, text=True, capture_output=True, timeout=15, check=True)

    @staticmethod
    def parallel(operations):
        # Every task is joined before returning, including failure. A failed
        # cleanup cannot leave mutations running against a future generation.
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(operations))) as executor:
            futures = [executor.submit(operation) for operation in operations]
            concurrent.futures.wait(futures)
            for future in futures:
                future.result()

    def capture_routes(self, sid):
        b = self.backend
        r = b.get(sid)
        routes = json.loads(self.run(['ip', '-j', '-4', 'route', 'show', 'dev', 'eth0'], r).stdout)
        saved = []
        for route in routes:
            if route.get('protocol') == 'kernel':
                continue  # Connected routes return automatically when the link is up.
            allowed = {'dst', 'gateway', 'prefsrc', 'metric', 'protocol', 'scope', 'type', 'dev', 'flags'}
            if set(route)-allowed or route.get('type', 'unicast') != 'unicast':
                raise RuntimeError('network slot has an unsupported CNI route')
            dst = str(ipaddress.IPv4Network('0.0.0.0/0' if route['dst'] == 'default' else route['dst']))
            command = 'route replace '+dst+' dev eth0'
            for field, option in [('gateway', 'via'), ('prefsrc', 'src')]:
                if route.get(field):
                    command += ' '+option+' '+str(ipaddress.IPv4Address(route[field]))
            if 'metric' in route:
                command += ' metric '+str(int(route['metric']))
            saved.append(command)
        with b.lock:
            b.get(sid)['network_routes'] = saved
            b.save()

    def claim(self, sid):
        b = self.backend
        with b.lock:
            idle = next(((key, r) for key, r in b.records.items()
                         if r.get('network_only') and r.get('status') == 'network_idle'), None)
            if idle is None:
                return None
            key, old = idle
            self.owned(old)
            r = b.get(sid)
            r.update({k: old[k] for k in FIELDS})
            r['network_generation'] += 1
            # The transfer is one durable transaction: one namespace never has
            # two live owners, including after an ambiguous commit response.
            del b.records[key]
            if key in b.holders:
                b.holders[sid] = b.holders.pop(key)
            b.save()
            copy = dict(r)
        mac = bytearray(secrets.token_bytes(6)); mac[0] = (mac[0] & 0xfe) | 0x02
        # Drop entries created by late packets during the idle interval too.
        address = ':'.join(f'{x:02x}' for x in mac)
        # No new VMM exists yet. Finish both identity and old-state cleanup
        # before the caller creates one or publishes the slot.
        self.parallel([lambda: self.clear_host_conntrack(copy),
                       lambda: self.run(['ip', 'link', 'set', 'dev', 'eth0', 'address', address], copy)])
        return copy['netns']

    def reset(self, r):
        self.owned(r)
        links = json.loads(self.run(['ip', '-j', '-d', 'link', 'show'], r).stdout)
        eth = next(x for x in links if x['ifname'] == 'eth0')
        tap = 'tap_kua_'+str(eth['ifindex'])
        names = {x['ifname'] for x in links}
        if eth.get('linkinfo', {}).get('info_kind') != 'veth' or names != {'lo', 'eth0', tap}:
            raise RuntimeError('network slot has unexpected interfaces; refusing reuse')
        # The old VMM was reaped: no old TAP FD or guest queue can survive.
        # Keep administrative link state and CNI routes; toggling them adds RTNL
        # synchronization and loses default routes. The next VMM is empty while
        # wiring is attached, and its network gate precedes guest restoration.
        if 'UP' not in eth.get('flags', []):
            raise RuntimeError('network slot link unexpectedly down; refusing reuse')
        qdiscs = json.loads(self.run(['tc', '-j', 'qdisc', 'show'], r).stdout)
        remove = ''.join(f'qdisc del dev {x["dev"]} ingress\n' for x in qdiscs
                         if x.get('kind') == 'ingress' and x.get('dev') in ('eth0', tap))
        work = [lambda: self.run(['ip', '-batch', '-'], r,
                                 input=f'neigh flush dev eth0\nneigh flush dev {tap}\n'),
                lambda: self.run(['ip', 'neigh', 'flush', 'to', r['ip'], 'dev', self.backend.cni['bridge']]),
                lambda: self.run([self.conntrack, '-F'], r),
                lambda: self.clear_host_conntrack(r)]
        if remove:
            work.append(lambda: self.run(['tc', '-batch', '-'], r, input=remove))
        self.parallel(work)

    def clear_host_conntrack(self, r):
        # Host bridge conntrack can also contain this slot's flows. Never flush
        # the shared host table: remove only tuples containing the owned IP.
        def delete(selector):
            result = subprocess.run([self.conntrack, '-D', '-f', 'ipv4', selector, r['ip']],
                                    text=True, capture_output=True, timeout=15)
            if result.returncode and not (result.returncode == 1 and
                    '0 flow entries have been deleted' in result.stderr):
                raise RuntimeError('owned conntrack cleanup failed: '+result.stderr)
        self.parallel([lambda: delete('--orig-src'), lambda: delete('--orig-dst')])

    def retire(self, sid):
        b = self.backend
        with b.lock:
            r = dict(b.get(sid))
        if (not r.get('pool_origin') or not r.get('direct_prepared') or
                r.get('network_only') or b.template_pool is None or not b.template_pool.enabled):
            return None
        self.reset(r)
        return dict({k: r[k] for k in FIELDS}, backend='vm', network_only=True,
                    status='network_idle', pool_state='network_idle', expiry=time.time()+86400)
