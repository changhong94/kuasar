# SPDX-License-Identifier: Apache-2.0
"""Real ext4 and on-disk chain assertions shared by SDK and kubectl tests."""
import hashlib, json, subprocess, time
from pathlib import Path

def digest_file(path):
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024), b''): digest.update(chunk)
    return digest.hexdigest()

class DiskChecks:
    def __init__(self, out, execute):
        self.out=Path(out); self.execute=execute; self.sealed={}; self.report={}
        self.repo=Path(__file__).resolve().parents[2]

    def seed(self, parent):
        mounts=self.execute(parent, '/bin/busybox cat /proc/mounts')
        assert any(line.split()[1]=='/' and line.split()[2]=='ext4' for line in mounts.splitlines()), mounts
        # Retain a large immutable ancestor file, a durable marker and a dirty
        # marker. The fork must preserve both disk contents and dirty page cache.
        self.execute(parent, '/bin/busybox dd if=/dev/urandom of=/tmp/ancestor bs=65536 count=32 2>/dev/null; echo durable > /tmp/durable; /bin/busybox sync; echo dirty > /tmp/dirty')
        self.ancestor=self.execute(parent, '/bin/busybox sha256sum /tmp/ancestor').split()[0]
        self.report['mounts']=mounts

    def state_dir(self, pod):
        k3s=self.repo.parents[1]/'tools/k3s-v1.31'
        raw=subprocess.check_output([str(k3s),'kubectl','--kubeconfig',str(self.out/'kubeconfig'),'get','pod',pod,'-o','json'])
        cid=json.loads(raw)['status']['containerStatuses'][0]['containerID'].split('://',1)[1]
        directories=[p.parent for p in (self.out/'state').glob('*/containerd-ids.json') if cid in json.loads(p.read_text()).values()]
        assert len(directories)==1, directories
        return directories[0]

    def drop_caches(self, pod):
        directory=self.state_dir(pod)
        try:
            subprocess.run([str(self.repo/'target/debug/examples/fork-agent-client'),'exec',str(directory/'task.vsock'),'/bin/busybox sync; echo 3 > /proc/sys/vm/drop_caches'],check=True,capture_output=True,text=True)
        except subprocess.CalledProcessError as error:
            raise AssertionError(f'guest cache-drop failed for {pod}: {error.stdout} {error.stderr}') from error

    def inspect(self, parent, child):
        self.states={pod:self.state_dir(pod) for pod in (parent,child)}
        for pod in (parent,child):
            assert self.execute(pod, '/bin/busybox cat /tmp/durable /tmp/dirty')=='durable\ndirty\n'
        rows=[]
        for file in (self.out/'state').glob('*/fork-disks/*'):
            st=file.stat(); writable=bool(st.st_mode & 0o222)
            row=dict(path=str(file),inode=st.st_ino,links=st.st_nlink,allocated=st.st_blocks*512,size=st.st_size,writable=writable)
            if not writable:
                digest=digest_file(file)
                self.sealed[file]=(st.st_ino,digest);row['sha256']=digest
            rows.append(row)
        leaves=[r for r in rows if r['writable']]
        assert len(leaves)==2 and len({r['inode'] for r in leaves})==2, rows
        assert all(r['allocated'] < 32*1024*1024 for r in leaves), rows
        assert any(r['links']>=2 for r in rows if not r['writable']), rows
        self.report['chain_after_fork']=rows
        for pod, value in ((parent,'parent'),(child,'child')):
            size=123 if pod==parent else 456
            action='/bin/busybox rm /tmp/dirty' if pod==parent else '/bin/busybox mv /tmp/dirty /tmp/renamed'
            self.execute(pod, f'echo {value} > /tmp/durable; {action}; echo {value} > /tmp/new; /bin/busybox dd if=/dev/zero of=/tmp/private bs=65536 count=8 2>/dev/null; /bin/busybox truncate -s {size} /tmp/private')
            self.drop_caches(pod)
        for pod,value in ((parent,'parent'),(child,'child')):
            assert self.execute(pod, '/bin/busybox cat /tmp/durable /tmp/new')==value+'\n'+value+'\n'
            assert self.execute(pod, '/bin/busybox stat -c %s /tmp/private').strip()==('123' if pod==parent else '456')
            assert self.execute(pod, '/bin/busybox sha256sum /tmp/ancestor').split()[0]==self.ancestor
        assert self.execute(child, '/bin/busybox cat /tmp/renamed')=='dirty\n'
        assert self.execute(parent, 'test ! -e /tmp/renamed && test ! -e /tmp/dirty && echo absent')=='absent\n'
        self.verify_sealed()
        self.save()

    def verify_sealed(self):
        for file,(inode,digest) in self.sealed.items():
            if file.exists():
                assert file.stat().st_ino==inode
                assert digest_file(file)==digest, str(file)

    def survivor(self, pod):
        # Pod deletion stops the VM before kubelet's sandbox GC removes its
        # directory. Check both stages, including loss of all peer hard links.
        assert self.execute(pod, '/bin/busybox cat /tmp/durable') in ('parent\n','child\n')
        peer=[directory for name,directory in self.states.items() if name!=pod][0]
        deadline=time.monotonic()+150
        while peer.exists() and time.monotonic()<deadline: time.sleep(1)
        assert not peer.exists(), 'peer sandbox directory was not reclaimed'
        self.report['peer_directory_removed']=str(peer)
        self.drop_caches(pod)
        assert self.execute(pod, '/bin/busybox sha256sum /tmp/ancestor').split()[0]==self.ancestor
        assert self.execute(pod, '/bin/busybox cat /tmp/durable') in ('parent\n','child\n')
        self.verify_sealed()
        self.report['survivor_after_peer_deletion']=pod
        self.save()

    def save(self):
        self.report['checks']=['ext4 rootfs', 'durable and dirty file inheritance', 'overwrite, unlink, create, truncate', 'sync and guest page-cache eviction', 'independent writable leaf inodes and shared history inodes', 'sealed layer SHA256 unchanged', 'ancestor reads and executable reload after peer destruction']
        (self.out/'writable-disk.json').write_text(json.dumps(self.report,indent=2))
