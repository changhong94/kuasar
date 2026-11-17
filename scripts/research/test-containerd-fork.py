#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
import json, os, signal, socket, subprocess, sys, time, re, threading
from pathlib import Path
repo = Path(__file__).resolve().parents[2]
out = Path(sys.argv[1]).resolve(); out.mkdir(parents=True,exist_ok=True)
state=out/'state'; state.mkdir(exist_ok=True)
config=json.loads('{}')
text=Path(sys.argv[2]).read_text(); (out/'config.toml').write_text(text)
admin=out/'admin.sock'; grpc=out/'sandboxer.sock'
log=(out/'host.log').open('w')
os.environ['KUASAR_CONTAINERD_TTRPC_ADDRESS']=str(out/'ctrd.sock.ttrpc')
proc=subprocess.Popen([str(repo/'target/debug/cloud_hypervisor'),'--config',str(out/'config.toml'),'--dir',str(state),'--listen',str(grpc),'--admin-listen',str(admin)],stdout=log,stderr=log,start_new_session=True)
client=str(repo/'target/debug/examples/fork-agent-client')

stop_logs=threading.Event()
def collect_serial(sid):
    with (out/(sid+'.serial.log')).open('ab') as log:
        while not stop_logs.is_set():
            try:
                with socket.socket(socket.AF_UNIX) as stream:
                    stream.settimeout(.2);stream.connect(str(state/sid/'serial.sock'))
                    while not stop_logs.is_set():
                        try:
                            b=stream.recv(65536)
                            if not b:break
                            log.write(b);log.flush()
                        except socket.timeout:pass
            except OSError:pass
            stop_logs.wait(.02)
log_threads=[threading.Thread(target=collect_serial,args=(sid,),daemon=True)
             for sid in ('seed00000001','child0000001','grand0000001')]
for thread in log_threads:thread.start()

def call(action, **kw):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(75);s.connect(str(admin));s.sendall((json.dumps(dict(action=action,**kw))+'\n').encode()); b=b''
        while not b.endswith(b'\n'):
            data=s.recv(65536); assert data; b+=data
    r=json.loads(b);assert r['ok'],r
    print(action,r,flush=True);return r

def rpc(op, sid, *args):
    r=subprocess.run([client,op,str(state/sid/'task.vsock'),*map(str,args)],capture_output=True,text=True,timeout=50)
    print(op,sid,r.stdout,r.stderr,flush=True)
    assert r.returncode==0,(r.stdout,r.stderr)
    return r.stdout

def wait_exec(sid, command, expected):
    for _ in range(100):
        r=rpc('exec',sid,command)
        if expected in r:return r
        time.sleep(.1)
    raise AssertionError((command,expected,r))

ctrd_repo=repo.parent/'containerd'
ctrd_config=out/'containerd.toml'
ctrd_config.write_text(f'''version = 3
root = "{out}/ctrd-root"
state = "{out}/ctrd-state"
disabled_plugins = ["io.containerd.grpc.v1.cri"]
[grpc]
address = "{out}/ctrd.sock"
[ttrpc]
address = "{out}/ctrd.sock.ttrpc"
[proxy_plugins.vmm]
type = "sandbox"
address = "{grpc}"
''')
event_processes=[];event_files=[]
ctrd_log=(out/'containerd.log').open('w')
ctrd=subprocess.Popen([str(ctrd_repo/'bin/containerd'),'--config',str(ctrd_config)],stdout=ctrd_log,stderr=ctrd_log,start_new_session=True)
def ctr(ns,*args,allowed=(0,)):
    result=subprocess.run([str(ctrd_repo/'bin/ctr'),'--address',str(out/'ctrd.sock'),'--namespace',ns,*map(str,args)],capture_output=True,text=True,timeout=60)
    print('ctr',ns,args,result.returncode,result.stdout,result.stderr,flush=True)
    assert result.returncode in allowed,(args,result.stdout,result.stderr)
    return result.stdout
try:
    for _ in range(200):
        if admin.exists() and (out/'ctrd.sock').exists():break
        assert proc.poll() is None and ctrd.poll() is None;time.sleep(.05)
    p,c='seed00000001','child0000001';pn,cn='fork-parent','fork-child'
    call('sandbox-boot',sandbox_id=p)
    rpc('create',p,'samecontainer',p,grpc,10,'inherited')
    rpc('exec-create',p,'samecontainer',p,grpc,'execworker')
    before=rpc('state',p,'samecontainer')
    ctr(pn,'sandboxes','import-running','--admin-address',admin,p)
    # Retry registration is read-only for already adopted guest processes.
    ctr(pn,'sandboxes','import-running','--admin-address',admin,p)
    ctr('wrong-owner','sandboxes','import-running','--admin-address',admin,p,allowed=(1,))
    ctr(pn,'sandboxes','fork','--admin-address',admin,'--target-namespace',pn,p,'rejected0001',allowed=(1,))
    assert not (state/'rejected0001').exists()
    ctr('occupied','containers','create','--rootfs',out,'samecontainer')
    ctr(pn,'sandboxes','fork','--admin-address',admin,'--target-namespace','occupied',p,'collision001',allowed=(1,))
    assert not (state/'collision001').exists()
    ctr('occupied','containers','delete','samecontainer')
    for ns in (pn,cn):
        file=(out/(ns+'.events.log')).open('w');event_files.append(file)
        event_processes.append(subprocess.Popen([str(ctrd_repo/'bin/ctr'),'--address',str(out/'ctrd.sock'),'--namespace',ns,'events','namespace=='+ns],stdout=file,stderr=file))
    time.sleep(.2)
    assert 'samecontainer' in ctr(pn,'containers','list')
    assert 'RUNNING' in ctr(pn,'tasks','list')
    ctr(pn,'sandboxes','fork','--admin-address',admin,'--target-namespace',cn,p,c)
    for sid,ns,value in ((p,pn,20),(c,cn,30)):
        assert rpc('state',sid,'samecontainer')==before
        assert 'samecontainer' in ctr(ns,'containers','list')
        assert 'RUNNING' in ctr(ns,'tasks','list')
        ctr(ns,'tasks','exec','--null-io','--exec-id','normalexec','samecontainer','/bin/sh','-c',f'echo {value} > /init-command; echo {value+1} > /execworker-command; echo {value} > /ctrd-exec')
        assert rpc('exec',sid,'cat /run/kuasar/state/samecontainer/rootfs/ctrd-exec').strip()==str(value)
        for worker,expected in (('init',value),('execworker',value+1)):
            for _ in range(100):
                sample=rpc('exec',sid,f'cat /run/kuasar/state/samecontainer/rootfs/{worker}-state').split()
                if len(sample)==3 and int(sample[1])==expected:break
                time.sleep(.05)
            assert len(sample)==3 and int(sample[1])==expected,sample
        time.sleep(.1)
        assert '/tasks/exit' in (out/(ns+'.events.log')).read_text()
        if sid==p:assert 'normalexec' not in (out/(cn+'.events.log')).read_text()
    assert 'samecontainer' not in ctr('unrelated','tasks','list')
    # Reload persisted task registrations without touching the guest.
    ctrd.terminate();ctrd.wait(timeout=20)
    ctrd=subprocess.Popen([str(ctrd_repo/'bin/containerd'),'--config',str(ctrd_config)],stdout=ctrd_log,stderr=ctrd_log,start_new_session=True)
    time.sleep(1)
    for ns in (pn,cn): assert 'RUNNING' in ctr(ns,'tasks','list')
    deletion=[(p,pn),(c,cn)]
    if os.environ.get('DELETE_CHILD_FIRST'):deletion.reverse()
    for index,(sid,ns) in enumerate(deletion):
        ctr(ns,'tasks','kill','--signal','SIGKILL','--exec-id','execworker','samecontainer')
        ctr(ns,'tasks','delete','--exec-id','execworker','samecontainer',allowed=(0,137))
        ctr(ns,'tasks','kill','--signal','SIGKILL','samecontainer')
        for _ in range(50):
            if 'STOPPED' in ctr(ns,'tasks','list'):break
            time.sleep(.05)
        ctr(ns,'tasks','delete','samecontainer')
        assert 'samecontainer' not in ctr(ns,'tasks','list')
        ctr(ns,'containers','delete','samecontainer')
        assert 'samecontainer' not in ctr(ns,'containers','list')
        assert json.loads((state/sid/'sandbox.json').read_text())['containers']=={}
        ctr(ns,'sandboxes','remove',sid)
        assert sid not in ctr(ns,'sandboxes','list')
        assert not (state/sid).exists()
        if index==0:
            survivor,survivor_ns=deletion[1]
            assert 'RUNNING' in ctr(survivor_ns,'tasks','list')
            ctr(survivor_ns,'tasks','exec','--null-io','--exec-id','after-peer','samecontainer','/bin/sh','-c','echo alive > /after-peer')
            assert rpc('exec',survivor,'cat /run/kuasar/state/samecontainer/rootfs/after-peer').strip()=='alive'
    assert call('sandbox-list')['sandboxes']==[]
    assert not list(out.rglob('memory.img'))
    pss=re.findall(r'combined_ram_pss_kib=(\d+) baseline_kib=(\d+)',(out/'host.log').read_text())
    assert len(pss)==1 and pss[0][0]==pss[0][1],pss
    result=dict(result='PASS',pss=pss,deletion_order=deletion,checks=['containerd sandbox/container/task registration without guest Create/Start','separate namespaces, same guest container ID, scoped exit events and rejected namespace/container-ID conflicts','normal ctr query/exec/kill/delete','containerd restart recovers inherited tasks','peer sandbox destruction leaves surviving inherited container operational; independent RAM writes','no memory.img; fork boundary RAM PSS unchanged'])
    (out/'results.json').write_text(json.dumps(result,indent=2)+'\n');print(result)
finally:
    stop_logs.set()
    for process in (proc,ctrd):
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass
        process.wait()
    for event in event_processes:
        if event.poll() is None:event.terminate()
        event.wait(timeout=5)
    for file in event_files:file.close()
    log.close();ctrd_log.close()
