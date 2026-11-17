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
# Never publish fixture events to the machine's existing containerd daemon.
os.environ.setdefault('KUASAR_CONTAINERD_TTRPC_ADDRESS',str(out/'unused-containerd.sock.ttrpc'))
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

try:
    for _ in range(100):
        if admin.exists():break
        assert proc.poll() is None;time.sleep(.05)
    p,c,g='seed00000001','child0000001','grand0000001'
    parent=call('sandbox-boot',sandbox_id=p)
    pcid=rpc('exec',p,'/bin/fork-probe cid').strip()
    assert int(pcid)==parent['live_fork']['cid']
    rpc('exec',p,'/bin/fork-probe >/dev/null 2>&1 &')
    wait_exec(p,'cat /run/probe-ready 2>/dev/null || true','10')
    hold=subprocess.Popen([client,'hold',str(state/p/'task.vsock')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    assert hold.stdout.readline().strip()=='CONNECTED'
    import http.client
    for bad_vsock in (None, {'cid':int(pcid),'socket':str(out/'bad.vsock')},
                      {'cid':2,'socket':str(out/'bad.vsock')}):
        conn=http.client.HTTPConnection('localhost')
        conn.sock=socket.socket(socket.AF_UNIX);conn.sock.connect(str(state/p/'api.sock'))
        conn.request('PUT','/api/v1/vm.fork',json.dumps({'child_api_socket':str(out/'bad.api'),
            'child_serial_socket':str(out/'bad.serial'),'child_vsock':bad_vsock}))
        response=conn.getresponse();assert response.status==400,response.read();response.read();conn.close()
        assert rpc('exec',p,'/bin/fork-probe cid').strip()==pcid
    start=time.monotonic();child=call('sandbox-fork',source_id=p,sandbox_id=c);elapsed=(time.monotonic()-start)*1000
    oldout,olderr=hold.communicate('\n',timeout=10);assert hold.returncode==0,(oldout,olderr)
    ccid=rpc('exec',c,'/bin/fork-probe cid').strip()
    assert int(ccid)==child['live_fork']['cid'] and ccid!=pcid
    assert rpc('exec',p,'/bin/fork-probe cid').strip()==pcid
    grand=call('sandbox-fork',source_id=c,sandbox_id=g)
    gcid=rpc('exec',g,'/bin/fork-probe cid').strip()
    assert int(gcid)==grand['live_fork']['cid'] and gcid not in (pcid,ccid)
    assert rpc('exec',c,'/bin/fork-probe cid').strip()==ccid
    for sid,value in ((p,20),(c,30),(g,40)):
        rpc('exec',sid,f'echo {value} > /run/probe-command')
        wait_exec(sid,'cat /run/probe-result 2>/dev/null || true',f'initial=10 value={value}')
        rpc('create',sid,'samecontainer',sid,grpc,value)
        wait_exec(sid,'cat /run/kuasar/state/samecontainer/rootfs/value 2>/dev/null || true',str(value))
        assert 'RUNNING' in rpc('state',sid,'samecontainer')
    assert not list(out.rglob('memory.img'))
    time.sleep(1.1)
    for sid,value in ((p,20),(c,30),(g,40)):
        lines=rpc('exec',sid,'cat /run/kuasar/state/samecontainer/rootfs/heartbeat').splitlines()
        assert len(lines)>=2 and set(lines)=={str(value)},lines
    rpc('delete',g,'samecontainer')
    assert rpc('exec',g,'/bin/fork-probe cid').strip()==gcid
    call('sandbox-destroy',sandbox_id=g)
    assert 'RUNNING' in rpc('state',c,'samecontainer')
    rpc('delete',p,'samecontainer')
    assert rpc('exec',p,'/bin/fork-probe cid').strip()==pcid
    assert 'RUNNING' in rpc('state',c,'samecontainer')
    call('sandbox-destroy',sandbox_id=p)
    assert 'RUNNING' in rpc('state',c,'samecontainer')
    assert rpc('exec',c,'cat /run/probe-result').strip()=='initial=10 value=30'
    rpc('delete',c,'samecontainer')
    assert rpc('exec',c,'/bin/fork-probe cid').strip()==ccid
    call('sandbox-destroy',sandbox_id=c)
    assert call('sandbox-list')['sandboxes']==[]
    assert not list(out.rglob('memory.img'))
    pss=re.findall(r'combined_ram_pss_kib=(\d+) baseline_kib=(\d+)',(out/'host.log').read_text())
    # First fork has exactly two sharers. Later forks include a live ancestor:
    # two-of-three PSS is greater than one-of-two without any page copies.
    assert len(pss)==2 and pss[0][0]==pss[0][1],pss
    for guest_log in out.glob('*.serial.log'):
        contents=guest_log.read_text(errors='replace')
        assert 'fatal runtime error' not in contents and 'Kernel panic' not in contents,contents[-3000:]
    result=dict(result='PASS',parent_cid=pcid,child_cid=ccid,grandchild_cid=gcid,fork_ms=elapsed,pss=pss,
        old_connection=oldout.strip(),checks=['normal Kuasar Controller Update and agent Task Create/Start/State/Kill/Wait/Delete','same container ID in both guests','parent-first and descendant-first sandbox destruction','guest observed distinct CIDs','inherited 32MiB probe 10 -> 20/30','no memory.img; first two-process fork boundary PSS unchanged'])
    (out/'results.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
finally:
    stop_logs.set()
    for thread in log_threads:thread.join(timeout=1)
    try:os.killpg(proc.pid,signal.SIGKILL)
    except ProcessLookupError:pass
    proc.wait();log.close()
