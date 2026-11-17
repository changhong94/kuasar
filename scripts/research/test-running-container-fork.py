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
    rpc('create',p,'samecontainer',p,grpc,10,'inherited')
    rpc('exec-create',p,'samecontainer',p,grpc,'execworker')
    root='/run/kuasar/state/samecontainer/rootfs'
    def sample(sid,worker):
        for _ in range(50):
            parts=rpc('exec',sid,f'cat {root}/{worker}-state').split()
            if len(parts)==3:return list(map(int,parts))
            time.sleep(.02)
        raise AssertionError(parts)
    original={w:sample(p,w) for w in ('init','execworker')}
    states={w:rpc('state',p,'samecontainer',*([w] if w!='init' else [])) for w in original}
    assert all('RUNNING' in x for x in states.values()),states
    starts={w:rpc('exec',p,"/bin/busybox awk '{print $22}' /proc/"+states[w].split('pid=')[1].strip()+"/stat").strip() for w in original}
    rpc('freeze',p,'test-gate')
    blocked=subprocess.run([client,'state',str(state/p/'task.vsock'),'samecontainer'],capture_output=True,text=True,timeout=20)
    assert blocked.returncode and 'FAILED_PRECONDITION' in blocked.stderr,blocked
    rpc('freeze',p,'test-gate')
    # An unrelated fork must fail without stealing the lease or leaving a child.
    with socket.socket(socket.AF_UNIX) as rejected:
        rejected.settimeout(20); rejected.connect(str(admin))
        rejected.sendall((json.dumps(dict(action='sandbox-fork',source_id=p,sandbox_id='rejected0001'))+'\n').encode())
        answer=json.loads(rejected.makefile().readline())
        assert not answer['ok'],answer
    assert not (state/'rejected0001').exists()
    rpc('thaw',p,'test-gate')
    hold=subprocess.Popen([client,'hold',str(state/p/'task.vsock')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    assert hold.stdout.readline().strip()=='CONNECTED'
    waiter=subprocess.Popen([client,'wait',str(state/p/'task.vsock'),'samecontainer'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    assert waiter.stdout.readline().strip()=='WAITING'
    time.sleep(.1)
    start=time.monotonic();child=call('sandbox-fork',source_id=p,sandbox_id=c);elapsed=(time.monotonic()-start)*1000
    oldout,olderr=hold.communicate('\n',timeout=10);assert hold.returncode==0,(oldout,olderr)
    waitout,waiterr=waiter.communicate(timeout=10);assert waiter.returncode==0 and 'Err(' in waitout,(waitout,waiterr)
    grand=call('sandbox-fork',source_id=c,sandbox_id=g)
    cids=[];observed={}
    for sid,base in ((p,20),(c,30),(g,40)):
        cids.append(rpc('exec',sid,'/bin/fork-probe cid').strip())
        metadata=json.loads((state/sid/'sandbox.json').read_text())
        (out/(sid+'.metadata.json')).write_text(json.dumps(metadata,indent=2))
        assert 'samecontainer' in metadata['containers'],metadata.keys()
        container=metadata['containers']['samecontainer']
        assert len(container['processes'])==1,container
        assert str(state/sid) in container['data']['bundle'],container
        if sid!=p:assert metadata['data']['labels']['io.kuasar.fork.event-namespace']==f'kuasar-fork-{sid}'
        observed[sid]={}
        for i,w in enumerate(original):
            assert rpc('state',sid,'samecontainer',*([w] if w!='init' else []))==states[w]
            assert rpc('exec',sid,"/bin/busybox awk '{print $22}' /proc/"+states[w].split('pid=')[1].strip()+"/stat").strip()==starts[w]
            before=sample(sid,w)
            assert before[1]==10 and before[2]==original[w][2] and before[0]>=original[w][0],before
            assert rpc('exec',sid,f'cat {root}/{w}-starts').strip()=='1'
            rpc('exec',sid,f'echo {base+i} > {root}/{w}-command')
            for _ in range(100):
                after=sample(sid,w)
                if after[1]==base+i:break
                time.sleep(.05)
            assert after[1]==base+i and after[0]>before[0] and after[2]==before[2],after
            observed[sid][w]=dict(before=before,after=after,state=states[w])
    assert len(set(cids))==3,cids
    for sid,base in ((p,20),(c,30),(g,40)):
        for i,w in enumerate(original):
            history=rpc('exec',sid,f'cat {root}/{w}-history').splitlines()
            values=[int(line.split()[1]) for line in history]
            assert values[-1]==base+i and set(values)<={10,base+i},values
    
    assert not list(out.rglob('memory.img'))
    # Kill an inherited exec independently before deleting its container.
    rpc('delete',g,'samecontainer','execworker')
    for sid in (p,c):assert 'RUNNING' in rpc('state',sid,'samecontainer','execworker')
    rpc('delete',g,'samecontainer')
    # Guest Task Delete precedes Controller removal: do not fork this mismatch.
    with socket.socket(socket.AF_UNIX) as rejected:
        rejected.settimeout(20); rejected.connect(str(admin))
        rejected.sendall((json.dumps(dict(action='sandbox-fork',source_id=g,sandbox_id='mismatch0001'))+'\n').encode())
        answer=json.loads(rejected.makefile().readline())
        assert not answer['ok'] and 'inventory differ' in answer['error'],answer
    assert not (state/'mismatch0001').exists()
    rpc('freeze',g,'after-mismatch')
    rpc('thaw',g,'after-mismatch')
    rpc('forget',g,g,grpc)
    assert json.loads((state/g/'sandbox.json').read_text())['containers']=={}
    assert not Path(json.loads((out/(g+'.metadata.json')).read_text())['containers']['samecontainer']['data']['bundle']).exists()
    for sid in (p,c):
        assert Path(json.loads((out/(sid+'.metadata.json')).read_text())['containers']['samecontainer']['data']['bundle']).exists()
    assert rpc('exec',g,'/bin/fork-probe cid').strip()==cids[2]
    call('sandbox-destroy',sandbox_id=g)
    assert 'RUNNING' in rpc('state',p,'samecontainer')
    call('sandbox-destroy',sandbox_id=p)
    for i,w in enumerate(original):
        assert 'RUNNING' in rpc('state',c,'samecontainer',*([w] if w!='init' else []))
        later=sample(c,w)
        assert later[1]==30+i and later[0]>observed[c][w]['after'][0]
    rpc('delete',c,'samecontainer','execworker')
    rpc('delete',c,'samecontainer')
    rpc('forget',c,c,grpc)
    assert json.loads((state/c/'sandbox.json').read_text())['containers']=={}
    assert rpc('exec',c,'/bin/fork-probe cid').strip()==cids[1]
    call('sandbox-destroy',sandbox_id=c)
    assert call('sandbox-list')['sandboxes']==[]
    assert not list(out.rglob('memory.img'))
    pss=re.findall(r'combined_ram_pss_kib=(\d+) baseline_kib=(\d+)',(out/'host.log').read_text())
    assert len(pss)==2 and pss[0][0]==pss[0][1],pss
    for guest_log in out.glob('*.serial.log'):
        contents=guest_log.read_text(errors='replace')
        assert 'fatal runtime error' not in contents and 'Kernel panic' not in contents,contents[-3000:]
    result=dict(result='PASS',cids=cids,fork_ms=elapsed,pss=pss,observed=observed,
        old_connection=oldout.strip(),pending_wait=waitout.strip(),process_start_times=starts,checks=['Create/Start only in source; inherited init and exec PID and startup nonce/count preserved',
        'each process checks all 16 MiB before writing branch-specific values',
        'host container/exec metadata and bundle paths inherited independently','Task lifecycle barrier rejects requests; conflicting/inconsistent forks roll back and thaw; pending Wait does not block fork',
        'Controller Update removes only child metadata; inherited open file writes remain isolated',
        'descendant-first and parent-first destruction; surviving inherited processes continue',
        'no memory.img; first two-process fork boundary PSS unchanged'])
    (out/'results.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))

finally:
    stop_logs.set()
    for thread in log_threads:thread.join(timeout=1)
    try:os.killpg(proc.pid,signal.SIGKILL)
    except ProcessLookupError:pass
    proc.wait();log.close()
