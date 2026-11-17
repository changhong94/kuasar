"""Real E2B/guest continuation check. Owns only IDs recorded in this run."""
import hashlib,json,os,shlex,sys,time,uuid,urllib.request
from pathlib import Path
B=Path('/home/hong/src/research/fork-features-113')
Q=Path('/home/hong/src/research/python-latest-113/src/kuasar-fork/kuasar-io/kuasar')
sys.path.insert(0,str(Q/'integrations/e2b'));import vm_rpc
P=Path('/home/hong/src/research/python-latest-113/deploy/fork')
os.environ.update(E2B_API_KEY=(P/'api-key').read_text().strip(),E2B_API_URL='http://127.0.0.1:18887',E2B_SANDBOX_URL='http://127.0.0.1:18887',NO_PROXY='*',no_proxy='*')
from e2b import Sandbox
owned={};results={'cycles':[]};ledger=B/'results/checkpoint-guest-owned.json'
def save():ledger.write_text(json.dumps(list(owned)))
def cmd(s,c):
 r=s.commands.run(c,user='root',timeout=90)
 assert r.exit_code==0,(c,r.exit_code,r.stderr)
 return r.stdout.strip()
def admin(sid):return vm_rpc.admin(P/'admin.sock','sandbox-get',sandbox_id=sid)
def memory(sid):
 pid=admin(sid)['live_fork']['pid'];r={'pid':pid}
 for line in Path(f'/proc/{pid}/status').read_text().splitlines():
  if line.split(':')[0] in ['VmSize','VmRSS','VmData','Threads']:r[line.split(':')[0]]=int(line.split()[1])
 return r
program='''import os,secrets,time
secret=secrets.token_hex(24)
counter=0
os.mkfifo('/tmp/checkpoint-fifo')
open('/tmp/checkpoint-initial','w').write(secret)
while True:
 with open('/tmp/checkpoint-fifo') as f: challenge=f.read().strip()
 counter+=1
 with open('/tmp/checkpoint-response','w') as f:f.write(f'{secret}:{challenge}:{counter}:{os.getpid()}')
'''
def challenge(s,secret):
 nonce=uuid.uuid4().hex
 cmd(s,'printf %s '+shlex.quote(nonce)+' > /tmp/checkpoint-fifo')
 for _ in range(200):
  value=cmd(s,'cat /tmp/checkpoint-response 2>/dev/null || true')
  if value.startswith(secret+':'+nonce+':'):return value
  time.sleep(.01)
 raise AssertionError('in-memory process did not answer fresh challenge')
def pause(s):
 before=memory(s.sandbox_id);t=time.monotonic();s.pause(request_timeout=300);elapsed=(time.monotonic()-t)*1000;after=memory(s.sandbox_id)
 assert before['VmSize']-after['VmSize']>1024*1024,(before,after)
 assert not s.is_running(), 'paused SDK health remained running'
 try:s.commands.run('true',user='root',timeout=5)
 except Exception:pass
 else:raise AssertionError('paused guest accepted exec')
 info=admin(s.sandbox_id);cp=Path(info['base_dir'])/'durable-checkpoint'
 assert (cp/'checkpoint.json').exists()
 row={'id':s.sandbox_id,'pause_ms':elapsed,'before':before,'paused':after,'checkpoint_bytes':sum(p.stat().st_size for p in cp.iterdir() if p.is_file())}
 results['cycles'].append(row);return row
try:
 parent=Sandbox.create('base',timeout=600,request_timeout=300);owned[parent.sandbox_id]=parent;save()
 cmd(parent,'printf %s '+shlex.quote(program)+' > /tmp/checkpoint-process.py')
 pid=int(cmd(parent,'nohup /usr/local/bin/python3 /tmp/checkpoint-process.py > /tmp/checkpoint-process.log 2>&1 < /dev/null & echo $!'))
 for _ in range(200):
  secret=cmd(parent,'cat /tmp/checkpoint-initial 2>/dev/null || true')
  if secret:break
  time.sleep(.01)
 assert secret
 original=challenge(parent,secret);assert original.endswith(':'+str(pid))
 results['guest_mountinfo']=cmd(parent,'cat /proc/self/mountinfo')
 assert 'upperdir=/run/image-writes/upper' in results['guest_mountinfo']
 info=admin(parent.sandbox_id)
 results['filesystem_backing']='qcow2-ext4'
 assert list((Path(info['base_dir'])/'fork-disks').glob('*.qcow2'))
 cmd(parent,"python3 -c \"import os; f=open('/tmp/fsync-proof','wb'); f.write(b'guest-fsync-proof'); f.flush(); os.fsync(f.fileno()); f.close(); d=os.open('/tmp',os.O_RDONLY|os.O_DIRECTORY); os.fsync(d); os.close(d)\"")
 data=b'checkpoint-data-'+uuid.uuid4().hex.encode();parent.files.write('/tmp/checkpoint-data',data)
 req=urllib.request.Request(os.environ['E2B_API_URL']+'/sandboxes/'+parent.sandbox_id+'/fork',data=json.dumps({'timeout':600}).encode(),headers={'X-API-Key':os.environ['E2B_API_KEY'],'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=180) as response:sid=json.load(response)[0]['sandbox']['sandboxID']
 child=Sandbox.connect(sid,timeout=600,request_timeout=300);owned[sid]=child;save()
 assert challenge(child,secret).endswith(':'+str(pid))
 parent.files.write('/tmp/checkpoint-data',b'parent-independent')
 assert child.files.read('/tmp/checkpoint-data',format='bytes')==data
 assert child.files.read('/tmp/fsync-proof',format='bytes')==b'guest-fsync-proof'
 for cycle in range(3):
  row=pause(child)
  if cycle==1:
   parent.kill();owned.pop(parent.sandbox_id);save();results['parent_destroyed_while_child_paused']=True
  t=time.monotonic();child.connect(timeout=600,request_timeout=300);row['resume_ms']=(time.monotonic()-t)*1000
  assert child.is_running()
  row['fresh_process_answer']=challenge(child,secret)
  assert row['fresh_process_answer'].endswith(':'+str(pid))
  assert child.files.read('/tmp/checkpoint-data',format='bytes')==data
  (B/'results/checkpoint-storage-guest.json').write_text(json.dumps(results,indent=2))
 pause(child);child.kill();owned.pop(sid);save();results.update(success=True,paused_destroy_passed=True)
finally:
 for sid,s in list(owned.items()):
  try:s.kill();owned.pop(sid);save()
  except Exception as e:results.setdefault('cleanup_errors',[]).append(str(e))
 (B/'results/checkpoint-storage-guest.json').write_text(json.dumps(results,indent=2))
print(json.dumps(results,indent=2))
