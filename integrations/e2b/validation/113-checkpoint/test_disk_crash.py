"""Owned VMM crash after guest fsync; never kills another service/process."""
import hashlib,json,os,shlex,signal,subprocess,sys,time,uuid,urllib.request
from pathlib import Path
B=Path('/home/hong/src/research/fork-features-113');F=Path('/home/hong/src/research/python-latest-113');P=F/'deploy/fork'
sys.path.insert(0,str(F/'src/kuasar-fork/kuasar-io/kuasar/integrations/e2b'));import vm_rpc
os.environ.update(E2B_API_KEY=(P/'api-key').read_text().strip(),E2B_API_URL='http://127.0.0.1:18887',E2B_SANDBOX_URL='http://127.0.0.1:18887',NO_PROXY='*',no_proxy='*')
from e2b import Sandbox
owned={};result={};ledger=B/'results/disk-crash-owned.json'
def save():ledger.write_text(json.dumps(list(owned)))
def cmd(s,c):
 r=s.commands.run(c,user='root',timeout=90);assert r.exit_code==0,(r.exit_code,r.stderr);return r.stdout.strip()
def fork(s):
 req=urllib.request.Request(os.environ['E2B_API_URL']+'/sandboxes/'+s.sandbox_id+'/fork',data=b'{"timeout":600}',headers={'X-API-Key':os.environ['E2B_API_KEY'],'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=180) as response:sid=json.load(response)[0]['sandbox']['sandboxID']
 child=Sandbox.connect(sid,timeout=600,request_timeout=300);owned[sid]=child;save();return child
try:
 parent=Sandbox.create('base',timeout=600,request_timeout=300);owned[parent.sandbox_id]=parent;save()
 cmd(parent,'echo parent-alive > /tmp/parent-proof');child=fork(parent)
 marker=uuid.uuid4().hex
 code="import os; f=open('/tmp/durable-crash-proof','wb'); f.write("+repr(marker.encode())+"); f.flush(); os.fsync(f.fileno()); f.close(); d=os.open('/tmp',os.O_RDONLY|os.O_DIRECTORY); os.fsync(d); os.close(d)"
 cmd(child,'python3 -c '+shlex.quote(code))
 info=vm_rpc.admin(P/'admin.sock','sandbox-get',sandbox_id=child.sandbox_id)
 pid=info['live_fork']['pid'];base=Path(info['base_dir']);assert child.sandbox_id in str(base)
 # Keep all chain names alive through owned cleanup; a hard link changes no data.
 kept=F/'data/fork'/('crash-proof-'+uuid.uuid4().hex);kept.mkdir(mode=0o700)
 for p in (base/'fork-disks').iterdir():
  assert p.is_file() and not p.is_symlink();os.link(p,kept/p.name)
 from image_pool import UnixHTTP
 http=UnixHTTP(info['live_fork']['api_socket']);http.request('GET','/api/v1/vm.info');response=http.getresponse();assert response.status==200;config=json.loads(response.read())['config'];http.close()
 disk=next(d for d in config['disks'] if not d['readonly']);leaf=kept/Path(disk['path']).name
 # Verify process identity against this sandbox before the deliberate crash.
 assert Path(f'/proc/{pid}/exe').resolve()==(P/'bin/cloud-hypervisor').resolve()
 assert pid!=vm_rpc.admin(P/'admin.sock','sandbox-get',sandbox_id=parent.sandbox_id)['live_fork']['pid']
 assert any(os.readlink(p)==disk['path'] for p in Path(f'/proc/{pid}/fd').iterdir())
 os.kill(pid,signal.SIGKILL)
 for _ in range(100):
  if not Path(f'/proc/{pid}').exists():break
  time.sleep(.02)
 # Shared immutable ancestors may still be locked by the live parent.
 export=kept/'export';export.mkdir()
 for p in kept.iterdir():
  if p.is_file():subprocess.run(['cp','--reflink=always','--',str(p),str(export/p.name)],check=True,capture_output=True)
 raw=kept/'verify.raw';conversion=subprocess.run(['qemu-img','convert','-O','raw',str(export/leaf.name),str(raw)],capture_output=True,text=True)
 assert conversion.returncode==0,conversion.stderr
 check=subprocess.run(['e2fsck','-p',str(raw)],capture_output=True,text=True);assert check.returncode in (0,1),check.stdout+check.stderr
 dumped=kept/'proof';subprocess.run(['debugfs','-R',f'dump /upper/tmp/durable-crash-proof {dumped}',str(raw)],check=True,capture_output=True)
 assert dumped.read_bytes()==marker.encode()
 assert cmd(parent,'cat /tmp/parent-proof')=='parent-alive'
 assert cmd(parent,'test ! -e /tmp/durable-crash-proof; echo isolated')=='isolated'
 result.update(success=True,fsync_survived_owned_vmm_sigkill=True,parent_continued=True,independent_writes=True,fsck_rc=check.returncode,sha256=hashlib.sha256(dumped.read_bytes()).hexdigest(),evidence=str(kept))
finally:
 for sid,s in list(owned.items()):
  try:s.kill();owned.pop(sid);save()
  except Exception as e:result.setdefault('cleanup_errors',[]).append(str(e))
 result['owned_remaining']=list(owned);(B/'results/disk-crash.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
