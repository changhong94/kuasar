"""Owned VMM crash after guest fsync; never kills another service/process."""
import hashlib,json,os,shlex,signal,subprocess,sys,time,uuid,urllib.request
from pathlib import Path
B=Path('/home/hong/src/research/fork-features-113');F=Path('/home/hong/src/research/python-latest-113');P=F/'deploy/fork'
sys.path.insert(0,str(F/'src/kuasar-fork/kuasar-io/kuasar/integrations/e2b'));import vm_rpc
os.environ.update(E2B_API_KEY=(P/'api-key').read_text().strip(),E2B_API_URL='http://127.0.0.1:18887',E2B_SANDBOX_URL='http://127.0.0.1:18887',NO_PROXY='*',no_proxy='*')
from e2b import Sandbox
owned={};result={};ledger=B/'results/checkpoint-metrics-owned.json'
def save():ledger.write_text(json.dumps(list(owned)))
def cmd(s,c):
 r=s.commands.run(c,user='root',timeout=90);assert r.exit_code==0,(r.exit_code,r.stderr);return r.stdout.strip()
def fork(s):
 req=urllib.request.Request(os.environ['E2B_API_URL']+'/sandboxes/'+s.sandbox_id+'/fork',data=b'{"timeout":600}',headers={'X-API-Key':os.environ['E2B_API_KEY'],'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=180) as response:sid=json.load(response)[0]['sandbox']['sandboxID']
 child=Sandbox.connect(sid,timeout=600,request_timeout=300);owned[sid]=child;save();return child
rows=[]
def resources(s):
 info=vm_rpc.admin(P/'admin.sock','sandbox-get',sandbox_id=s.sandbox_id);base=Path(info['base_dir']);pid=info['live_fork']['pid'];out={}
 for line in Path(f'/proc/{pid}/status').read_text().splitlines():
  if line.split(':')[0] in ['VmSize','VmRSS','VmData','Threads']:out[line.split(':')[0]]=int(line.split()[1])
 for line in Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines():
  if line.startswith('Pss:'):out['Pss_kib']=int(line.split()[1])
 out['owned_directory_du_bytes']=int(subprocess.check_output(['du','-s','-B1',str(base)],text=True).split()[0]);return out
def print_ready(s):
 marker=uuid.uuid4().hex;received=None;chunks=[]
 def output(value):
  nonlocal received
  chunks.append(value)
  if received is None and marker+'\n' in ''.join(chunks):received=time.monotonic()
 r=s.commands.run('exec /usr/local/bin/python3 -c '+shlex.quote('import sys; print(sys.argv[1], flush=True)')+' '+marker,user='root',timeout=90,on_stdout=output)
 assert r.exit_code==0 and received is not None;return received
io="""import hashlib,time,os,json
h=hashlib.sha256();t=time.monotonic()
with open('/tmp/fiveway.bin','wb') as f:
 for i in range(32):
  b=hashlib.shake_256(('fiveway'+str(i)).encode()).digest(1048576);h.update(b);f.write(b)
 f.flush();os.fsync(f.fileno())
print(json.dumps([h.hexdigest(),(time.monotonic()-t)*1000]))"""
try:
 for i in range(5):
  t=time.monotonic();s=Sandbox.create('base',timeout=600,request_timeout=300);owned[s.sandbox_id]=s;save();api=time.monotonic();printed=print_ready(s)
  row={'round':i+1,'api_ms':(api-t)*1000,'python_print_ms':(printed-t)*1000,'live':resources(s)}
  row['io']=json.loads(cmd(s,'python3 -c '+shlex.quote(io)));row['written']=resources(s)
  t=time.monotonic();s.pause(request_timeout=300);row['pause_ms']=(time.monotonic()-t)*1000;assert not s.is_running();row['paused']=resources(s)
  t=time.monotonic();s.connect(timeout=600,request_timeout=300);row['resume_api_ms']=(time.monotonic()-t)*1000;row['resume_print_ms']=(print_ready(s)-t)*1000
  assert cmd(s,'sha256sum /tmp/fiveway.bin').split()[0]==row['io'][0]
  t=time.monotonic();s.kill();row['delete_ms']=(time.monotonic()-t)*1000;owned.pop(s.sandbox_id);save();row['success']=True;rows.append(row)
  (B/'results/checkpoint-metrics.json').write_text(json.dumps({'rows':rows,'filesystem':'qcow2-ext4','timing':'client request through response/print; 32 MiB write + fsync','storage':'du allocated bytes, includes shared/reflink extents; not exclusive physical use'},indent=2))
finally:
 for sid,s in list(owned.items()):
  try:s.kill();owned.pop(sid);save()
  except Exception:pass
print(json.dumps(rows,indent=2))
