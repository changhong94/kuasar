"""Owned VMM crash after guest fsync; never kills another service/process."""
import hashlib,json,os,shlex,signal,subprocess,sys,time,uuid,urllib.request
from pathlib import Path
B=Path('/home/hong/src/research/fork-features-113');F=Path('/home/hong/src/research/python-latest-113');P=F/'deploy/fork'
sys.path.insert(0,str(F/'src/kuasar-fork/kuasar-io/kuasar/integrations/e2b'));import vm_rpc
os.environ.update(E2B_API_KEY=(P/'api-key').read_text().strip(),E2B_API_URL='http://127.0.0.1:18887',E2B_SANDBOX_URL='http://127.0.0.1:18887',NO_PROXY='*',no_proxy='*')
from e2b import Sandbox
owned={};result={};ledger=B/'results/languages-owned.json'
def save():ledger.write_text(json.dumps(list(owned)))
def cmd(s,c):
 r=s.commands.run(c,user='root',timeout=90);assert r.exit_code==0,(r.exit_code,r.stderr);return r.stdout.strip()
def fork(s):
 req=urllib.request.Request(os.environ['E2B_API_URL']+'/sandboxes/'+s.sandbox_id+'/fork',data=b'{"timeout":600}',headers={'X-API-Key':os.environ['E2B_API_KEY'],'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=180) as response:sid=json.load(response)[0]['sandbox']['sandboxID']
 child=Sandbox.connect(sid,timeout=600,request_timeout=300);owned[sid]=child;save();return child
rows=[]
try:
 for template,command in [('node',"node -e 'console.log(process.argv[1])'"),('ruby',"ruby -e 'puts ARGV[0]'")]:
  parent=Sandbox.create(template,timeout=600,request_timeout=300);owned[parent.sandbox_id]=parent;save();marker=uuid.uuid4().hex
  assert cmd(parent,command+' '+marker)==marker
  parent.files.write('/tmp/language-proof',marker.encode());child=fork(parent)
  parent.files.write('/tmp/language-proof',b'parent-changed')
  parent.pause(request_timeout=300);parent.connect(timeout=600,request_timeout=300)
  assert parent.files.read('/tmp/language-proof',format='bytes')==b'parent-changed'
  assert child.files.read('/tmp/language-proof',format='bytes')==marker.encode()
  child.pause(request_timeout=300);assert not child.is_running();parent.kill();owned.pop(parent.sandbox_id);save()
  child.connect(timeout=600,request_timeout=300)
  assert child.files.read('/tmp/language-proof',format='bytes')==marker.encode()
  assert cmd(child,command+' '+marker)==marker
  child.kill();owned.pop(child.sandbox_id);save();rows.append({'template':template,'fork_pause_resume_print_file_and_independent_destroy':True})
finally:
 for sid,s in list(owned.items()):
  try:s.kill();owned.pop(sid);save()
  except Exception:pass
 (B/'results/checkpoint-languages.json').write_text(json.dumps({'rows':rows,'owned_remaining':list(owned)},indent=2))
print(json.dumps(rows))
