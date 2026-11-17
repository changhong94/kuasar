#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Bounded disk chains through real guest, CRI/kubelet and optional E2B SDK."""
import concurrent.futures, hashlib, json, os, random, re, socket, struct, subprocess, sys, time
from pathlib import Path
from writable_disk_checks import DiskChecks, digest_file

out=Path(sys.argv[1]); repo=Path(__file__).resolve().parents[2]
k3s=repo.parents[1]/'tools/k3s-v1.31'
count=int(os.environ.get('FORK_GENERATIONS','24')); assert count>=12
sdk=bool(os.environ.get('FORK_E2B'))
timings=[]; instances=[]; immutable={}; samples=[]; deletion=[]; known_dirs=set()

def measure(op, fn):
 start=time.perf_counter();start_unix=time.time()
 try:return fn()
 finally:
  timings.append({'operation':op,'seconds':time.perf_counter()-start,'start_unix':start_unix,'end_unix':time.time()})
  (out/'multigen-timings.json').write_text(json.dumps(timings,indent=2))

def kubectl(*args, data=None):
 result=subprocess.run([str(k3s),'kubectl','--kubeconfig',str(out/'kubeconfig'),*map(str,args)],input=data,capture_output=True,text=True,timeout=150)
 assert result.returncode==0,(args,result.stdout,result.stderr)
 return result.stdout

def admin(action,**kw):
 with socket.socket(socket.AF_UNIX) as s:
  s.settimeout(120);s.connect(str(out/'admin.sock'));s.sendall((json.dumps(dict(action=action,**kw))+'\n').encode());r=json.loads(s.makefile().readline())
 assert r['ok'],r;return r

if sdk:
 from e2b import Sandbox
 options=dict(api_key=(out/'e2b-key').read_text().strip(),api_url='http://127.0.0.1:18881',sandbox_url='http://127.0.0.1:18881',request_timeout=150)

def create(parent=None):
 if sdk:
  box=measure('sdk.fork' if parent else 'sdk.create',lambda:parent['box'].fork(count=1,timeout=3600)[0] if parent else Sandbox.create(timeout=3600,**options))
  if isinstance(box,Exception):raise box
  name=box.sandbox_id
 else:
  name='fork-generation-'+str(len(instances))
  source=parent['uid'] if parent else 'seed00000001'
  obj={'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'annotations':{'io.kuasar.fork.source':source,'io.kuasar.fork.allow':'true'}},'spec':{'serviceAccountName':'fork-test','nodeName':'fork-research','runtimeClassName':'kuasar-fork','automountServiceAccountToken':False,'enableServiceLinks':False,'restartPolicy':'Never','terminationGracePeriodSeconds':2,'tolerations':[{'operator':'Exists'}],'containers':[{'name':'samecontainer','image':'docker.io/library/fork-workload:latest','imagePullPolicy':'Never','command':['/bin/fork-workload','init']}]}}
  def boot():
   kubectl('create','-f','-',data=json.dumps(obj));kubectl('wait','--for=condition=Ready','pod/'+name,'--timeout=120s')
  measure('kubernetes.fork',boot);box=None
 info=json.loads(kubectl('get','pod',name,'-o','json'))
 entry={'name':name,'box':box,'uid':info['metadata']['uid'],'cid':info['status']['containerStatuses'][0]['containerID'],'ip':info['status']['podIP'],'alive':True,'lineage':parent['lineage'] if parent else ''}
 instances.append(entry)
 entry['dir']=checks.state_dir(name);known_dirs.add(entry['dir'])
 return entry

def execute(entry,command):
 if sdk:
  return measure('sdk.exec',lambda:entry['box'].commands.run(command,user='root').stdout)
 return measure('kubectl.exec',lambda:kubectl('exec',entry['name'],'--','/bin/sh','-c',command))

def write(entry,path,value):
 if sdk:entry['box'].files.write(path,value,user='root')
 else:execute(entry,f"printf '%s' '{value}' > {path}")

def read(entry,path):
 if sdk:return entry['box'].files.read(path,user='root')
 return execute(entry,'/bin/busybox cat '+path)

def destroy(entry):
 if sdk:entry['box'].kill()
 else:kubectl('delete','pod',entry['name'],'--wait=true','--timeout=90s')
 entry['alive']=False;deletion.append(entry['name'])

def inspect(entry):
 folder=entry['dir']/'fork-disks'; all_paths=list(folder.iterdir())
 pending=[p for p in all_paths if p.name.startswith('.merge-')]
 paths=[p for p in all_paths if p not in pending]
 assert len(pending)<=1 and len(paths)<=4,(folder,all_paths)
 leaves=[p for p in paths if p.stat().st_mode & 0o222]
 assert len(leaves)==1,paths
 chain=[];p=leaves[0]
 while True:
  assert p not in chain and p.exists(),chain
  chain.append(p)
  with p.open('rb') as f:
   header=f.read(32)
   if header[:4]!=b'QFI\xfb':break
   offset,length=struct.unpack('>QI',header[8:20]);f.seek(offset);backing=f.read(length).decode()
  assert Path(backing).name==backing and backing not in ('.','..')
  p=folder/backing
 assert len(chain)<=4 and set(paths)==set(chain),(paths,chain)
 for p in paths:
  st=p.stat()
  if not st.st_mode & 0o222 and st.st_ino not in immutable:
   immutable[st.st_ino]={'hash':digest_file(p),'path':str(p),'allocated':st.st_blocks*512}
 samples.append({'pod':entry['name'],'chain_files':len(chain),'directory_files':len(all_paths),'pending_candidates':len(pending),'allocated_bytes':sum(p.stat().st_blocks*512 for p in paths)})
 (out/'chain-samples.json').write_text(json.dumps(samples,indent=2))

def verify(entry):
 checks.drop_caches(entry['name'])
 assert read(entry,'/tmp/lineage')==entry['lineage']
 assert execute(entry,'/bin/busybox sha256sum /tmp/ancestor').split()[0]==ancestor
 before=execute(entry,'/bin/busybox cat /init-state').split()
 assert before[2]==nonce
 if os.environ.get('FORK_IO_STRESS'):
  time.sleep(.12)
  after=execute(entry,'/bin/busybox cat /init-state').split()
  assert int(after[0])>int(before[0]),('direct IO workload stalled or failed',entry['name'],before,after)
 inspect(entry)

checks=DiskChecks(out,lambda name,cmd:execute(next(e for e in instances if e['name']==name),cmd))
try:
 current=create()
 mounts=execute(current,'/bin/busybox cat /proc/mounts')
 assert any(line.split()[1]=='/' and line.split()[2]=='ext4' for line in mounts.splitlines()),mounts
 execute(current,'/bin/busybox dd if=/dev/urandom of=/tmp/ancestor bs=65536 count=32 2>/dev/null; /bin/busybox sync')
 ancestor=execute(current,'/bin/busybox sha256sum /tmp/ancestor').split()[0]
 nonce=execute(current,'/bin/busybox cat /init-state').split()[2]
 if os.environ.get('FORK_IO_STRESS'):
  write(current,'/enable-disk-probe','1')
 current['lineage']='generation-0';write(current,'/tmp/lineage',current['lineage'])
 admin('sandbox-destroy',sandbox_id='seed00000001')
 inspect(current)
 if os.environ.get('FORK_FD_CENSUS'):
  from fd_census import capture
  capture(out,repo,current,0)
 for generation in range(1,count):
  parent=current
  # An older, independent branch writes while the newest branch is forking.
  if generation>=3:
   with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
    background=pool.submit(execute,instances[0],f'echo side-{generation} > /tmp/side; /bin/busybox sync')
    current=create(parent);background.result()
  else:current=create(parent)
  assert read(current,'/tmp/lineage')==parent['lineage']
  current['lineage']=parent['lineage']+'/'+str(generation)
  if os.environ.get('FORK_IO_STRESS'):write(current,'/init-command',str(1000+generation))
  write(current,'/tmp/lineage',current['lineage'])
  execute(current,f'echo {generation} > /tmp/private; /bin/busybox truncate -s {generation+17} /tmp/private; /bin/busybox sync')
  assert read(parent,'/tmp/lineage')==parent['lineage']
  verify(current);inspect(parent)
  if os.environ.get('FORK_FD_CENSUS'):
   capture(out,repo,current,generation)
   if generation % 25 == 0:capture(out,repo,instances[0],generation,'ancestor')
  if os.environ.get('FORK_RESTART_CONTAINERD'):
   ctr=[str(repo.parent/'containerd/bin/ctr'),'-a',str(out/'ctrd.sock'),'-n','k8s.io']
   if generation==3:
    subprocess.run(ctr+['tasks','exec','--null-io','--detach','--exec-id','restart-owned',current['cid'].split('://')[-1],'/bin/sh','-c','i=0; while :; do echo $i > /tmp/restart-probe; i=$((i+1)); /bin/busybox sleep 1; done'],check=True)
   if generation==4:
    (out/'restart-containerd.request').touch()
    deadline=time.monotonic()+40
    while not (out/'restart-containerd.done').exists() and time.monotonic()<deadline:time.sleep(.1)
    assert (out/'restart-containerd.done').exists(),'containerd restart did not finish'
    before_probe=int(read(current,'/tmp/restart-probe'));time.sleep(1.2)
    assert int(read(current,'/tmp/restart-probe'))>before_probe
    assert 'restart-owned' in (current['dir']/'sandbox.json').read_text()
   if generation==5:
    before_probe=int(read(current,'/tmp/restart-probe'));time.sleep(1.2)
    assert int(read(current,'/tmp/restart-probe'))>before_probe
    assert 'restart-owned' in (current['dir']/'sandbox.json').read_text()
    cid=current['cid'].split('://')[-1]
    subprocess.run(ctr+['tasks','kill','--signal','SIGKILL','--exec-id','restart-owned',cid],check=True)
    deleted=subprocess.run(ctr+['tasks','delete','--exec-id','restart-owned',cid])
    assert deleted.returncode==137,deleted.returncode # ctr returns the killed process's exit status
    assert 'restart-owned' not in (current['dir']/'sandbox.json').read_text()
    assert 'restart-owned' in (parent['dir']/'sandbox.json').read_text()
    before_probe=int(read(parent,'/tmp/restart-probe'));time.sleep(1.2)
    assert int(read(parent,'/tmp/restart-probe'))>before_probe
    (out/'restart-inventory-verified.json').write_text(json.dumps({'result':'PASS','inherited_after_restart':True,'child_delete_isolated':True}))
  print('GENERATION',generation,'PASS',flush=True)
 # Shared immutable inodes must not have been changed by compaction or older VM writes.
 for entry in instances:
  verify(entry)
  for p in (entry['dir']/'fork-disks').iterdir():
   st=p.stat()
   if not p.name.startswith('.merge-') and not st.st_mode & 0o222:assert digest_file(p)==immutable[st.st_ino]['hash'],str(p)
 assert len({e['uid'] for e in instances})==count
 assert len({e['cid'] for e in instances})==count
 assert len({e['ip'] for e in instances})==count
 # Destroy both ancestors and descendants in a reproducible shuffled order,
 # retaining a middle generation to fork again after its own parent is reclaimed.
 survivor=instances[count//2];order=[e for e in instances if e is not survivor]
 random.Random(int(os.environ.get('FORK_DELETE_SEED','19'))).shuffle(order)
 for entry in order:
  destroy(entry)
  assert read(survivor,'/tmp/lineage')==survivor['lineage']
 deadline=time.monotonic()+150
 while any(e['dir'].exists() for e in order) and time.monotonic()<deadline:time.sleep(1)
 assert all(not e['dir'].exists() for e in order)
 verify(survivor)
 current=create(survivor)
 assert read(current,'/tmp/lineage')==survivor['lineage']
 current['lineage']+='/after-gc';write(current,'/tmp/lineage',current['lineage'])
 verify(current);verify(survivor)
 destroy(survivor);verify(current);destroy(current)
 deadline=time.monotonic()+150
 while admin('sandbox-list')['sandboxes'] and time.monotonic()<deadline:time.sleep(1)
 assert admin('sandbox-list')['sandboxes']==[]
 assert not list((out/'state').glob('*/fork-disks'))
 assert all(not (out/'ipam/fork-research'/e['ip']).exists() for e in instances)
 assert not list(out.rglob('memory.img'))
 log=(out/'host.log').read_text()
 pss=re.findall(r'combined_ram_pss_kib=(\d+) baseline_kib=(\d+)',log)
 assert len(pss)==count+1,pss
 shared=re.findall(r'ram_shared_present_pages=(\d+) mismatched_pages=0',log)
 assert len(shared)==count+1 and all(int(n)>0 for n in shared),shared
 merges=re.findall(r'disk_compact input_files=(\d+) clusters=(\d+) bytes_read=(\d+) zero_clusters=(\d+) elapsed_ms=([\d.]+)',log)
 assert len(merges)>=4,merges
 assert 'disk_gc deferred' not in log
 exec_limit=float(os.environ['MAX_EXEC_SECONDS']) if os.environ.get('MAX_EXEC_SECONDS') else None
 exec_slo_failures=[t for t in timings if 'exec' in t['operation'] and exec_limit is not None and t['seconds']>=exec_limit]
 if exec_slo_failures and not os.environ.get('FORK_REPORT_EXEC_SLO'):
  raise AssertionError(('exec latency SLO exceeded', exec_slo_failures))
 result={'result':'PASS','interface':'E2B SDK 2.64.0' if sdk else 'Kubernetes kubectl/CRI','generations':count,'forks':len(pss),'pss':pss,'identical_ram_pfn_pages':shared,'compactions':merges,'max_chain_files':max(s['chain_files'] for s in samples),'max_directory_files':max(s['directory_files'] for s in samples),'exec_samples':sum('exec' in t['operation'] for t in timings),'max_exec_seconds':max(t['seconds'] for t in timings if 'exec' in t['operation']),'deletion_order':deletion,'fork_after_ancestor_gc':True,'inherited_nonce':nonce,'ancestor_sha256':ancestor,'immutable_inodes_checked':len(immutable),'all_disk_directories_and_ipam_reclaimed':True,'direct_io_stress':bool(os.environ.get('FORK_IO_STRESS')),'pause_ms':list(map(float,re.findall(r'VM_FORK pause_ms=([\d.]+)',log))),'background_publications':log.count('disk_merge_publish'),'merge_wait_before_pause_ms':list(map(float,re.findall(r'disk_merge_wait_before_pause_ms=([\d.]+)',log)))}
 result['exec_slo']={'limit_seconds':exec_limit,'result':'FAIL' if exec_slo_failures else 'PASS','failures':exec_slo_failures}
 (out/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
finally:
 for entry in reversed(instances):
  if entry['alive']:
   try:destroy(entry)
   except Exception:pass
