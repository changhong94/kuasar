#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
import json,os,socket,subprocess,sys,time,signal,hashlib,tarfile,io,re,threading
from pathlib import Path
import fcntl
# The fixture owns fixed Kubernetes ports and the forkbr0 subnet.
fixture_lock=open("/tmp/kuasar-kubernetes-fork.lock", "a")
fcntl.flock(fixture_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
repo=Path(__file__).resolve().parents[2]; ct=repo.parent/'containerd'; k3s=repo.parents[1]/'tools/k3s-v1.31'
out=Path(sys.argv[1]).resolve();out.mkdir(exist_ok=True);state=out/'state';state.mkdir()
admin=out/'admin.sock';grpc=out/'sandboxer.sock';sock=out/'ctrd.sock'
(out/'config.toml').write_text(Path(sys.argv[2]).read_text())
env=os.environ.copy();env['KUASAR_CONTAINERD_TTRPC_ADDRESS']=str(out/'ctrd.sock.ttrpc');env['KUASAR_FORK_ADMIN_ADDRESS']=str(admin)
env['KUASAR_FORK_TEMPLATE_IDS']='seed00000001'
network=bool(os.environ.get('FORK_NETWORK'))
seedns='forkseed'+str(os.getpid())
cni_bin=Path('/var/lib/rancher/k3s/data/current/bin')
cni_config={'cniVersion':'1.0.0','name':'fork-research','type':'bridge','bridge':'forkbr0','isGateway':True,'ipMasq':False,'ipam':{'type':'host-local','dataDir':str(out/'ipam'),'subnet':'10.233.77.0/24','routes':[{'dst':'0.0.0.0/0'}]}}
processes=[];logs=[]
restart_stop=threading.Event();restart_thread=None
def launch(args,name):
 f=(out/name).open('w');logs.append(f);p=subprocess.Popen(list(map(str,args)),env=env,stdout=f,stderr=f,start_new_session=True);processes.append(p);return p

def run(args,timeout=60):
 r=subprocess.run(list(map(str,args)),env=env,capture_output=True,text=True,timeout=timeout)
 print('RUN',list(map(str,args)),r.returncode,r.stdout,r.stderr,flush=True)
 assert r.returncode==0,(r.stdout,r.stderr);return r.stdout

def call(action,**kw):
 with socket.socket(socket.AF_UNIX) as s:
  s.settimeout(90);s.connect(str(admin));s.sendall((json.dumps(dict(action=action,**kw))+'\n').encode());r=json.loads(s.makefile().readline())
 assert r['ok'],r;return r

def rpc(op,sid,*args):return run([repo/'target/debug/examples/fork-agent-client',op,state/sid/'task.vsock',*args])
native_timings=[]
def kubectl(*args,timeout=90):
 started=time.perf_counter()
 result=run([k3s,'kubectl','--kubeconfig',out/'kubeconfig',*args],timeout)
 if args and args[0]=='exec':
  elapsed=time.perf_counter()-started
  native_timings.append({'pod':args[1],'seconds':elapsed})
  (out/'native-timings.json').write_text(json.dumps(native_timings,indent=2))
  if os.environ.get('MAX_EXEC_SECONDS'):assert elapsed<float(os.environ['MAX_EXEC_SECONDS']),native_timings[-1]
 return result
def ctr(*args):return run([ct/'bin/ctr','-a',sock,'-n','k8s.io',*args])
try:
 if network:
  (out/'cni').mkdir(); (out/'cni/10-fork.conf').write_text(json.dumps(cni_config))
  run(['ip','netns','add',seedns])
  seed_env={**env,'CNI_COMMAND':'ADD','CNI_CONTAINERID':'seed00000001','CNI_NETNS':'/var/run/netns/'+seedns,'CNI_IFNAME':'eth0','CNI_PATH':str(cni_bin)}
  cni=subprocess.run([str(cni_bin/'bridge')],env=seed_env,input=json.dumps(cni_config),text=True,capture_output=True);assert cni.returncode==0,cni.stderr
 launch([os.environ.get('FORK_SANDBOXER_BIN',str(repo/'target/debug/cloud_hypervisor')),'--config',out/'config.toml','--dir',state,'--listen',grpc,'--admin-listen',admin],'host.log')
 (out/'containerd.toml').write_text(f'''version = 3
root = "{out}/ctrd-root"
state = "{out}/ctrd-state"
[grpc]
address = "{sock}"
[ttrpc]
address = "{out}/ctrd.sock.ttrpc"
[proxy_plugins.vmm]
type = "sandbox"
address = "{grpc}"
[plugins.'io.containerd.cri.v1.runtime']
disable_apparmor = true
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.kuasar-vmm]
runtime_type = "io.containerd.kuasar-vmm.v1"
sandboxer = "vmm"
io_type = "streaming"
''')
 if network:
  with (out/'containerd.toml').open('a') as config_file:config_file.write(f"\n[plugins.'io.containerd.cri.v1.runtime'.cni]\nbin_dir = \"{cni_bin}\"\nconf_dir = \"{out}/cni\"\n")
 containerd_args=[ct/'bin/containerd','--config',out/'containerd.toml']
 containerd_process=launch(containerd_args,'containerd.log')
 if os.environ.get('FORK_RESTART_CONTAINERD'):
  def restart_containerd():
   while not restart_stop.wait(.05):
    if not (out/'restart-containerd.request').exists():continue
    os.killpg(containerd_process.pid,signal.SIGTERM);containerd_process.wait(timeout=15)
    replacement=launch(containerd_args,'containerd-restarted.log')
    for _ in range(300):
     if restart_stop.wait(.1):return
     r=subprocess.run([str(ct/'bin/ctr'),'-a',str(sock),'version'],capture_output=True)
     if r.returncode==0:
      (out/'restart-containerd.done').write_text(json.dumps({'old_pid':containerd_process.pid,'new_pid':replacement.pid}));return
    (out/'restart-containerd.failed').write_text('containerd failed to become ready')
  restart_thread=threading.Thread(target=restart_containerd,daemon=True);restart_thread.start()
 for _ in range(100):
  if admin.exists() and sock.exists():break
  time.sleep(.1)
 seed='seed00000001';call('sandbox-boot',sandbox_id=seed,netns='/var/run/netns/'+seedns if network else '')
 if os.environ.get('FORK_WRITABLE_DISK'):
  rpc('exec',seed,'mkdir -p /tmp/fork-disk; mount -t ext4 /dev/vda /tmp/fork-disk && cp -a /run/kuasar/state/. /tmp/fork-disk/ && mount --move /tmp/fork-disk /run/kuasar/state')
 if network:rpc('exec',seed,'/bin/busybox touch /opt/container-rootfs/enable-http')
 try:rpc('create',seed,'samecontainer',seed,grpc,10,'inherited')
 except Exception:
  rpc('exec',seed,'/bin/busybox cat /proc/mounts; /bin/busybox ls -laR /run/kuasar/state/samecontainer; /bin/runc --debug --log /tmp/runc-debug.log create --bundle /run/kuasar/state/samecontainer diagnostic; /bin/busybox cat /tmp/runc-debug.log')
  raise
 rpc('exec-create',seed,'samecontainer',seed,grpc,'execworker')
 # Metadata-only OCI image: the validated template already owns its guest rootfs.
 blobs={}
 def blob(data):
  raw=json.dumps(data,separators=(',',':')).encode();digest=hashlib.sha256(raw).hexdigest();blobs['blobs/sha256/'+digest]=raw
  return {'digest':'sha256:'+digest,'size':len(raw)}
 config=blob({'architecture':'amd64','os':'linux','config':{'Cmd':['/bin/fork-workload','init']},'rootfs':{'type':'layers','diff_ids':[]}});config['mediaType']='application/vnd.oci.image.config.v1+json'
 manifest=blob({'schemaVersion':2,'mediaType':'application/vnd.oci.image.manifest.v1+json','config':config,'layers':[]});manifest.update(mediaType='application/vnd.oci.image.manifest.v1+json',annotations={'org.opencontainers.image.ref.name':'docker.io/library/fork-workload:latest'})
 blobs['oci-layout']=json.dumps({'imageLayoutVersion':'1.0.0'}).encode();blobs['index.json']=json.dumps({'schemaVersion':2,'manifests':[manifest]}).encode()
 with tarfile.open(out/'image.tar','w') as tar:
  for name,data in blobs.items():
   info=tarfile.TarInfo(name);info.size=len(data);tar.addfile(info,io.BytesIO(data))
 ctr('images','import','--no-unpack',out/'image.tar');ctr('images','label','docker.io/library/fork-workload:latest','io.cri-containerd.image=managed')
 launch([k3s,'server','--data-dir',out/'k3s','--write-kubeconfig',out/'kubeconfig','--https-listen-port','16443','--advertise-address','127.0.0.1','--bind-address','127.0.0.1','--node-name','fork-research','--container-runtime-endpoint','unix://'+str(sock),'--flannel-backend','none','--disable-network-policy','--disable-kube-proxy','--disable','traefik,servicelb,coredns,local-storage,metrics-server','--kubelet-arg','root-dir='+str(out/'kubelet'),'--kubelet-arg','fail-swap-on=false'],'k3s.log')
 for _ in range(180):
  if (out/'kubeconfig').exists():
   r=subprocess.run([str(k3s),'kubectl','--kubeconfig',str(out/'kubeconfig'),'get','nodes'],capture_output=True,text=True)
   if r.returncode==0 and 'fork-research' in r.stdout:break
  time.sleep(1)
 else:raise AssertionError('Kubernetes node did not register')
 kubectl('create','serviceaccount','fork-test')
 runtime={'apiVersion':'node.k8s.io/v1','kind':'RuntimeClass','metadata':{'name':'kuasar-fork'},'handler':'kuasar-vmm'}
 (out/'runtime.json').write_text(json.dumps(runtime));kubectl('apply','-f',out/'runtime.json')
 if os.environ.get('FORK_E2B'):
  import secrets
  (out/'e2b-key').write_text(secrets.token_urlsafe(32));(out/'e2b-key').chmod(0o600)
  launch(['python3',repo/'integrations/e2b/server.py','--backend','kubernetes','--k3s',k3s,'--kubeconfig',out/'kubeconfig','--source',seed,'--api-key-file',out/'e2b-key','--state',out/'e2b-state.json'],'e2b.log')
  time.sleep(.3)
  script='test-multigeneration-fork.py' if os.environ.get('FORK_GENERATIONS') else 'test-e2b-sdk.py'
  run([repo.parents[1]/'tools/e2b-venv/bin/python',repo/'scripts/research'/script,out],timeout=1800)
  raise SystemExit(0)
 if os.environ.get('FORK_GENERATIONS'):
  run(['python3',repo/'scripts/research/test-multigeneration-fork.py',out],timeout=1800)
  raise SystemExit(0)
 def pod(name,source):
  obj={'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'annotations':{'io.kuasar.fork.source':source,'io.kuasar.fork.allow':'true'}},'spec':{'serviceAccountName':'fork-test','nodeName':'fork-research','runtimeClassName':'kuasar-fork','hostNetwork':not network,'automountServiceAccountToken':False,'enableServiceLinks':False,'restartPolicy':'Never','terminationGracePeriodSeconds':2,'tolerations':[{'operator':'Exists'}],'containers':[{'name':'samecontainer','image':'docker.io/library/fork-workload:latest','imagePullPolicy':'Never','command':['/bin/fork-workload','init']}]}}
  path=out/(name+'.json');path.write_text(json.dumps(obj));kubectl('apply','-f',path);kubectl('wait','--for=condition=Ready','pod/'+name,'--timeout=120s',timeout=135)
  result=json.loads(kubectl('get','pod',name,'-o','json'));(out/(name+'.observed.json')).write_text(json.dumps(result,indent=2));return result
 parent=pod('fork-parent',seed);uid=parent['metadata']['uid']
 call('sandbox-destroy',sandbox_id=seed)
 held=None
 if network:
  parent_ip=parent['status']['podIP'];held=socket.create_connection((parent_ip,8080),timeout=5)
 disk_checks=None
 if os.environ.get('FORK_WRITABLE_DISK'):
  from writable_disk_checks import DiskChecks
  disk_checks=DiskChecks(out,lambda name,cmd:kubectl('exec',name,'--','/bin/sh','-c',cmd))
  disk_checks.seed('fork-parent')
 child=pod('fork-child',uid)
 if network:
  import urllib.request
  child_ip=child['status']['podIP']; assert parent_ip!=child_ip
  held.sendall(b'GET /init-state HTTP/1.0\r\n\r\n');assert b'200 OK' in held.recv(4096);held.close()
  macs=[re.search(r'link/ether ([0-9a-f:]+)',kubectl('exec',name,'--','/bin/busybox','ip','link','show','eth0')).group(1) for name in ('fork-parent','fork-child')]
  assert macs[0]!=macs[1]

  for ip in (parent_ip,child_ip):
   data=urllib.request.build_opener(urllib.request.ProxyHandler({})).open('http://'+ip+':8080/init-state',timeout=5).read().decode();assert len(data.split())==3

 assert uid!=child['metadata']['uid'];assert parent['status']['containerStatuses'][0]['containerID']!=child['status']['containerStatuses'][0]['containerID']
 # Verify the normal CRI/containerd identities and the host's guest-ID map.
 registrations=[]
 for observed in (parent,child):
  cid=observed['status']['containerStatuses'][0]['containerID'].split('://',1)[1]
  record=json.loads(ctr('containers','info',cid));sid=record['SandboxID']
  assert record['Labels']['io.kubernetes.pod.uid']==observed['metadata']['uid']
  assert json.loads((state/sid/'containerd-ids.json').read_text())['samecontainer']==cid
  registrations.append({'pod_uid':observed['metadata']['uid'],'container_id':cid,'sandbox_id':sid})
 if network:
  # Both directions traverse independent guest NICs and CNI endpoints.
  for name,peer in [('fork-parent',child_ip),('fork-child',parent_ip)]:
   assert len(kubectl('exec',name,'--','/bin/busybox','wget','-qO-','http://'+peer+':8080/init-state').split())==3
 values={}
 for name,value in [('fork-parent',20),('fork-child',30)]:
  before=kubectl('exec',name,'--','/bin/busybox','cat','/init-state').strip();values[name]=before
  kubectl('exec',name,'--','/bin/sh','-c',f'echo {value} > /init-command')
  for _ in range(30):
   after=kubectl('exec',name,'--','/bin/busybox','cat','/init-state').split()
   if len(after)==3 and int(after[1])==value:break
   time.sleep(.1)
  assert len(after)==3 and int(after[1])==value
 assert values['fork-parent'].split()[2]==values['fork-child'].split()[2]
 if disk_checks:disk_checks.inspect('fork-parent','fork-child')
 order=['fork-parent','fork-child']
 if os.environ.get('DELETE_CHILD_FIRST'):order.reverse()
 kubectl('delete','pod',order[0],'--wait=true','--timeout=90s')
 if disk_checks:disk_checks.survivor(order[1])
 assert kubectl('exec',order[1],'--','/bin/busybox','cat','/init-state').split()[1]==('20' if order[1]=='fork-parent' else '30')
 kubectl('delete','pod',order[1],'--wait=true','--timeout=90s')
 for _ in range(150):
  if call('sandbox-list')['sandboxes']==[]:break
  time.sleep(1)
 else:raise AssertionError(call('sandbox-list'))
 if network:
  for pod in (parent,child):
   assert not (out/'ipam/fork-research'/pod['status']['podIP']).exists(), 'CNI IP allocation leaked'
 assert not list(out.rglob('memory.img'))
 assert not list((out/'state').glob('*/fork-disks')), 'disk layers leaked after sandbox destruction'
 pss=re.findall(r'combined_ram_pss_kib=(\d+) baseline_kib=(\d+)',(out/'host.log').read_text());assert len(pss)==2 and all(a==b for a,b in pss)
 result={'result':'PASS','parent_uid':uid,'child_uid':child['metadata']['uid'],'process_state':values,'pss':pss,'deletion_order':order,'registrations':registrations,'pod_ips':[parent['status'].get('podIP'),child['status'].get('podIP')],'network':network,'macs':macs if network else []}
 (out/'results.json').write_text(json.dumps(result,indent=2));print(result)
finally:
 restart_stop.set()
 if restart_thread:restart_thread.join(timeout=20)
 for p in reversed(processes):
  try:os.killpg(p.pid,signal.SIGTERM)
  except ProcessLookupError:pass
 for p in reversed(processes):
  try:p.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
 for f in logs:f.close()
 if network:
  subprocess.run([str(cni_bin/'bridge')],env={**seed_env,'CNI_COMMAND':'DEL'},input=json.dumps(cni_config),text=True,capture_output=True)
  subprocess.run(['ip','netns','del',seedns],capture_output=True)
  subprocess.run(['ip','link','del','forkbr0'],capture_output=True)
 # Failed fixtures may stop containerd before CRI removes its netns mounts.
 # Reclaim only mounts rooted in this fixture after all owned processes exit.
 netns_prefix=str(out/'ctrd-state/io.containerd.grpc.v1.cri/netns')+'/'
 for line in Path('/proc/self/mountinfo').read_text().splitlines():
  target=line.split()[4]
  if target.startswith(netns_prefix):subprocess.run(['umount',target],check=True)
