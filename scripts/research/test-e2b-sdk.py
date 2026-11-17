#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run against the real, unmodified E2B Python SDK and real Kubernetes Pods."""
import json, os, re, socket, sys, time, urllib.request, urllib.error, threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from e2b import Sandbox

out=Path(sys.argv[1]);url='http://127.0.0.1:18881'
key=(out/'e2b-key').read_text().strip()
opts=dict(api_key=key,api_url=url,sandbox_url=url,request_timeout=150)
def admin(action,**kw):
 with socket.socket(socket.AF_UNIX) as s:
  s.settimeout(90);s.connect(str(out/'admin.sock'));s.sendall((json.dumps(dict(action=action,**kw))+'\n').encode());r=json.loads(s.makefile().readline())
 assert r['ok'],r;return r

def record(sandbox):return json.loads((out/'e2b-state.json').read_text())[sandbox.sandbox_id]
def http(ip,path):return urllib.request.build_opener(urllib.request.ProxyHandler({})).open('http://'+ip+':8080'+path,timeout=5).read()
timings=[]
timing_lock=threading.Lock()
def measure(label, fn):
 started=time.perf_counter()
 try:return fn()
 finally:
  item={'operation':label,'seconds':time.perf_counter()-started}
  with timing_lock:
   timings.append(item)
   (out/'timings.json').write_text(json.dumps(timings,indent=2))
   print('TIMING',item,flush=True)
def command(sandbox,cmd,**kwargs):
 return measure('sdk.exec.'+('parent' if sandbox is parent else 'child'),lambda:sandbox.commands.run(cmd,**kwargs))
disk_checks=None
parent=child=None
try:
 parent=Sandbox.create(timeout=1200,**opts)
 print('SDK created',parent.sandbox_id,flush=True)
 result=command(parent,"printf 'stdout'; printf 'stderr' >&2",user='root')
 assert result.stdout=='stdout' and result.stderr=='stderr' and result.exit_code==0
 assert command(parent,'printf "$FORK_ENV"; pwd',envs={'FORK_ENV':'test:'},cwd='/tmp',user='root').stdout=='test:/tmp\n'
 try:command(parent,'exit 7',user='root');raise AssertionError('nonzero exit accepted')
 except Exception as e:assert getattr(e,'exit_code',None)==7,str(e)
 for iteration, size in enumerate([65792] * 4 + [1048833]):
  payload=(bytes(range(256))*((size+255)//256))[:size]
  parent.files.write('/tmp/binary',payload,user='root')
  received=parent.files.read('/tmp/binary',format='bytes',user='root')
  (out/'binary-read.bin').write_bytes(received)
  assert received==payload, {'iteration':iteration,'sdk_bytes':len(received),'expected':len(payload),'guest_http_bytes':len(http(record(parent)['ip'],'/tmp/binary'))}
 print('SDK binary round trips: four 65792-byte files and one 1048833-byte file',flush=True)
 parent.files.write('/tmp/value',b'before fork\x00binary',user='root')
 assert parent.files.read('/tmp/value',format='bytes',user='root')==b'before fork\x00binary'
 before=command(parent,'/bin/busybox cat /init-state',user='root').stdout.split()
 assert len(before)==3
 admin('sandbox-destroy',sandbox_id='seed00000001')
 if os.environ.get('FORK_WRITABLE_DISK'):
  from writable_disk_checks import DiskChecks
  def disk_exec(pod,cmd):
   box=parent if pod==parent.sandbox_id else child
   return command(box,cmd,user='root').stdout
  disk_checks=DiskChecks(out,disk_exec)
  disk_checks.seed(parent.sandbox_id)
 pinfo=record(parent);held=socket.create_connection((pinfo['ip'],8080),timeout=5)
 child=measure('sdk.fork',lambda:parent.fork(count=1,timeout=1200))[0]
 if isinstance(child,Exception):raise child
 print('SDK forked',child.sandbox_id,flush=True)
 cinfo=record(child)
 held.sendall(b'GET /init-state HTTP/1.0\r\n\r\n');assert b'200 OK' in held.recv(4096);held.close()
 def first_commands(pair):
  label,box=pair
  assert measure('sdk.first_exec.'+label,lambda:box.commands.run('printf ready',user='root')).stdout=='ready'
  import subprocess
  argv=[str(Path(__file__).resolve().parents[4]/'tools/k3s-v1.31'),'kubectl','--kubeconfig',str(out/'kubeconfig'),'exec',box.sandbox_id,'--','/bin/busybox','echo','ready']
  assert measure('kubectl.exec.'+label,lambda:subprocess.check_output(argv))==b'ready\n' 
 pairs=[('parent',parent),('child',child)]
 if os.environ.get('LATENCY_PARALLEL'):
  with ThreadPoolExecutor(max_workers=2) as pool:
   list(pool.map(first_commands,pairs))
   for _ in range(5):
    replies=list(pool.map(lambda box:command(box,'printf ready',user='root'),[parent,child]))
    assert all(reply.stdout=='ready' for reply in replies)
 else:
  for pair in pairs:first_commands(pair)
 assert parent.get_info().sandbox_id==parent.sandbox_id
 assert child.get_info().sandbox_id==child.sandbox_id

 assert parent.sandbox_id!=child.sandbox_id and pinfo['uid']!=cinfo['uid'] and pinfo['ip']!=cinfo['ip']
 macs=[]
 for sandbox in (parent,child):
  link=command(sandbox,'/bin/busybox ip link show eth0',user='root').stdout
  macs.append(re.search(r'link/ether ([0-9a-f:]+)',link).group(1))
 assert macs[0]!=macs[1]
 old_ip_hex=socket.inet_aton(pinfo['ip'])[::-1].hex().upper()
 tcp=command(child,'/bin/busybox cat /proc/net/tcp',user='root').stdout
 assert not any(line.split()[1].startswith(old_ip_hex+':') and line.split()[3]=='01' for line in tcp.splitlines()[1:])

 assert child.files.read('/tmp/value',format='bytes',user='root')==b'before fork\x00binary'
 assert command(child,'/bin/busybox cat /init-state',user='root').stdout.split()[2]==before[2]
 for sandbox,value in ((parent,20),(child,30)):
  sandbox.files.write('/tmp/value',str(value),user='root')
  command(sandbox,'echo '+str(value)+' > /init-command',user='root')
  assert sandbox.files.read('/tmp/value',user='root')==str(value)
  assert command(sandbox,'/bin/busybox cat /init-state',user='root').stdout.split()[1]==str(value)
  assert http(record(sandbox)['ip'],'/tmp/value')==str(value).encode()
 assert command(parent,'/bin/busybox wget -qO- http://'+cinfo['ip']+':8080/tmp/value',user='root').stdout=='30'
 assert command(child,'/bin/busybox wget -qO- http://'+pinfo['ip']+':8080/tmp/value',user='root').stdout=='20'
 if disk_checks:disk_checks.inspect(parent.sandbox_id,child.sandbox_id)
 # Distinct envd tokens cannot be used to address the other sandbox.
 req=urllib.request.Request(url+'/files?path=/tmp/value',headers={'E2b-Sandbox-Id':child.sandbox_id,'X-Access-Token':pinfo['token']})
 try:urllib.request.urlopen(req);raise AssertionError('cross-sandbox token accepted')
 except urllib.error.HTTPError as e:assert e.code==401
 identities=[{'sandbox_id':x.sandbox_id,'pod_uid':record(x)['uid'],'ip':record(x)['ip']} for x in (parent,child)]
 order=[parent,child]
 if os.environ.get('DELETE_CHILD_FIRST'):order.reverse()
 order[0].kill()
 if disk_checks:disk_checks.survivor(order[1].sandbox_id)
 assert command(order[1],'/bin/busybox cat /tmp/value',user='root').stdout==('20' if order[1] is parent else '30')
 assert http(record(order[1])['ip'],'/tmp/value') in (b'20',b'30')
 order[1].kill()
 for _ in range(150):
  if admin('sandbox-list')['sandboxes']==[]:break
  time.sleep(1)
 else:raise AssertionError('kubelet did not reclaim sandboxes')
 for identity in identities:
  assert not (out/'ipam/fork-research'/identity['ip']).exists(), 'CNI IP allocation leaked'
 pss=re.findall(r'combined_ram_pss_kib=(\d+) baseline_kib=(\d+)',(out/'host.log').read_text());assert len(pss)==2 and all(a==b for a,b in pss),pss
 assert not list(out.rglob('memory.img'))
 assert not list((out/'state').glob('*/fork-disks')), 'disk layers leaked after sandbox destruction'
 if os.environ.get('MAX_EXEC_SECONDS'):
  limit=float(os.environ['MAX_EXEC_SECONDS'])
  assert all(x['seconds']<limit for x in timings if 'exec' in x['operation']), timings
 result={'result':'PASS','sdk':'e2b 2.64.0 (unmodified)','binary_sizes':[65792]*4+[1048833],'identities':identities,'inherited_nonce':before[2],'macs':macs,'pss':pss,'deletion_order':[x.sandbox_id for x in order], 'checks':['SDK create, get_info, commands, binary file write/read, live fork, kill','new Pod UID, MAC, IP and envd token','parent TCP connection survives fork; child identity reset','inherited process nonce and independent CoW RAM and files','bidirectional guest NIC/CNI HTTP; survivor exec and HTTP after peer deletion','kubelet and CNI IPAM cleanup; no memory.img; fork PSS unchanged']}
 (out/'results.json').write_text(json.dumps(result,indent=2));print(result,flush=True)
finally:
 for sandbox in (child,parent):
  if sandbox:
   try:sandbox.kill()
   except Exception:pass
