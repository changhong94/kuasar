"""Supplementary paired two-sandbox storage observations; run only after timing tests."""
import sys,subprocess as sp,json,time,hashlib,shlex,fcntl,traceback,shutil,os
from pathlib import Path
import runner as b,extent_accounting as ex
paths={'kuasar-off':['comparison/cube-xfs/kuasar'],'cube':['comparison/cube-xfs/data','comparison/cube-xfs/support'],'agentenv':['comparison/agentenv'],'kuasar-uffd':['uffd'],'kuasar-mlink':['mlink-generic'],'kuasar-fork':['/home/hong/src/research/python-latest-113/data/fork','/home/hong/src/research/python-latest-113/control/fork-sqlite']}
def footprint(pr):
 roots=[Path('/home/hong/data')/x for x in paths[pr]]
 try:return dict(valid=True,fs_dev=os.stat(roots[0]).st_dev,**ex.scan(roots))
 except Exception as e:return dict(valid=False,reason=repr(e))
def work(pr,diff,p):
 b.pr=pr;b.setup(pr);out={'product':pr,'different':diff,'stages':{},'instances':[]};owned={}
 def snap(name):
  z=out['stages'][name]={'extent':footprint(pr),'pss_mib':b.resources()['pss_mib'],'allocated_bytes':b.disk()};b.save(p,out)
 try:
  snap('before');h=[]
  for i in range(2):
   s=b.create();owned[b.sid(s)]=s;out['instances'].append(b.sid(s));b.save(p,out);b.ready(s,'printf READY','READY')
   seed=i if diff else 0;code=b.IO.replace("'fiveway'+str(i)","'fiveway-storage-%d-'%seed+str(i)".replace('%seed','%'+str(seed)))
   # Independently generate the expected digest using the same workload seed.
   exp=hashlib.sha256()
   for k in range(32):exp.update(hashlib.shake_256(('fiveway-storage-%d-'%seed+str(k)).encode()).digest(1048576))
   hh=json.loads(b.py(s,code))[0];assert hh==exp.hexdigest();h.append(hh)
  assert (h[0]!=h[1])==bool(diff)
  out['digests']=h;snap('live2')
  out['filesystem_write_backing']='qcow2-ext4'
  for s in owned.values():b.pause(s)
  time.sleep(1);snap('paused2')
  for i,(key,s) in enumerate(list(owned.items())):
   s=b.resume(s)
   owned[key]=s;b.ready(s,'printf READY','READY');assert b.cmd(s,'sha256sum /tmp/fiveway.bin').split()[0]==h[i]
   b.kill(s);owned.pop(key);b.save(p,out)
  time.sleep(2);snap('settled');out['success']=True
 except Exception:out['success']=False;out['error']=traceback.format_exc()
 finally:
  for k,s in list(owned.items()):
   try:b.kill(s);owned.pop(k)
   except Exception:pass
  out['remaining']=list(owned);b.save(p,out)
 return out['success'] and not owned
if __name__=='__main__':
 lock=open('/run/skyrl-threeway-benchmark.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 root=Path('/home/hong/src/research/fork-features-113/results/storage');root.mkdir(exist_ok=True)
 for r in range(3):
  for different in [0,1]:
   target=root/f'{r+1}-{different}-kuasar-fork.json'
   print('START',target.name,flush=True)
   if not work('kuasar-fork',different,target):raise RuntimeError(str(target))
   print('DONE',target.name,flush=True)
