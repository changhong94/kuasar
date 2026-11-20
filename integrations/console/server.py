#!/usr/bin/env python3
"""Standalone LAN console. Runtime, storage, network and binaries are dedicated.
No raw host paths, commands or runtime admin endpoints are exposed over HTTP.
"""
import secrets
import concurrent.futures, contextlib, http.server, json, os, re, shutil, socket, sqlite3, subprocess as sp, sys, threading, time, uuid
from pathlib import Path
from urllib.parse import urlsplit
ROOT=Path(os.environ.get('FORKLAB_ROOT','/srv/forklab'));DATA=ROOT/'data'
sys.path.insert(0,str(ROOT/'app/e2b'))
import vm_rpc
from image_pool import UnixHTTP,add_image_disk,MOUNT_OCI_IMAGE
MAX_SANDBOXES=int(os.environ.get('FORKLAB_MAX_SANDBOXES','6'))
DB=DATA/'console.sqlite';LOCK=threading.RLock();BOOT=threading.Lock();POOL=concurrent.futures.ThreadPoolExecutor(4)

def db():
 c=sqlite3.connect(DB,timeout=30);c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA synchronous=FULL');return c

def init():
 with db() as c:
  for table in ['sandboxes','snapshots']:c.execute(f'CREATE TABLE IF NOT EXISTS {table}(id TEXT PRIMARY KEY, name TEXT UNIQUE NOT NULL, body TEXT NOT NULL)')

def rows(table):
 with db() as c:return [json.loads(x[0]) for x in c.execute(f'SELECT body FROM {table} ORDER BY rowid DESC')]

def get(table,ident):
 with db() as c:r=c.execute(f'SELECT body FROM {table} WHERE id=?',(ident,)).fetchone()
 if not r:raise ValueError('记录不存在')
 return json.loads(r[0])

def put(table,row):
 with db() as c:c.execute(f'INSERT INTO {table} VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name, body=excluded.body',(row['id'],row['name'],json.dumps(row,ensure_ascii=False)))

def update(table,ident,**kw):
 with LOCK:
  row=get(table,ident);row.update(kw);put(table,row);return row

def admin(action,**kw):return vm_rpc.admin(Path('/run/forklab/admin.sock'),action,**kw)
def info(ident):return admin('sandbox-get',sandbox_id=ident)['live_fork']
def vm(ident,action,body=None):
 c=UnixHTTP(info(ident)['api_socket'])
 try:
  c.request('GET' if action=='info' else 'PUT','/api/v1/vm.'+action,body=json.dumps(body) if body is not None else None)
  r=c.getresponse();data=r.read()
  if r.status>=300:raise RuntimeError(data.decode(errors='replace'))
  return json.loads(data) if data else {}
 finally:c.close()
def guest(ident,argv):
 r=vm_rpc.execute(info(ident)['vsock_socket'],argv)
 if r['exit_code']:raise RuntimeError(r['stderr'].decode(errors='replace')[:2000])
 return r['stdout'].decode(errors='replace')
def images():
 result=[json.loads(p.read_text()) for p in sorted((DATA/'registry').glob('*.json'))]
 for row in result:
  if row['state']=='downloading':
   total=0
   for p in (DATA/'images'/('.import-'+row['id'])/'oci/blobs/sha256').glob('*'):
    try:
     st=p.stat();total+=st.st_blocks*512 if p.name.endswith('.range-part') else st.st_size
    except FileNotFoundError:pass
   row['downloaded_bytes']=min(total,row.get('compressed_bytes',total))
 return result
def image(ident):
 return next(i for i in images() if i['id']==ident)
def cni(ident,action):
 ns='/run/netns/fl-'+ident
 env={**os.environ,'CNI_COMMAND':action,'CNI_CONTAINERID':ident,'CNI_NETNS':ns,'CNI_IFNAME':'eth0','CNI_PATH':str(ROOT/'bin/cni')}
 p=sp.run([str(ROOT/'bin/cni/bridge')],input=(ROOT/'cni.json').read_text(),text=True,capture_output=True,env=env)
 if p.returncode:raise RuntimeError(p.stderr or p.stdout)
 return json.loads(p.stdout) if p.stdout.strip() else {}
def boot(ident):
 with BOOT:
  sp.run(['cp','--reflink=always',str(DATA/'base.raw'),str(DATA/'seed.next')],check=True)
  (DATA/'seed.next').chmod(0o600);os.replace(DATA/'seed.next',DATA/'seed.raw')
  sp.run(['ip','netns','add','fl-'+ident],check=True)
  result=cni(ident,'ADD');(DATA/'network'/f'{ident}.json').write_text(json.dumps(result))
  return admin('sandbox-boot',sandbox_id=ident,netns='/run/netns/fl-'+ident)
def cleanup_network(ident):
 if Path('/run/netns/fl-'+ident).exists():
  cni(ident,'DEL');sp.run(['ip','netns','del','fl-'+ident],check=True)
 (DATA/'network'/f'{ident}.json').unlink(missing_ok=True)
def allocated(path):
 return sum(p.stat().st_blocks*512 for p in path.rglob('*') if p.is_file())
def create_worker(ident,source,kind):
 try:
  boot(ident)
  if kind=='snapshot':
   snap=get('snapshots',source)
   admin('sandbox-snapshot-live-restore',sandbox_id=ident,template=source)
   im=image(snap['image_id'])
  else:
   im=image(source);add_image_disk(info(ident)['api_socket'],im['disk'])
   # Writable application files belong to the persistent guest disk.
   mount='mount -o remount,rw /lower\n'+MOUNT_OCI_IMAGE.replace('/run/image-upper','/lower/image-upper').replace('/run/image-work','/lower/image-work')
   guest(ident,['/bin/sh','-c',mount])
   cfg=im.get('oci_config',{});env=cfg.get('Env') or ['PATH=/usr/local/bin:/usr/bin:/bin'];cwd=cfg.get('WorkingDir') or '/';user=cfg.get('User') or '0'
   # A sandbox remains available independently of an image's short-lived CMD.
   guest(ident,['/usr/bin/oci-launch','start','/run/image-root','/run/console-workload',cwd,user,str(len(env)),*env,'--','/.__kuasar/busybox','sleep','infinity'])
  guest(ident,['/bin/sh','-c','test -d /run/image-root; test -r /run/image-root/etc/os-release'])
  update('sandboxes',ident,state='running',busy=False,error=None)
 except Exception as e:
  update('sandboxes',ident,state='failed',busy=False,error=str(e))

def action_worker(ident,action):
 try:
  if action=='destroy':
   try:admin('sandbox-destroy',sandbox_id=ident)
   except Exception as e:
    if 'not found' not in str(e):raise
   cleanup_network(ident)
   update('sandboxes',ident,state='deleted',busy=False)
   purge_deleted()
  else:
   vm(ident,action);update('sandboxes',ident,state='paused' if action=='pause' else 'running',busy=False,error=None)
 except Exception as e:update('sandboxes',ident,busy=False,error=str(e))

def finish_snapshot(snap):
 try:
  state=admin('sandbox-snapshot-live-status',template=snap['id'])
  kw={k:state[k] for k in ['pause_ms','durable_ms','phases','persistence_phases'] if k in state}
  if state['state']=='durable':kw.update(state='ready',allocated_bytes=allocated(DATA/'templates'/snap['id']))
  elif state['state']=='failed':kw.update(state='failed',error=state.get('error','保存失败'))
  update('snapshots',snap['id'],**kw)
 except Exception as e:
  if snap['state']=='saving':update('snapshots',snap['id'],last_check_error=str(e))

def capture_worker(ident,snapid):
 try:
  reply=admin('sandbox-snapshot-live',sandbox_id=ident,template=snapid)
  with LOCK:
   current=get('snapshots',snapid)
   if current['state']=='capturing':update('snapshots',snapid,state='saving',pause_ms=reply.get('pause_ms'))
 except Exception as e:update('snapshots',snapid,state='failed',error=str(e))
 finally:update('sandboxes',ident,busy=False)

def purge_deleted():
 with LOCK:
  live=rows('sandboxes')
  for s in rows('snapshots'):
   if s['state']!='deleted' or s.get('purged'):continue
   if any(x.get('snapshot_id')==s['id'] and x['state']!='deleted' for x in live):continue
   # Keep backing files pinned until the last restored sandbox is destroyed.
   p=DATA/'templates'/s['id']
   if p.exists():shutil.rmtree(p)
   (DATA/'templates/.live-jobs'/f'{s["id"]}.json').unlink(missing_ok=True)
   update('snapshots',s['id'],purged=True)

def reconcile():
 while True:
  try:
   for s in rows('snapshots'):
    if s['state'] in ['capturing','saving']:finish_snapshot(s)
   for s in rows('sandboxes'):
    if s['state'] in ['running','paused','unavailable'] and not s.get('busy'):
     try:
      actual=vm(s['id'],'info').get('state');state={'Running':'running','Paused':'paused'}.get(actual,'unavailable')
      with LOCK:
       now=get('sandboxes',s['id'])
       if not now.get('busy') and now['state'] in ['running','paused','unavailable']:update('sandboxes',s['id'],state=state)
     except Exception as e:
      with LOCK:
       now=get('sandboxes',s['id'])
       if not now.get('busy') and now['state'] in ['running','paused','unavailable']:update('sandboxes',s['id'],state='unavailable',error='运行状态暂不可查询：'+str(e))
  except Exception as e:print('reconcile:',e,flush=True)
  time.sleep(2)

def image_short_name(reference):
 # Drop the registry/organization and the tag or digest, not repository text.
 return reference.split('@',1)[0].rsplit('/',1)[-1].split(':',1)[0]

def unique_display_name(prefix,reference):
 # Called under LOCK together with the SQLite insert. Existing (including
 # deleted) names stay unchanged and cannot be reused by a new object.
 stem=f'{prefix}-{image_short_name(reference)}-'
 used={r['name'] for table in ['sandboxes','snapshots'] for r in rows(table)}
 if sum(n.startswith(stem) and re.fullmatch(r'[0-9]{5}',n[len(stem):]) is not None for n in used)>=100000:
  raise ValueError('此镜像的五位编号已用尽')
 while True:
  name=f'{stem}{secrets.randbelow(100000):05d}'
  if name not in used:return name

def submit(path,body):
 with LOCK:
  if path=='/api/create':
   kind=body.get('kind');source=body.get('source')
   if kind=='image':
    im=image(source)
    if im['state']!='ready':raise ValueError('镜像尚未准备完成')
   elif kind=='snapshot':
    snap=get('snapshots',source)
    if snap['state']!='ready':raise ValueError('快照尚未保存完成或已删除')
    im=image(snap['image_id'])
   else:raise ValueError('无效的创建来源')
   if len([r for r in rows('sandboxes') if r['state']!='deleted'])>=MAX_SANDBOXES:raise ValueError(f'此专属服务最多同时保留 {MAX_SANDBOXES} 个沙箱，请先销毁不用的沙箱')
   ident='sb-'+uuid.uuid4().hex[:12]
   row=dict(id=ident,name=unique_display_name('sandbox',im['name']),image_id=im['id'],image_name=im['name'],snapshot_id=source if kind=='snapshot' else None,state='creating',busy=True,created_at=time.time())
   put('sandboxes',row);POOL.submit(create_worker,ident,source,kind);return row
  if path=='/api/snapshot':
   ident=body.get('sandbox_id');row=get('sandboxes',ident)
   if row['state']!='running' or row.get('busy'):raise ValueError('只有运行中的空闲沙箱可以创建快照')
   if any(s['parent']==ident and s['state'] in ['capturing','saving'] for s in rows('snapshots')):raise ValueError('此沙箱已有快照正在保存')
   name=unique_display_name('snapshot',row['image_name'])
   snap=dict(id='sn-'+uuid.uuid4().hex,name=name,parent=ident,image_id=row['image_id'],image_name=row['image_name'],state='capturing',created_at=time.time())
   put('snapshots',snap);update('sandboxes',ident,busy=True);POOL.submit(capture_worker,ident,snap['id']);return snap
  if path=='/api/action':
   ident=body.get('id');action=body.get('action');row=get('sandboxes',ident)
   if row.get('busy'):raise ValueError('操作进行中，请稍候')
   allowed={'pause':{'running'},'resume':{'paused'},'destroy':{'running','paused','failed','unavailable'}}
   if action not in allowed or row['state'] not in allowed[action]:raise ValueError('当前状态不支持此操作')
   update('sandboxes',ident,busy=True);POOL.submit(action_worker,ident,action);return {'ok':True}
  if path=='/api/delete-snapshot':
   row=get('snapshots',body.get('id'))
   if row['state'] not in ['ready','failed']:raise ValueError('正在保存的快照不能删除')
   update('snapshots',row['id'],state='deleted');purge_deleted();return {'ok':True}
  raise ValueError('未知操作')

class Handler(http.server.BaseHTTPRequestHandler):
 def reply(self,status,value,content_type='application/json; charset=utf-8'):
  b=json.dumps(value,ensure_ascii=False).encode() if isinstance(value,(dict,list)) else value
  self.send_response(status);self.send_header('Content-Type',content_type);self.send_header('Content-Length',str(len(b)));self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff');self.send_header('X-Frame-Options','DENY');self.end_headers();self.wfile.write(b)
 def do_GET(self):
  if self.path=='/api/state':
   imgs=images();order={'python':0,'numpy':1,'pygmt':2,'pandas':3,'twisted':4};imgs.sort(key=lambda x:order[x['id']])
   public=[{k:v for k,v in x.items() if k not in ['disk','oci_config']} for x in imgs]
   return self.reply(200,dict(images=public,sandboxes=[r for r in rows('sandboxes') if r['state']!='deleted'],snapshots=[r for r in rows('snapshots') if r['state']!='deleted'],max_sandboxes=MAX_SANDBOXES,storage='local',server_time=time.time()))
  if self.path in ['/','/index.html']:return self.reply(200,(ROOT/'app/index.html').read_bytes(),'text/html; charset=utf-8')
  return self.reply(404,{'error':'Not found'})
 def do_POST(self):
  try:
   origin=self.headers.get('Origin')
   if origin and urlsplit(origin).netloc!=self.headers.get('Host'):return self.reply(403,{'error':'不允许跨站操作'})
   if self.headers.get('Content-Type','').split(';')[0]!='application/json':return self.reply(415,{'error':'JSON required'})
   n=int(self.headers.get('Content-Length','0'))
   if not 0<n<=4096:raise ValueError('请求大小无效')
   body=json.loads(self.rfile.read(n));result=submit(self.path,body);self.reply(202,result)
  except (ValueError,KeyError,StopIteration,sqlite3.IntegrityError) as e:self.reply(409,{'error':str(e) or '记录不存在'})
  except Exception as e:print('request:',repr(e),flush=True);self.reply(500,{'error':str(e)})
 def log_message(self,fmt,*args):
  if self.path!='/api/state':super().log_message(fmt,*args)

if __name__=='__main__':
 init()
 # A web process restart does not destroy VMs. Recover unfinished UI jobs.
 for row in rows('sandboxes'):
  if row.get('busy'):
   try:state={'Running':'running','Paused':'paused'}.get(vm(row['id'],'info').get('state'),'failed')
   except Exception:state='failed'
   update('sandboxes',row['id'],busy=False,state=state,error='网页服务重启，请核对状态')
 threading.Thread(target=reconcile,daemon=True).start()
 http.server.ThreadingHTTPServer(('0.0.0.0',int(os.environ.get('PORT','18088'))),Handler).serve_forever()
