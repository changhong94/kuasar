"""Verified parallel byte-range prefetch for Docker Hub blobs.
Skopeo still creates the final OCI layout. No signed URLs or tokens are logged.
"""
import concurrent.futures,hashlib,os,re,time
from pathlib import Path
import requests

def prefetch(name,manifest,layout):
 if not name.startswith('docker.io/'):return
 repo=name[len('docker.io/'):];root=Path(layout)/'blobs/sha256';root.mkdir(parents=True,exist_ok=True)
 token=requests.get('https://auth.docker.io/token',params={'service':'registry.docker.io','scope':f'repository:{repo}:pull'},timeout=60)
 token.raise_for_status();headers={'Authorization':'Bearer '+token.json()['token']}
 for desc in [manifest['config'],*manifest.get('layers',[])]:
  digest=desc['digest'];size=desc['size']
  if not re.fullmatch('sha256:[0-9a-f]{64}',digest):raise ValueError('unsupported blob digest')
  target=root/digest[7:]
  def verify(p):
   if not p.exists() or p.stat().st_size!=size:return False
   h=hashlib.sha256()
   with p.open('rb') as f:
    for data in iter(lambda:f.read(8*1024*1024),b''):h.update(data)
   return h.hexdigest()==digest[7:]
  if verify(target):continue
  endpoint=f'https://registry-1.docker.io/v2/{repo}/blobs/{digest}'
  response=requests.get(endpoint,headers=headers,allow_redirects=False,stream=True,timeout=60)
  if response.is_redirect:
   url=response.headers['Location'];auth={}
  else:
   response.raise_for_status();url=endpoint;auth=headers
  response.close()
  part=target.with_suffix('.range-part');fd=os.open(part,os.O_RDWR|os.O_CREAT|os.O_TRUNC,0o600);os.ftruncate(fd,size)
  def chunk(bounds):
   start,end=bounds
   for attempt in range(4):
    try:
     with requests.get(url,headers={**auth,'Range':f'bytes={start}-{end}'},stream=True,timeout=(45,90)) as r:
      r.raise_for_status()
      if not ((r.status_code==206 and r.headers.get('Content-Range')==f'bytes {start}-{end}/{size}') or (r.status_code==200 and start==0 and end==size-1)):
       raise RuntimeError('registry does not honor exact byte ranges')
      offset=start
      for data in r.iter_content(256*1024):
       if offset+len(data)>end+1:raise RuntimeError('range overflow')
       view=memoryview(data)
       while view:
        n=os.pwrite(fd,view,offset);offset+=n;view=view[n:]
      if offset!=end+1:raise RuntimeError('incomplete range')
     return
    except Exception:
     if attempt==3:raise RuntimeError('registry range download failed (4 attempts)') from None
     time.sleep(1+attempt)
  try:
   chunk_size=16*1024*1024
   with concurrent.futures.ThreadPoolExecutor(32) as pool:list(pool.map(chunk,[(s,min(size-1,s+chunk_size-1))for s in range(0,size,chunk_size)]))
   os.fsync(fd)
  finally:os.close(fd)
  if not verify(part):raise ValueError('downloaded blob digest mismatch')
  os.replace(part,target);print('verified blob',digest,'bytes',size,flush=True)
