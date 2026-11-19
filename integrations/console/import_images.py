#!/usr/bin/env python3
"""Pinned OCI imports for the standalone fork console (no benchmark state)."""
import json, os, subprocess as sp, time, hashlib, shutil, tempfile
from pathlib import Path
D=Path(os.environ.get('FORKLAB_DATA','/srv/forklab/data'))
IMAGES=[('python','python:3.12-slim','python@sha256:2fe5997d249a808b8eeea52c58a1dbffbba28754dc11699ef5c029f2d818ce79'),('numpy','namanjain12/numpy_final:ff12de3f3dead7f522ab5a1076fcef4fbbdf3314',None),('pygmt','swerebench/sweb.eval.x86_64.genericmappingtools_1776_pygmt-1530:latest',None),('pandas','namanjain12/pandas_final:461772af9bc8c4899f7af5639c69cc93d87e6f1b',None),('twisted','swerebench/sweb.eval.x86_64.twisted_1776_twisted-12096:latest',None)]
def atomic(p,v):
 tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(v,ensure_ascii=False,indent=2));
 with tmp.open('rb') as f:os.fsync(f.fileno())
 os.replace(tmp,p)
 fd=os.open(p.parent,os.O_DIRECTORY);os.fsync(fd);os.close(fd)
def run(args,**kw):return sp.run(args,check=True,**kw)
def main():
 D.joinpath('registry').mkdir(parents=True,exist_ok=True)
 for ident,name,ref in IMAGES:
  p=D/'registry'/f'{ident}.json'
  if not p.exists():atomic(p,dict(id=ident,name=name,state='pending'))
 for ident,name,ref in IMAGES:
  if os.environ.get('FORKLAB_ONLY_IMAGE') and os.environ['FORKLAB_ONLY_IMAGE']!=ident:continue
  p=D/'registry'/f'{ident}.json';row=json.loads(p.read_text())
  if row.get('state')=='ready' and Path(row['disk']).exists():continue
  row.update(state='downloading',error=None);atomic(p,row);started=time.monotonic()
  work=D/'images'/('.import-'+ident);work.mkdir(exist_ok=True)
  try:
   source='docker://'+(ref or name)
   inspected=json.loads(sp.check_output(['skopeo','--command-timeout','120s','inspect','--override-arch','amd64',source]))
   pinned='docker://'+inspected['Name']+'@'+inspected['Digest']
   raw=json.loads(sp.check_output(['skopeo','--command-timeout','120s','inspect','--raw',pinned]))
   row.update(manifest=inspected['Digest'],compressed_bytes=sum(l['size'] for l in raw.get('layers',[])),source=pinned);atomic(p,row)
   from prefetch_blobs import prefetch
   prefetch(inspected['Name'],raw,work/'oci')
   run(['skopeo','--command-timeout','3600s','--override-os','linux','--override-arch','amd64','copy',pinned,'oci:'+str(work/'oci')+':image'])
   row['state']='unpacking';atomic(p,row)
   bundle=work/'bundle'
   if bundle.exists():shutil.rmtree(bundle)
   run(['umoci','unpack','--image',str(work/'oci')+':image',str(bundle)])
   idx=json.loads((work/'oci/index.json').read_text());md=idx['manifests'][0]['digest'].split(':')[1]
   manifest=json.loads((work/'oci/blobs/sha256'/md).read_text());cd=manifest['config']['digest'].split(':')[1]
   cfg=json.loads((work/'oci/blobs/sha256'/cd).read_text())
   if cfg['architecture']!='amd64' or cfg['os']!='linux':raise ValueError('linux/amd64 required')
   if (bundle/'rootfs/.__kuasar').exists():raise ValueError('reserved runtime path')
   used=int(sp.check_output(['du','-s','-B1',str(bundle/'rootfs')]).split()[0]);size=max(512*1024**2,int(used*1.35)+256*1024**2)
   size=((size+1024**2-1)//1024**2)*1024**2
   if shutil.disk_usage(D).free<size+3*1024**3:raise RuntimeError('Not enough dedicated disk space')
   row.update(state='building',unpacked_bytes=used);atomic(p,row)
   disk=work/'root.ext4';run(['truncate','-s',str(size),str(disk)]);run(['mkfs.ext4','-q','-F','-d',str(bundle/'rootfs'),str(disk)])
   disk.chmod(0o444)
   with disk.open('rb') as f:
    os.fsync(f.fileno());h=hashlib.sha256()
    for chunk in iter(lambda:f.read(8*1024*1024),b''):h.update(chunk)
    sha=h.hexdigest()
   final=D/'images'/(ident+'.ext4');os.replace(disk,final)
   row.update(state='ready',disk=str(final),sha256=sha,oci_config=cfg.get('config',{}),logical_bytes=size,allocated_bytes=final.stat().st_blocks*512,preparation_ms=(time.monotonic()-started)*1000)
   atomic(p,row);shutil.rmtree(work)
  except Exception as e:
   row.update(state='failed',error=str(e));atomic(p,row);print(ident,e,flush=True)
if __name__=='__main__':main()
