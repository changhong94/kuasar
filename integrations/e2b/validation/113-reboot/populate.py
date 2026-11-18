from pathlib import Path
import os,subprocess as sp,shutil,secrets
B=Path('/home/hong/src/research/fork-features-113/nested');R=B/'root';F=Path('/home/hong/src/research/python-latest-113');Q=F/'src/kuasar-fork/kuasar-io/kuasar'
def run(*args):sp.run(list(map(str,args)),check=True)
run('mount','-o','loop',B/'root.raw',R)
try:
 def copy(src):
  src=Path(src);dst=R/str(src).lstrip('/');dst.parent.mkdir(parents=True,exist_ok=True)
  run('rsync','-aS','--exclude=.git','--exclude=target','--exclude=__pycache__',src,dst.parent)
 for src in [F/'assets',F/'deploy/fork',Q,Path('/home/hong/deploy/comparison/venv'),Path('/home/hong/src/research/sixway-languages-20261117/assets')]:copy(src)
 target=R/str(F).lstrip('/');(target/'venv').symlink_to('/home/hong/deploy/comparison/venv');(target/'data').mkdir();(target/'control/fork-sqlite').mkdir(parents=True)
 run('mount','-o','loop',B/'data.raw',target/'data')
 try:
  data=target/'data/fork';data.mkdir();(data/'state').mkdir();shutil.copyfile(F/'data/fork/writable-template.raw',data/'writable-template.raw');(data/'writable-template.raw').chmod(0o400)
 finally:run('umount',target/'data')
 supervisor=target/'deploy/fork/supervisor.py'
 source=supervisor.read_text()
 needle="z=sp.run([str(B/'assets/cni/bridge')],input=json.dumps(cni),text=True,capture_output=True,env=ce,check=True)"
 assert needle in source
 source=source.replace(needle,"sp.run([str(B/'assets/cni/bridge')],input=json.dumps(cni),text=True,capture_output=True,env={**ce,'CNI_COMMAND':'DEL'},check=True)\n"+needle)
 supervisor.write_text(source)
 (target/'deploy/fork/api-key').write_text('e2b_'+secrets.token_hex(20));(target/'deploy/fork/api-key').chmod(0o600)
 unit='''[Unit]
Description=Owned nested checkpoint recovery sandboxer
After=nested-network.service local-fs.target
Requires=nested-network.service
RequiresMountsFor=/home/hong/src/research/python-latest-113/data
[Service]
Type=simple
ExecStartPre=/usr/bin/mkdir -p /run/containerd
Environment=E2B_DURABLE_STORE=sqlite
Environment=KUASAR_FORK_RUNTIME_DIR=/run/python113-fork
Environment=E2B_TEMPLATE_POOL_SIZE=2
Environment=E2B_IMAGE_CATALOG=/home/hong/src/research/python-latest-113/deploy/fork/images.json
Environment=E2B_DURABLE_SQLITE_ROOT=/home/hong/src/research/python-latest-113/control/fork-sqlite
Environment=E2B_SNAPSHOT_POOL=1
Environment=E2B_WRITABLE_BASE=/home/hong/src/research/python-latest-113/data/fork/writable-template.raw
Environment=E2B_TEMPLATE_METADATA=1
Environment=E2B_TEMPLATE_READ_CACHE_BYTES=536870912
ExecStart=/usr/bin/python3 /home/hong/src/research/python-latest-113/deploy/fork/supervisor.py
TimeoutStopSec=30
[Install]
WantedBy=multi-user.target
'''
 (R/'etc/systemd/system/nested-fork.service').write_text(unit)
 # Enable only after the new recovery binary has been deployed inside the guest.
finally:run('umount',R)
