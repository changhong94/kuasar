from pathlib import Path
import subprocess,time,sys
ssh='/home/hong/src/research/fork-features-113/nested/ssh'
probe="""import urllib.request,json
from pathlib import Path
p=Path('/home/hong/src/research/python-latest-113/deploy/fork')
r=urllib.request.Request('http://127.0.0.1:18887/template-pool',headers={'X-API-Key':(p/'api-key').read_text().strip()})
json.load(urllib.request.urlopen(r,timeout=2))
"""
end=time.monotonic()+240
while time.monotonic()<end:
 try:
  r=subprocess.run([ssh,'python3 -'],input=probe,text=True,capture_output=True,timeout=8)
  if r.returncode==0:break
 except subprocess.TimeoutExpired:pass
 time.sleep(2)
else:raise RuntimeError('nested API did not recover')
assert sys.argv[1] in ['reboot1','reboot2','reboot3']
subprocess.run([ssh,'/home/hong/deploy/comparison/venv/bin/python /root/test_reboot_guest.py verify '+sys.argv[1]],check=True)
