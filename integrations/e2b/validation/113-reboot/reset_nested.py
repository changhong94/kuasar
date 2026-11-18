"""QMP reset only of the verified, owned nested host; never reboot physical 113."""
from pathlib import Path
import socket,json
b=Path('/home/hong/src/research/fork-features-113/nested');pid=int((b/'qemu.pid').read_text());cmd=Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')
assert b'fork-reboot-test' in cmd and str(b/'data.raw').encode() in b' '.join(cmd)
s=socket.socket(socket.AF_UNIX);s.connect(str(b/'qmp.sock'));f=s.makefile('rwb',buffering=0)
assert 'QMP' in json.loads(f.readline())
for command in ['qmp_capabilities','system_reset']:
 f.write((json.dumps({'execute':command})+'\n').encode())
 while True:
  r=json.loads(f.readline())
  if 'return' in r:break
  if 'error' in r:raise RuntimeError(r)
print('Nested host reset',pid)
