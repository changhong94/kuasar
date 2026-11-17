# SPDX-License-Identifier: Apache-2.0
"""Opt-in FD ownership snapshots outside the VM pause measurement."""
import json, os, subprocess
from pathlib import Path

GUEST = r'''for p in /proc/[0-9]*; do
[ -r "$p/comm" ] || continue
read name < "$p/comm"
echo "PROCESS ${p##*/} $name"
/bin/busybox grep 'Max open files' "$p/limits"
/bin/busybox ls -l "$p/fd" 2>/dev/null
done
echo EXEC_TIMINGS
/bin/busybox cat /run/kuasar-exec-timing.jsonl 2>/dev/null
true'''

def capture(out, repo, entry, generation, role="current"):
    label = f"{generation:03d}" + ("" if role == "current" else "-"+role)
    folder = out/'fd-census'; folder.mkdir(exist_ok=True)
    result = subprocess.run([str(repo/'target/debug/examples/fork-agent-client'), 'exec',
                             str(entry['dir']/'task.vsock'), GUEST], capture_output=True, text=True, timeout=30)
    (folder/f'{label}-guest.txt').write_text(result.stdout+'\nSTDERR\n'+result.stderr)
    hosts=[]
    for p in Path('/proc').iterdir():
        if not p.name.isdigit(): continue
        try:
            cmd=(p/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace')
            if str(out)+'/' not in cmd: continue
            fds={}
            for f in (p/'fd').iterdir():
                try: fds[f.name]=os.readlink(f)
                except OSError: pass
            hosts.append(dict(pid=int(p.name),command=cmd,limits=(p/'limits').read_text(),fds=fds))
        except OSError: pass
    (folder/f'{label}-host.json').write_text(json.dumps(hosts,indent=2))
    assert result.returncode==0, result.stderr
