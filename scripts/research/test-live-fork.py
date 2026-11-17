#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Drive Kuasar's real admin service; no direct CH fork calls or RAM writes."""
import json, os, select, signal, socket, subprocess, sys, time, re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

binary, ch, kernel, initramfs, directory = sys.argv[1:]
out = Path(directory).resolve()
out.mkdir(parents=True, exist_ok=True)
state = out / 'state'
state.mkdir(exist_ok=True)
admin = out / 'admin.sock'
disk = out / 'disk.raw'
with disk.open('wb') as f:
    f.write(b'fork-device-state')
    f.truncate(1024 * 1024)
config = out / 'config.toml'
config.write_text(f'''[sandbox]
log_level = "info"
enable_tracing = false
[hypervisor]
live_fork = true
path = {json.dumps(ch)}
vcpus = 2
memory_in_mb = 256
kernel_path = {json.dumps(kernel)}
initrd_path = {json.dumps(initramfs)}
image_path = {json.dumps(str(disk))}
kernel_params = "console=ttyS0 panic=-1 rdinit=/init"
hugepages = false
entropy_source = "/dev/urandom"
container_storage_backend = "virtio-blk"
[hypervisor.task]
debug = false
enable_tracing = false
''')


def call(action, ok=True, **args):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(60)
        s.connect(str(admin))
        s.sendall((json.dumps(dict(action=action, **args))+'\n').encode())
        data = b''
        while not data.endswith(b'\n'):
            chunk = s.recv(65536)
            assert chunk, data
            data += chunk
    result = json.loads(data)
    if ok is not None:
        assert result['ok'] == ok, result
    return result


def wait_state(id, desired):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        data = call('sandbox-get', sandbox_id=id)
        if data['status'] == desired:
            return data
        time.sleep(.05)
    raise AssertionError((id, desired, data))


class Serial:
    def __init__(self, path, name):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(5)
        self.sock.connect(path)
        self.log = (out / name).open('w')
        self.data = ''

    def wait(self, value, fresh=False):
        if fresh:
            self.data = ''
        deadline = time.monotonic() + 45
        while value not in self.data and time.monotonic() < deadline:
            if not select.select([self.sock], [], [], .2)[0]:
                continue
            data = self.sock.recv(65536)
            assert data, ('disconnected', value, self.data[-1000:])
            data = data.decode(errors='replace')
            self.log.write(data)
            self.log.flush()
            self.data += data
        assert value in self.data, (value, self.data[-1500:])

    def close(self):
        self.sock.close()
        self.log.close()


log = (out / 'sandboxer.log').open('w')
proc = subprocess.Popen([binary, '--config', str(config), '--dir', str(state),
                         '--listen', str(out / 'sandboxer.sock'), '--admin-listen', str(admin)],
                        stdout=log, stderr=log, start_new_session=True)
connections = []
try:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and not admin.exists():
        assert proc.poll() is None, (out / 'sandboxer.log').read_text()[-4000:]
        time.sleep(.05)
    seed_id, child_id, grand_id = 'seed00000001', 'child0000001', 'grand0000001'
    seed = call('sandbox-boot', sandbox_id=seed_id)
    parent = Serial(seed['live_fork']['serial_socket'], 'seed.log')
    connections.append(parent)
    parent.wait('FORK_READY ')
    call('sandbox-fork', source_id=seed_id, sandbox_id=seed_id, ok=False)
    call('sandbox-fork', source_id='missing00001', sandbox_id=child_id, ok=False)
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=2) as executor:
        attempts = list(executor.map(lambda _: call('sandbox-fork', ok=None,
                        source_id=seed_id, sandbox_id=child_id), range(2)))
    assert sorted(r['ok'] for r in attempts) == [False, True], attempts
    child_info = next(r for r in attempts if r['ok'])
    latency_ms = (time.monotonic() - started) * 1000
    parent.close()
    connections.remove(parent)
    parent = Serial(seed['live_fork']['serial_socket'], 'parent.log')
    child = Serial(child_info['live_fork']['serial_socket'], 'child.log')
    connections.extend([parent, child])
    parent.sock.sendall(b'20\n')
    parent.wait('FORK_RESULT initial=10 value=20')
    child.sock.sendall(b'30\n')
    child.wait('FORK_RESULT initial=10 value=30')
    parent.wait('FORK_ALIVE value=20', fresh=True)
    child.wait('FORK_ALIVE value=30', fresh=True)
    call('sandbox-fork', source_id=seed_id, sandbox_id=child_id, ok=False)
    assert wait_state(seed_id, 'running')['live_fork']['pid'] != wait_state(child_id, 'running')['live_fork']['pid']
    for id in (seed_id, child_id):
        saved = json.loads((state / id / 'sandbox.json').read_text())
        assert saved['id'] == id and saved['data']['id'] == id
        assert not saved['data']['task_address'] and saved['vm']['live_fork']
    # Destroy the source sandbox completely; the child's PID, RAM, disk and
    # lifecycle must not depend on the source directory or process.
    parent_pid = seed['live_fork']['pid']
    child_pid = child_info['live_fork']['pid']
    call('sandbox-destroy', sandbox_id=seed_id)
    assert not (state / seed_id).exists()
    assert not Path(f'/proc/{parent_pid}').exists()
    child.wait('FORK_ALIVE value=30', fresh=True)
    assert wait_state(child_id, 'running')['live_fork']['pid'] == child_pid
    # The adopted, non-direct-child VMM can fork again through Kuasar.
    grand_info = call('sandbox-fork', source_id=child_id, sandbox_id=grand_id)
    child.close()
    connections.remove(child)
    child = Serial(child_info['live_fork']['serial_socket'], 'child-second.log')
    grand = Serial(grand_info['live_fork']['serial_socket'], 'grand.log')
    connections.extend([child, grand])
    child.wait('FORK_ALIVE value=30')
    grand.wait('FORK_ALIVE value=30')
    # Destroy the descendant first: the source remains live.
    grand_pid = grand_info['live_fork']['pid']
    call('sandbox-destroy', sandbox_id=grand_id)
    assert not (state / grand_id).exists()
    child.wait('FORK_ALIVE value=30', fresh=True)
    # Unexpected exit is detected through pidfd, not waitpid(parent).
    os.kill(child_pid, signal.SIGKILL)
    wait_state(child_id, 'stopped')
    call('sandbox-fork', source_id=child_id, sandbox_id='failed000001', ok=False)
    assert not (state / 'failed000001').exists()
    preserved = state / 'existing0001'
    preserved.mkdir()
    (preserved / 'sentinel').write_text('preserve')
    call('sandbox-boot', sandbox_id='existing0001', ok=False)
    assert (preserved / 'sentinel').read_text() == 'preserve'
    (preserved / 'sentinel').unlink()
    preserved.rmdir()
    call('sandbox-destroy', sandbox_id=child_id)
    assert call('sandbox-list')['sandboxes'] == []
    assert not list(out.rglob('memory.img'))
    text = (out / 'sandboxer.log').read_text()
    pss = re.findall(r'combined_ram_pss_kib=(\d+) baseline_kib=(\d+)', text)
    assert len(pss) == 2, text[-3000:]
    assert all(abs(int(a)-int(b)) <= 16 for a,b in pss), pss
    result = {'result': 'PASS', 'fork_latency_ms': latency_ms,
              'parent_pid': parent_pid, 'child_pid': child_pid, 'grandchild_pid': grand_pid,
              'ram_pss_combined_baseline_kib': pss, 'child':child_info, 'grandchild':grand_info,
              'checks': ['Kuasar boot/fork/status/destroy', 'Linux CoW 10 -> 20/30',
                         'parent-first and child-first destruction', 'non-child PID monitoring',
                         'unique sandbox metadata', 'concurrent duplicate rejected',
                         'failed fork rollback and existing directory preserved',
                         'no memory.img or full RAM copy']}
    (out / 'results.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))
finally:
    for serial in connections:
        serial.close()
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()
    log.close()
