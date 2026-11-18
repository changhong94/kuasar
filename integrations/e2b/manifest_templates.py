# SPDX-License-Identifier: Apache-2.0
"""Manifest-backed templates with durable, private raw-file materialization.

This first backend fetches the full sparse template before KVM restore. It is
not demand paging: its fetch, verification and sync costs belong to create.
The content store is authoritative; raw files are private materializations.
"""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ManifestTemplates:
    def __init__(self, executable, config, root):
        self.executable = str(executable)
        self.config = str(config)
        self.root = Path(root).resolve(strict=True)

    def invoke(self, *args):
        result = subprocess.run(
            [self.executable, '--manifest-config', self.config, *map(str, args)],
            capture_output=True, timeout=600)
        if result.returncode:
            # Backend diagnostics can contain endpoints; never return config or
            # arbitrary subprocess stderr to the public create response.
            raise RuntimeError('Manifest template transfer failed')
        return json.loads(result.stdout)

    def publish(self, directory):
        directory = Path(directory).resolve(strict=True)
        if directory.parent != self.root:
            raise ValueError('Template is outside the configured root')
        started = time.monotonic()
        entries = []
        for path in sorted(directory.iterdir()):
            if path.is_symlink() or not path.is_file():
                raise ValueError('Template contains a non-regular file')
            info = self.invoke('store', path)
            entries.append(dict(name=path.name, manifest=info['manifest'],
                                logical_bytes=info['logical_bytes'], stats=info['stats']))
        required = {'config.json', 'state.json', 'manifest.json', 'memory-ranges'}
        if not required <= {e['name'] for e in entries}:
            raise ValueError('Incomplete VM template')
        return dict(version=1, source=str(directory), files=entries,
                    publication_ms=(time.monotonic()-started)*1000)

    def owned_path(self, key):
        if not re.fullmatch(r'remote-e2b-[0-9a-f]{24}', key):
            raise ValueError('Invalid private materialization identity')
        return self.root / key

    def restore(self, descriptor, key):
        destination = self.owned_path(key)
        if destination.exists():
            raise FileExistsError(destination)
        if descriptor.get('version') != 1:
            raise ValueError('Unsupported Manifest template descriptor')
        started = time.monotonic()
        temporary = Path(tempfile.mkdtemp(prefix='.'+key+'-', dir=self.root))
        copied = 0
        try:
            names = set()
            for entry in descriptor['files']:
                name = entry['name']
                if not isinstance(name, str) or Path(name).name != name or name in ('.', '..') or name in names:
                    raise ValueError('Invalid or duplicate template filename')
                names.add(name)
                info = self.invoke('load', entry['manifest'], temporary/name)
                if info['logical_bytes'] != entry['logical_bytes']:
                    raise ValueError('Restored file size mismatch')
                copied += info['materialized_data_bytes']
            if not {'config.json','state.json','manifest.json','memory-ranges'} <= names:
                raise ValueError('Incomplete restored VM template')
            source = descriptor['source']
            def relocate(value):
                if isinstance(value, str) and value.startswith(source+'/'):
                    suffix = value[len(source)+1:]
                    if suffix not in names:
                        raise ValueError('Template refers to an unpublished file')
                    return str(destination/suffix)
                if isinstance(value, list):
                    return [relocate(x) for x in value]
                if isinstance(value, dict):
                    return {k:relocate(v) for k,v in value.items()}
                return value
            path = temporary/'config.json'
            config = relocate(json.loads(path.read_text()))
            path.chmod(0o600)
            with path.open('w') as f:
                json.dump(config, f)
                f.flush()
                os.fchmod(f.fileno(), 0o400)
                os.fsync(f.fileno())
            sync_directory(temporary)
            # Destination is tied to a durably registered unique sandbox ID;
            # lifecycle serialization prevents concurrent publication for it.
            os.rename(temporary, destination)
            sync_directory(self.root)
            return dict(key=key, directory=str(destination), materialized_data_bytes=copied,
                        restore_ms=(time.monotonic()-started)*1000)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def collect(self, live_keys):
        # Caller holds the durable-records lock. Never remove native templates.
        for path in self.root.glob('remote-e2b-*'):
            if path.name not in live_keys:
                self.owned_path(path.name)
                if path.is_symlink():
                    raise ValueError('Private template unexpectedly became a symlink')
                shutil.rmtree(path)
        sync_directory(self.root)
