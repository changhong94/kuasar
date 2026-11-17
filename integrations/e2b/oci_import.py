#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Import one OCI image into an immutable filesystem + execution-config catalog.
Uses skopeo for registry/platform selection and umoci for verified layer unpacking.
The catalog is durable and atomic. POST /images/reload adds new image IDs without rebuilding slots.
"""
import argparse
import fcntl
import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from oci_config import normalize


def sync_dir(path):
    fd = os.open(path, os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def digest(path):
    with path.open('rb') as f: return hashlib.file_digest(f, 'sha256').hexdigest()


def blob(layout, descriptor):
    algorithm, value = descriptor['digest'].split(':', 1)
    if algorithm != 'sha256' or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('Invalid OCI digest')
    path = layout/'blobs'/algorithm/value
    if path.stat().st_size != descriptor['size'] or digest(path) != value:
        raise ValueError('OCI descriptor integrity failure')
    return path


def run(args):
    started = time.monotonic(); args.store.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix='.import-', dir=args.store))
    published = False
    try:
        layout = staging/'oci'
        subprocess.run([args.skopeo, '--override-os', 'linux', '--override-arch', args.architecture,
                        'copy', args.source, 'oci:'+str(layout)+':image'], check=True)
        index = json.loads((layout/'index.json').read_text())
        if len(index['manifests']) != 1: raise ValueError('Expected one resolved image manifest')
        desc = index['manifests'][0]; manifest = json.loads(blob(layout, desc).read_text())
        raw_config = json.loads(blob(layout, manifest['config']).read_text())
        if raw_config.get('os') != 'linux' or raw_config.get('architecture') != args.architecture:
            raise ValueError('Image platform does not match the selected runtime architecture')
        for layer in manifest['layers']: blob(layout, layer)
        config = normalize(raw_config.get('config', {}))
        subprocess.run([args.umoci, 'unpack', '--image', str(layout)+':image', str(staging/'bundle')], check=True)
        reserved = staging/'bundle/rootfs/.__kuasar'
        if reserved.exists() or reserved.is_symlink():
            raise ValueError('Image uses reserved runtime path /.__kuasar')
        unpack_ms = (time.monotonic()-started)*1000
        disk = staging/('root.'+args.filesystem)
        if args.filesystem == 'ext4':
            subprocess.run(['truncate', '-s', str(args.disk_mib)+'M', str(disk)], check=True)
            subprocess.run(['mkfs.ext4', '-q', '-F', '-d', str(staging/'bundle/rootfs'), str(disk)], check=True)
        else:
            # Require a tool that preserves OCI mtimes. Never silently degrade
            # metadata or select filesystem layout based on image names/files.
            subprocess.run([args.mkfs_erofs, '--preserve-mtime', str(disk),
                            str(staging/'bundle/rootfs')], check=True)
        disk.chmod(0o444)
        with disk.open('rb') as f: os.fsync(f.fileno())
        disk_hash = digest(disk)
        identity = hashlib.sha256((desc['digest']+disk_hash).encode()).hexdigest()
        final = args.store/identity
        record = dict(disk=str(final/disk.name), sha256=disk_hash,
                      manifest=desc['digest'], config_digest=manifest['config']['digest'],
                      source=args.source, oci_config=config, filesystem=args.filesystem, architecture=args.architecture,
                      preparation_ms=(time.monotonic()-started)*1000, unpack_ms=unpack_ms)
        (staging/'image.json').write_text(json.dumps(record, indent=2))
        with (staging/'image.json').open('rb') as f: os.fsync(f.fileno())
        sync_dir(staging)
        os.rename(staging, final); published = True; sync_dir(args.store)
        args.catalog.parent.mkdir(parents=True, exist_ok=True)
        with open(str(args.catalog)+'.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            catalog = json.loads(args.catalog.read_text()) if args.catalog.exists() else {}
            catalog[args.name] = record
            fd, tmp = tempfile.mkstemp(prefix='.catalog-', dir=args.catalog.parent)
            try:
                with os.fdopen(fd, 'w') as f:
                    json.dump(catalog, f, indent=2); f.flush(); os.fsync(f.fileno())
                os.replace(tmp, args.catalog); sync_dir(args.catalog.parent)
            finally:
                if os.path.exists(tmp): os.unlink(tmp)
        print(json.dumps(record, indent=2))
    finally:
        if not published: shutil.rmtree(staging, ignore_errors=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('source'); p.add_argument('--name', required=True)
    p.add_argument('--catalog', type=Path, required=True); p.add_argument('--store', type=Path, required=True)
    p.add_argument('--filesystem',choices=['ext4','erofs'],default='ext4')
    p.add_argument('--mkfs-erofs',default='mkfs.erofs')
    p.add_argument('--architecture',choices=['amd64','arm64'],default={'x86_64':'amd64','aarch64':'arm64'}.get(platform.machine()))
    p.add_argument('--disk-mib', type=int, default=1024)
    p.add_argument('--skopeo', default='skopeo'); p.add_argument('--umoci', default='umoci')
    a = p.parse_args()
    if a.architecture is None:p.error('select a supported runtime architecture')
    if not 64 <= a.disk_mib <= 65536: p.error('disk-mib must be between 64 and 65536')
    a.catalog=a.catalog.resolve(); a.store=a.store.resolve(); run(a)
