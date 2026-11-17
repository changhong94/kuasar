# SPDX-License-Identifier: Apache-2.0
"""Single-use, image-independent preattached raw devices on a reflink filesystem.

Only a durably private slot may be bound. The device has never had a filesystem
mounted; guest cache invalidation is mandatory before the first mount. Once
bound the inode is immutable, including across live-parent forks.
"""
import fcntl
import hashlib
import os
from pathlib import Path
import shutil
import struct
import time

FICLONERANGE = 0x4020940D
CAPACITY = 16 * 1024**3

def sync_dir(path):
    fd = os.open(path, os.O_DIRECTORY | os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)

class SlotDisks:
    def __init__(self, path, catalog):
        started = time.monotonic()
        self.path = Path(path)
        self.path.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.cache = self.path/'images'
        self.cache.mkdir(mode=0o700, exist_ok=True)
        self.capacity = max(CAPACITY, *(Path(i["disk"]).stat().st_size for i in catalog.values()))
        for image in catalog.values():
            source = Path(image['disk'])
            if source.stat().st_size % 4096:
                raise ValueError('Prepared device requires a page-aligned image')
            target = self.cache/(image['sha256']+'.raw')
            if not target.exists():
                temp = target.with_suffix('.tmp')
                with source.open('rb') as src, temp.open('wb') as dst:
                    shutil.copyfileobj(src, dst, 1024*1024)
                    dst.flush(); os.fchmod(dst.fileno(), 0o400); os.fsync(dst.fileno())
                os.replace(temp, target); sync_dir(self.cache)
            with target.open('rb') as f:
                if hashlib.file_digest(f, 'sha256').hexdigest() != image['sha256']:
                    raise ValueError('Prepared image cache checksum mismatch')
        sync_dir(self.path)
        self.preparation_ms = (time.monotonic()-started)*1000
        import json
        (self.path/'preparation.json').write_text(json.dumps(dict(
            cache_validation_ms=self.preparation_ms, images=len(catalog),
            device_capacity_bytes=self.capacity,
            cached_image_bytes=sum(p.stat().st_size for p in self.cache.glob('*.raw')))))

    def create(self, path):
        with open(path, 'xb') as f:
            os.fchmod(f.fileno(), 0o600)
            f.truncate(self.capacity); f.flush(); os.fsync(f.fileno())
        sync_dir(self.path)

    def bind(self, path, image):
        # No fallback to full copying on lease. Reject non-reflink filesystems.
        with open(path, 'r+b') as dst, (self.cache/(image['sha256']+'.raw')).open('rb') as src:
            if os.fstat(dst.fileno()).st_mode & 0o222 == 0:
                raise RuntimeError('Slot disk already sealed')
            length = os.fstat(src.fileno()).st_size
            fcntl.ioctl(dst.fileno(), FICLONERANGE, struct.pack('qQQQ', src.fileno(), 0, length, 0))
            os.fchmod(dst.fileno(), 0o400)
            os.fsync(dst.fileno())  # data/extents and seal durable before guest mount

    def collect(self, referenced):
        removed = False
        for path in self.path.glob('e2b-*.raw'):
            if str(path) not in referenced:
                path.unlink(); removed = True
        if removed: sync_dir(self.path)
