#!/usr/bin/env python3
import concurrent.futures, subprocess,os
from pathlib import Path
from import_images import IMAGES

def run(item):
 return subprocess.run(['/usr/bin/python3',str(Path(__file__).with_name('import_images.py'))],env={**os.environ,'FORKLAB_ONLY_IMAGE':item[0]}).returncode
with concurrent.futures.ThreadPoolExecutor(2) as pool:
 results=list(pool.map(run,IMAGES))
if any(results):raise SystemExit(1)
