# SPDX-License-Identifier: Apache-2.0
"""OCI image execution defaults, kept independently of generic VM slots."""
import re


def normalize(config):
    result = {}
    for field in ('Env', 'Entrypoint', 'Cmd'):
        value = config.get(field) or []
        if not isinstance(value, list) or any(not isinstance(x, str) or '\0' in x for x in value):
            raise ValueError('Invalid OCI '+field)
        result[field] = value
    for entry in result['Env']:
        if '=' not in entry or not entry.split('=', 1)[0]:
            raise ValueError('Invalid OCI environment entry')
    for field, default in [('WorkingDir', '/'), ('User', '0')]:
        value = config.get(field) or default
        if not isinstance(value, str) or '\0' in value or len(value) > 4096:
            raise ValueError('Invalid OCI '+field)
        result[field] = value
    if not result['WorkingDir'].startswith('/'):
        raise ValueError('OCI WorkingDir must be absolute')
    if not re.fullmatch(r'[^:\s]+(?::[^:\s]+)?', result['User']):
        raise ValueError('Invalid OCI User')
    if not result['Entrypoint'] + result['Cmd']:
        raise ValueError('OCI image has no executable command')
    if not (result['Entrypoint'] + result['Cmd'])[0]:
        raise ValueError('Empty OCI executable')
    return result


def launch_argv(config, *, command=None, cwd=None, user=None, env=None, start=False):
    values = dict(x.split('=', 1) for x in config['Env'])
    values.update(env or {})
    return ['/usr/bin/oci-launch', 'start' if start else 'exec', '/run/image-root',
            '/run/oci-entrypoint' if start else '-', cwd or config['WorkingDir'],
            config['User'] if user is None else user, str(len(values)),
            *[k+'='+v for k, v in values.items()], '--',
            *(config['Entrypoint'] + config['Cmd'] if command is None else command)]
