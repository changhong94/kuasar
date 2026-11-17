# SPDX-License-Identifier: Apache-2.0
"""Small bounded protobuf/ttrpc client for the guest's unary ExecVMProcess RPC.
Field numbers match vmm/common/src/protos/sandbox.proto and ttrpc Request/Response.
No SDK patch or guest network connection is involved.
"""
import json
import os
import socket
import struct
import time

LIMIT = 16 * 1024 * 1024

def varint(n):
    out = bytearray()
    while n > 127:
        out.append((n & 127) | 128)
        n >>= 7
    out.append(n)
    return bytes(out)

def integer(number, value):
    return varint(number << 3) + varint(value)

def blob(number, value):
    if isinstance(value, str):
        value = value.encode()
    return varint((number << 3) | 2) + varint(len(value)) + value

def fields(data):
    pos = 0
    def readint():
        nonlocal pos
        value = 0
        for shift in range(0, 70, 7):
            if pos >= len(data):
                raise ValueError('truncated protobuf varint')
            b = data[pos]
            pos += 1
            value |= (b & 127) << shift
            if not b & 128:
                return value
        raise ValueError('invalid protobuf varint')
    result = {}
    while pos < len(data):
        tag = readint()
        number, wire = tag >> 3, tag & 7
        if not number:
            raise ValueError('invalid protobuf field')
        if wire == 0:
            value = readint()
        else:
            if wire == 2:
                n = readint()
            elif wire in (1, 5):
                n = 8 if wire == 1 else 4
            else:
                raise ValueError('unsupported protobuf wire type')
            if pos + n > len(data):
                raise ValueError('truncated protobuf field')
            value = data[pos:pos+n]
            pos += n
        result[number] = value
    return result

def exact(sock, n):
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n-len(out))
        if not chunk:
            raise EOFError('guest RPC disconnected')
        out.extend(chunk)
    return bytes(out)

def execute(path, argv, stdin=b'', timeout=60):
    started_ns = time.perf_counter_ns()
    payload = b''.join(blob(3, a) for a in argv) + blob(2, stdin) + integer(5, 1)
    if os.environ.get('E2B_TRACE_TIMING'):
        payload += integer(4, 1)
    request = blob(1, 'grpc.SandboxService') + blob(2, 'ExecVMProcess') + blob(3, payload) + integer(4, int(timeout*1e9))
    if len(request) > LIMIT:
        raise ValueError('guest RPC request exceeds 16 MiB')
    with socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(timeout)
        sock.connect(str(path))
        connected_ns = time.perf_counter_ns()
        sock.sendall(b'CONNECT 1024\n')
        line = b''
        while not line.endswith(b'\n'):
            if len(line) > 128:
                raise ValueError('invalid vsock handshake')
            line += exact(sock, 1)
        if not line.startswith(b'OK '):
            raise ValueError('vsock handshake rejected')
        handshake_ns = time.perf_counter_ns()
        sock.sendall(struct.pack('>IIBB', len(request), 1, 1, 0) + request)
        n, stream, kind, flags = struct.unpack('>IIBB', exact(sock, 10))
        if n > LIMIT or stream != 1 or kind != 2 or flags:
            raise ValueError('invalid ttrpc reply header')
        reply = fields(exact(sock, n))
    status = fields(reply.get(1, b''))
    if status.get(1, 0):
        raise RuntimeError(status.get(2, b'guest RPC failed').decode(errors='replace'))
    response = fields(reply.get(2, b''))
    if not response.get(6):
        raise RuntimeError('guest agent lacks structured execution support')
    code = response.get(4, 0) & 0xffffffff
    if code >= 1 << 31:
        code -= 1 << 32
    result = {'stdout': response.get(1, b''), 'stderr': response.get(3, b''),
              'exit_code': code, 'pid': response.get(5, 0)}
    if os.environ.get('E2B_TRACE_TIMING'):
        result['timing'] = json.loads(response.get(2, b'{}'))
        result['timing']['host_connect_us'] = (connected_ns-started_ns)/1000
        result['timing']['host_handshake_us'] = (handshake_ns-connected_ns)/1000
        result['timing']['host_rpc_us'] = (time.perf_counter_ns()-handshake_ns)/1000
    return result

def admin(path, action, **kwargs):
    with socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(600 if action.startswith('sandbox-checkpoint-') else 180)
        sock.connect(str(path))
        sock.sendall((json.dumps(dict(action=action, **kwargs))+'\n').encode())
        data = sock.makefile('rb').readline(LIMIT+1)
    if len(data) > LIMIT:
        raise ValueError('oversized sandbox response')
    result = json.loads(data)
    if not result.get('ok'):
        raise RuntimeError(result.get('error', 'sandbox request failed'))
    return result


def execute_stream(path, argv, stdin=b'', timeout=60):
    """Bounded ttrpc server stream; deliver output before process termination."""
    started = time.perf_counter_ns()
    payload = b''.join(blob(3, a) for a in argv) + blob(2, stdin) + integer(5, 1)
    if os.environ.get('E2B_TRACE_TIMING'):
        payload += integer(4, 1)
    request = (blob(1, 'grpc.SandboxService') + blob(2, 'ExecVMProcessStreaming')
               + blob(3, payload) + integer(4, int(timeout*1e9)))
    if len(request) > LIMIT:
        raise ValueError('guest RPC request exceeds 16 MiB')
    with socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(timeout)
        sock.connect(str(path))
        connected = time.perf_counter_ns()
        sock.sendall(b'CONNECT 1024\n')
        line = b''
        while not line.endswith(b'\n'):
            if len(line) > 128:
                raise ValueError('invalid vsock handshake')
            line += exact(sock, 1)
        if not line.startswith(b'OK '):
            raise ValueError('vsock handshake rejected')
        handshaken = time.perf_counter_ns()
        sock.sendall(struct.pack('>IIBB', len(request), 1, 1, 1) + request)
        seen_start = seen_exit = False
        while True:
            n, stream, kind, flags = struct.unpack('>IIBB', exact(sock, 10))
            if n > LIMIT or stream != 1:
                raise ValueError('invalid ttrpc stream header')
            data = exact(sock, n)
            if kind == 2:
                status = fields(fields(data).get(1, b''))
                raise RuntimeError(status.get(2, b'guest stream failed').decode(errors='replace'))
            if kind != 3 or flags not in (0, 5):
                raise ValueError('invalid ttrpc stream frame')
            if flags == 5:
                if n or not seen_exit:
                    raise ValueError('guest stream closed without process exit')
                return
            event = fields(data)
            if seen_exit:
                raise ValueError('guest stream data after exit')
            if event.get(1):
                if seen_start:
                    raise ValueError('duplicate process start')
                seen_start = True
                yield {'pid': event[1]}
            elif event.get(4):
                if not seen_start:
                    raise ValueError('process exit before start')
                seen_exit = True
                code = event.get(5, 0) & 0xffffffff
                if code >= 1 << 31:
                    code -= 1 << 32
                timing = json.loads(event.get(6, b'{}'))
                timing.update(host_connect_us=(connected-started)/1000,
                              host_handshake_us=(handshaken-connected)/1000,
                              host_rpc_us=(time.perf_counter_ns()-handshaken)/1000)
                yield {'exit_code': code, 'timing': timing}
            else:
                if not seen_start or not (event.get(2) or event.get(3)):
                    raise ValueError('invalid process output event')
                yield {'stdout': event.get(2, b''), 'stderr': event.get(3, b'')}
