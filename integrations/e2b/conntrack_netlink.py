# SPDX-License-Identifier: Apache-2.0
"""Delete only conntrack tuples containing one owned IPv4 address.

Use filtered kernel dumps and exact tuple+ID deletions, never a host table flush.
Requires CTA_FILTER support; all malformed/interrupted/error replies fail closed.
"""
import errno
import ipaddress
import socket
import struct
import time


def attr(kind, payload):
    size = len(payload)+4
    return struct.pack('=HH', size, kind)+payload+b'\0'*((-size) % 4)


def attributes(data):
    result = {}
    while data:
        if len(data) < 4:
            raise ValueError('short conntrack attribute')
        size, kind = struct.unpack_from('=HH', data)
        if size < 4 or size > len(data):
            raise ValueError('invalid conntrack attribute length')
        key = kind & 0x3fff
        if key in result:
            raise ValueError('duplicate conntrack attribute')
        result[key] = (kind, data[4:size])
        data = data[(size+3) & ~3:]
    return result


def deletion_payload(payload, packed_ip, side):
    if len(payload) < 4 or payload[0] != socket.AF_INET:
        raise ValueError('unexpected conntrack address family')
    fields = attributes(payload[4:])
    original = fields[1]
    addresses = attributes(attributes(original[1])[1][1])
    if addresses.get(side, (None, None))[1] != packed_ip:
        raise ValueError('kernel returned a conntrack tuple outside the owned filter')
    identity = fields[12]
    if len(identity[1]) != 4:
        raise ValueError('missing conntrack identity')
    # Exact tuple plus ID prevents deletion of a different entry that reused
    # the tuple after this dump. Preserve the conntrack zone when present.
    selected = [original, identity]
    if 18 in fields:
        selected.append(fields[18])
    return payload[:4]+b''.join(attr(kind, value) for kind, value in selected)


class Conntrack:
    def __enter__(self):
        self.sock = socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, 12)
        try:
            self.sock.bind((0, 0))
            self.sock.settimeout(15)
        except BaseException:
            self.sock.close()
            raise
        self.seq = 0
        self.deadline = time.monotonic()+15
        return self

    def __exit__(self, *args):
        self.sock.close()

    def request(self, kind, flags, payload):
        self.seq += 1
        seq = self.seq
        header = struct.pack('=IHHII', 16+len(payload), kind, flags, seq, 0)
        self.sock.sendto(header+payload, (0, 0))
        while True:
            left = self.deadline-time.monotonic()
            if left <= 0:
                raise TimeoutError('conntrack cleanup deadline exceeded')
            self.sock.settimeout(left)
            data, _, message_flags, sender = self.sock.recvmsg(262144)
            if sender[0] != 0 or message_flags & socket.MSG_TRUNC:
                raise ValueError('invalid conntrack sender or truncated message')
            while data:
                if len(data) < 16:
                    raise ValueError('short conntrack netlink header')
                size, reply_kind, reply_flags, reply_seq, _ = struct.unpack_from('=IHHII', data)
                if size < 16 or size > len(data) or reply_seq != seq or reply_flags & 0x10:
                    raise ValueError('invalid or interrupted conntrack reply')
                body = data[16:size]
                data = data[(size+3) & ~3:]
                if reply_kind in (2, 3):
                    if len(body) < 4:
                        raise ValueError('short conntrack completion')
                    error = struct.unpack_from('=i', body)[0]
                    if error:
                        raise OSError(-error, 'conntrack netlink request rejected')
                    return
                if reply_kind != 0x100:
                    raise ValueError('unexpected conntrack reply kind')
                yield body

    def delete_ip(self, address):
        packed = ipaddress.IPv4Address(address).packed
        targets = set()
        for side, flag in [(1, 1), (2, 2)]:
            tuple_filter = attr(0x8001, attr(0x8001, attr(side, packed)))
            filter_flags = attr(0x8019, attr(1, struct.pack('=I', flag)))
            payload = bytes([socket.AF_INET, 0, 0, 0])+tuple_filter+filter_flags
            for entry in self.request(0x101, 0x301, payload):
                targets.add(deletion_payload(entry, packed, side))
        for payload in targets:
            try:
                list(self.request(0x102, 5, payload))
            except OSError as error:
                if error.errno != errno.ENOENT:
                    raise
        return len(targets)


def delete_ip(address):
    with Conntrack() as client:
        return client.delete_ip(address)
