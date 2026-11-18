# SPDX-License-Identifier: Apache-2.0
import ipaddress
import struct
import unittest
from conntrack_netlink import attr, attributes, deletion_payload, Conntrack


def entry(source, dest, identity=77):
    ips=attr(1,ipaddress.IPv4Address(source).packed)+attr(2,ipaddress.IPv4Address(dest).packed)
    proto=attr(1,b'\x11')+attr(2,struct.pack('!H',1111))+attr(3,struct.pack('!H',2222))
    return b'\x02\0\0\0'+attr(0x8001,attr(0x8001,ips)+attr(0x8002,proto))+attr(12,struct.pack('!I',identity))+attr(18,b'\0\0')


class ConntrackTests(unittest.TestCase):
    def test_unowned_tuple_cannot_be_deleted(self):
        with self.assertRaises(ValueError):
            deletion_payload(entry('10.0.0.2','10.0.0.3'),ipaddress.IPv4Address('10.0.0.4').packed,1)

    def test_exact_deletion_keeps_id_zone_and_tuple(self):
        original=entry('10.0.0.2','10.0.0.3')
        payload=deletion_payload(original,ipaddress.IPv4Address('10.0.0.2').packed,1)
        self.assertEqual(payload,original)
        self.assertEqual(set(attributes(payload[4:])),{1,12,18})

    def test_failed_dump_never_sends_delete(self):
        client=Conntrack();calls=[]
        def request(kind, flags, payload):
            calls.append(kind)
            yield entry('10.0.0.2','10.0.0.3')
            raise OSError('dump interrupted')
        client.request=request
        with self.assertRaises(OSError):client.delete_ip('10.0.0.2')
        self.assertEqual(calls,[0x101])

    def test_empty_dump_never_issues_unscoped_delete(self):
        client=Conntrack();calls=[]
        def request(kind,flags,payload):
            calls.append(kind)
            return iter([])
        client.request=request
        self.assertEqual(client.delete_ip('10.0.0.2'),0)
        self.assertEqual(calls,[0x101])

    def test_one_dump_filters_both_sides_and_preserves_unrelated(self):
        client = Conntrack(); calls = []
        source = entry('10.0.0.2', '10.0.0.3', 1)
        dest = entry('10.0.0.3', '10.0.0.2', 2)
        both = entry('10.0.0.2', '10.0.0.2', 3)
        unrelated = entry('10.0.0.3', '10.0.0.4', 4)
        def request(kind, flags, payload):
            calls.append((kind, payload))
            return iter([source, dest, both, unrelated] if kind == 0x101 else [])
        client.request = request
        self.assertEqual(client.delete_ip('10.0.0.2'), 3)
        self.assertEqual(sum(k == 0x101 for k, _ in calls), 1)
        self.assertEqual({p for k, p in calls if k == 0x102}, {source, dest, both})
