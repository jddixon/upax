#!/usr/bin/env python3

# testUpaxNode.py
import time
import unittest
from rnglib import SimpleRNG
from upax.ftlog import LogEntry
from xlattice import QQQ, check_using_sha
from upax.node import check_node_id, Peer

RNG = SimpleRNG(int(time.time()))


class TestUpaxNode(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # tests both sha1 and sha3 versions of code
    def do_test_check_node_id(self, using_sha):
        check_using_sha(using_sha)
        for i in range(15):
            count = i + 18
            node_id = bytearray(count)
            if using_sha == QQQ.USING_SHA1 and count == 20:
                try:
                    check_node_id(node_id, using_sha)
                except ValueError as val_err:
                    self.fail('unexpected value error on %s' % node_id)

            elif (using_sha != QQQ.USING_SHA1) and count == 32:
                try:
                    check_node_id(node_id, using_sha)
                except ValueError as val_err:
                    self.fail('unexpected value error on %s' % node_id)
            else:
                try:
                    check_node_id(node_id, using_sha)
                    self.fail('expected value error on %s' % node_id)
                except ValueError as val_err:
                    pass

    def test_check_node_id(self):
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_check_node_id(using)

    # ---------------------------------------------------------------

    def do_test_peer(self, using_sha):
        """ simple integrity checks on Peer type"""

        check_using_sha(using_sha)
        if using_sha == QQQ.USING_SHA1:
            node_id = bytearray(20)
        else:
            # 32-byte key
            node_id = bytearray(32)
        RNG.next_bytes(node_id)
        pub_key = bytearray(162)         # number not to be taken seriously
        RNG.next_bytes(pub_key)
        peer = Peer(node_id, pub_key, using_sha)
        self.assertEqual(node_id, peer.node_id)
        self.assertEqual(pub_key, peer.rsa_pub_key)
        self.assertIsNone(peer.node_ndx)
        peer.node_ndx = 42
        try:
            peer.node_ndx = 43
            self.fail("changed existing nodeID")
        except:
            pass
        self.assertEqual(42, peer.node_ndx)

        # expect three empty lists
        self.assertEqual(0, len(peer.cnx))
        self.assertEqual(0, len(peer.ip_addr))
        self.assertEqual(0, len(peer.fqdn))

        # verify that nodeNdx must be non-negative integer
        peer2 = Peer(node_id, pub_key, using_sha)          # True = sha1
        try:
            peer2.node_ndx = 'sugar'
            self.fail('successfully set nodeNdx to string value')
        except:
            pass
        peer3 = Peer(node_id, pub_key, using_sha)
        try:
            peer3.node_ndx = -19
            self.fail('successfully set nodeNdx to negative number')
        except:
            pass

    def test_peer(self):
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_peer(using)

    # ---------------------------------------------------------------

    def test_upax_node(self):
        """
        """
        pass

    def test_string_serialization(self):
        pass

    def test_equals(self):
        pass

    def test_l_hash(self):
        pass

if __name__ == '__main__':
    unittest.main()
