#!/usr/bin/python3

# testUpaxNode3.py
import time
import unittest
from rnglib import SimpleRNG
from upax.node import *
from upax.ftlog import LogEntry

rng = SimpleRNG(int(time.time()))


class TestUpaxNode3 (unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testCheckNodeID(self):
        for i in range(15):
            count = i + 18
            nodeID = bytearray(count)
            # SIMPLIFIED - USE upax VERSION
            if count == 32:
                try:
                    checkNodeID(nodeID, False)
                except ValueError as ve:
                    self.fail('unexpected value error on %s' % nodeID)
            else:
                try:
                    checkNodeID(nodeID, False)
                    self.fail('expected value error on %s' % nodeID)
                except ValueError as ve:
                    pass

    def testPeer(self):
        """ simple integrity checks on Peer type"""
        nodeID = bytearray(32)          # NOTE 32
        rng.nextBytes(nodeID)
        pubKey = bytearray(162)         # number not to be taken seriously
        rng.nextBytes(pubKey)
        peer = Peer(nodeID, pubKey)
        self.assertEqual(nodeID, peer.nodeID)
        self.assertEqual(pubKey, peer.rsaPubKey)
        self.assertIsNone(peer.nodeNdx)
        peer.nodeNdx = 42
        try:
            peer.nodeNdx = 43
            self.fail("changed existing nodeID")
        except:
            pass
        self.assertEqual(42, peer.nodeNdx)

        # expect three empty lists
        self.assertEqual(0, len(peer.cnx))
        self.assertEqual(0, len(peer.ipAddr))
        self.assertEqual(0, len(peer.fqdn))

        # verify that nodeNdx must be non-negative integer
        peer2 = Peer(nodeID, pubKey)
        try:
            peer2.nodeNdx = 'sugar'
            self.fail('successfully set nodeNdx to string value')
        except:
            pass
        peer3 = Peer(nodeID, pubKey)
        try:
            peer3.nodeNdx = -19
            self.fail('successfully set nodeNdx to negative number')
        except:
            pass

    def testUpaxNode3(self):
        """
        """
        pass

    def testStringSerialization(self):
        pass

    def testEquals(self):
        pass

    def testLHash(self):
        pass

if __name__ == '__main__':
    unittest.main()
