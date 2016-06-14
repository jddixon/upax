#!/usr/bin/env python3

# testUpaxNode.py
import time
import unittest
from rnglib import SimpleRNG
from upax.node import *
from upax.ftlog import LogEntry

rng = SimpleRNG(int(time.time()))


class TestUpaxNode (unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # tests both sha1 and sha3 versions of code
    def doTestCheckNodeID(self, usingSHA1):
        for i in range(15):
            count = i + 18
            nodeID = bytearray(count)
            if usingSHA1 and count == 20:
                try:
                    checkNodeID(nodeID, usingSHA1)
                except ValueError as ve:
                    self.fail('unexpected value error on %s' % nodeID)

            elif not usingSHA1 and count == 32:
                try:
                    checkNodeID(nodeID, usingSHA1)
                except ValueError as ve:
                    self.fail('unexpected value error on %s' % nodeID)
            else:
                try:
                    checkNodeID(nodeID, usingSHA1)
                    self.fail('expected value error on %s' % nodeID)
                except ValueError as ve:
                    pass

    def testCheckNodeID(self):
        self.doTestCheckNodeID(True)
        self.doTestCheckNodeID(False)

    # ---------------------------------------------------------------

    def doTestPeer(self, usingSHA1):
        """ simple integrity checks on Peer type"""
        if usingSHA1:
            nodeID = bytearray(20)
        else:
            nodeID = bytearray(32)
        rng.nextBytes(nodeID)
        pubKey = bytearray(162)         # number not to be taken seriously
        rng.nextBytes(pubKey)
        peer = Peer(nodeID, pubKey, usingSHA1)    # True means using SHA1
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
        peer2 = Peer(nodeID, pubKey, usingSHA1)          # True = sha1
        try:
            peer2.nodeNdx = 'sugar'
            self.fail('successfully set nodeNdx to string value')
        except:
            pass
        peer3 = Peer(nodeID, pubKey, usingSHA1)
        try:
            peer3.nodeNdx = -19
            self.fail('successfully set nodeNdx to negative number')
        except:
            pass

    def testPeer(self):
        self.doTestPeer(True)
        self.doTestPeer(False)

    # ---------------------------------------------------------------

    def testUpaxNode(self):
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
