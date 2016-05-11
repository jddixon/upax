#!/usr/bin/python3

# ~/dev/py/upax/testUWalker.py

import os
import re
import time
import unittest

from upax import *
from upax.ftlog import *
from upax.walker import UWalker
from rnglib import SimpleRNG

rng = SimpleRNG(time.time())


class TestUWalker (unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # utility functions #############################################

    # actual unit tests #############################################
    def testWalkingEmptyDir(self):
        uDir = 'uDir'
        if not os.path.exists(uDir):
            os.makedirs(uDir)
        w = UWalker(uDir)
        self.assertEqual(uDir, w.uDir)
        self.assertEqual(0, w.count)

    def testWalkingRealDir(self):
        uDir = '/var/U'
        limit = 100              # 1000
        startAt = 'a0'
        if not os.path.exists(uDir):
            os.makedirs(uDir)
        w = UWalker(uDir=uDir, limit=limit, startAt=startAt, usingSHA1=True)
        self.assertEqual(uDir, w.uDir)
        self.assertEqual(0, w.count)
        self.assertEqual(limit, w.limit)

        keys = w.walk()
        count = len(keys)
        self.assertEqual(count, w.count)
        print(("\nFOUND %u ENTRIES IN RANGE" % count))         # DEBUG

        print(("THE FIRST SIXTEEN KEYS IN %s FROM %s" % (uDir, startAt)))
        for i in range(16):
            p = w.keys[i]
            print(('%s' % p))


if __name__ == '__main__':
    unittest.main()
