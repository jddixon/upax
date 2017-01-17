#!/usr/bin/env python3

# ~/dev/py/upax/testUWalker.py

import os
# import re
import time
import unittest

from upax.walker import UWalker
from rnglib import SimpleRNG
from xlattice import HashTypes

RNG = SimpleRNG(time.time())


class TestUWalker(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # utility functions #############################################

    # actual unit tests #############################################
    def test_walking_empty_dir(self):
        u_path = 'uDir'
        if not os.path.exists(u_path):
            os.makedirs(u_path)
        walker = UWalker(u_path)
        self.assertEqual(u_path, walker.u_path)
        self.assertEqual(0, walker.count)

    # ---------------------------------------------------------------

    def do_test_walking_real_dir(self, hashtype):
        u_path = '/var/U'
        limit = 1000
        start_at = 'a0'
        if not os.path.exists(u_path):
            os.makedirs(u_path)
        walker = UWalker(
            u_path=u_path,
            limit=limit,
            start_at=start_at,
            hashtype=hashtype)
        self.assertEqual(u_path, walker.u_path)
        self.assertEqual(0, walker.count)
        self.assertEqual(limit, walker.limit)

        keys = walker.walk()
        count = len(keys)
        self.assertEqual(count, walker.count)
        # print(("\nFOUND %u ENTRIES IN RANGE" % count))         # DEBUG

        #print(("THE FIRST SIXTEEN KEYS IN %s FROM %s" % (uDir, startAt)))
        # for i in range(16):
        #    p = w.keys[i]
        #    print(('%s' % p))

    def test_walking_real_dir(self):
        self.do_test_walking_real_dir(HashTypes.SHA1)
        # the real directory used actually uses SHA1
        # self.doTestWalkingRealDir(False)

if __name__ == '__main__':
    unittest.main()
