#!/usr/bin/env python3

# ~/dev/py/upax/testUWalker.py

import os
import re
import time
import unittest

from upax.walker import UWalker
from rnglib import SimpleRNG
from xlattice import QQQ

RNG = SimpleRNG(time.time())


class TestUWalker(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # utility functions #############################################

    # actual unit tests #############################################
    def test_walking_empty_dir(self):
        u_dir = 'u_dir'
        if not os.path.exists(u_dir):
            os.makedirs(u_dir)
        www = UWalker(u_dir)
        self.assertEqual(u_dir, www.u_dir)
        self.assertEqual(0, www.count)

    # ---------------------------------------------------------------

    def do_test_walking_real_dir(self, using_sha):
        u_dir = '/var/U'
        limit = 100              # 1000
        start_at = 'a0'
        if not os.path.exists(u_dir):
            os.makedirs(u_dir)
        www = UWalker(
            u_dir=u_dir,
            limit=limit,
            start_at=start_at,
            using_sha=using_sha)
        self.assertEqual(u_dir, www.u_dir)
        self.assertEqual(0, www.count)
        self.assertEqual(limit, www.limit)

        keys = www.walk()
        count = len(keys)
        self.assertEqual(count, www.count)
        # print(("\nFOUND %u ENTRIES IN RANGE" % count))         # DEBUG

        #print(("THE FIRST SIXTEEN KEYS IN %s FROM %s" % (uDir, startAt)))
        # for i in range(16):
        #    p = w.keys[i]
        #    print(('%s' % p))

    def test_walking_real_dir(self):
        self.do_test_walking_real_dir(True)
        # the real directory used actually uses SHA1
        # self.doTestWalkingRealDir(False)

if __name__ == '__main__':
    unittest.main()
