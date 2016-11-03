#!/usr/bin/env python3

# testUpaxServer.py

import os
import time
import unittest
import rnglib
import upax
from xlattice import u, QQQ, check_using_sha

RNG = rnglib.SimpleRNG(time.time())

DATA_PATH = 'myData'


class TestUpaxServer(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def do_test_construct_from_nothing(self, using_sha):
        check_using_sha(using_sha)
        # SETUP
        u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))
        while os.path.exists(u_path):
            u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))

        # we are guaranteed that uPath does _not_ exist
        string = upax.BlockingServer(u_path, using_sha)
        try:
            self.assertIsNotNone(string)
            self.assertTrue(os.path.exists(string.u_path))

            # subdirectories
            self.assertTrue(os.path.exists(os.path.join(u_path, 'in')))
            self.assertTrue(os.path.exists(os.path.join(u_path, 'tmp')))

            # nodeID
            id_path = os.path.join(u_path, 'node_id')
            self.assertTrue(os.path.exists(id_path))
            with open(id_path, 'rb') as file:
                node_id = file.read()
            if using_sha == QQQ.USING_SHA1:
                self.assertEqual(41, len(node_id))
                self.assertEqual(ord('\n'), node_id[40])
            else:
                # we only look at the hash length, so this is ok for
                # both SHA2 and SHA3
                self.assertEqual(65, len(node_id))
                self.assertEqual(ord('\n'), node_id[64])
            node_id = node_id[:-1]
        finally:
            string.close()
        self.assertTrue(os.path.exists(os.path.join(u_path, 'L')))   # GEEP

    def test_construct_from_nothing(self):
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_construct_from_nothing(using)

#   # ---------------------------------------------------------------

    def make_some_files(self, using_sha):
        """ return a map: hash=>path """

        check_using_sha(using_sha)

        # create a random number of unique data files of random length
        #   in myData/; take hash of each as it is created, using
        #   this to verify uniqueness; add hash to list (or use hash
        #   as key to map
        file_count = 3 + RNG.next_int16(16)
        files = {}             # a map hash->path
        for nnn in range(file_count):
            d_key = None
            d_path = None
            # create a random file name                  maxLen   minLen
            (d_len, d_path) = RNG.next_data_file(DATA_PATH, 16 * 1024, 1)
            # perhaps more restrictions needed
            while d_path.endswith('.'):
                (d_len, d_path) = RNG.next_data_file(DATA_PATH, 16 * 1024, 1)
            if using_sha == QQQ.USING_SHA1:
                d_key = u.file_sha1hex(d_path)
            elif using_sha == QQQ.USING_SHA2:
                d_key = u.file_sha2hex(d_path)
            elif using_sha == QQQ.USING_SHA3:
                d_key = u.file_sha3hex(d_path)
            files[d_key] = d_path

        self.assertEqual(file_count, len(files))
        return files

    # ---------------------------------------------------------------

    def do_test_put_to_empty(self, using_sha):
        # SETUP
        u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))
        while os.path.exists(u_path):
            u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))

        string = upax.BlockingServer(u_path, using_sha)
        try:
            file_map = self.make_some_files(using_sha)
            file_count = len(file_map)

            for key in list(file_map.keys()):
                string.put(file_map[key], key, 'test_put_to_empty')
                self.assertTrue(string.exists(key))
                with open(file_map[key], 'rb') as file:
                    data_in_file = file.read()
                data_in_u = string.get(key)
                self.assertEqual(data_in_file, data_in_u)

            log = string.log
            self.assertEqual(file_count, len(log))

            for key in list(file_map.keys()):
                string.exists(key)
                entry = log.get_entry(key)
                self.assertIsNotNone(entry)
                # STUB: shold examine properties of log entry
        finally:
            string.close()
        self.assertTrue(os.path.exists(os.path.join(u_path, 'L')))   # GEEP

    def test_put_to_empty(self):
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_put_to_empty(using)

    # ---------------------------------------------------------------

    def do_test_put_close_reopen_and_put(self, using_sha):
        # SETUP
        u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))
        while os.path.exists(u_path):
            u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))

        string = upax.BlockingServer(u_path, using_sha)
        try:
            file_map1 = self.make_some_files(using_sha)
            file_count1 = len(file_map1)
            for key in list(file_map1.keys()):
                string.put(file_map1[key], key, 'testPut ... first phase')
        finally:
            string.close()

        string = upax.BlockingServer(u_path, using_sha)
        try:
            file_map2 = self.make_some_files(using_sha)
            file_count2 = len(file_map2)
            for key in list(file_map2.keys()):
                string.put(file_map2[key], key, 'testPut ... SECOND PHASE')

            log = string.log
            self.assertEqual(file_count1 + file_count2, len(log))

            for key in list(file_map1.keys()):
                string.exists(key)
                entry = log.get_entry(key)
                self.assertIsNotNone(entry)

            for key in list(file_map2.keys()):
                string.exists(key)
                entry = log.get_entry(key)
                self.assertIsNotNone(entry)
        finally:
            string.close()
        self.assertTrue(os.path.exists(os.path.join(u_path, 'L')))

    def test_put_close_reopen_and_put(self):
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_put_close_reopen_and_put(using)          # GEEP

if __name__ == '__main__':
    unittest.main()
