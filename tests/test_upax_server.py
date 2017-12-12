#!/usr/bin/env python3
# testUpaxServer.py

""" Test functions of a Upax server. """

import os
import time
import unittest

import rnglib
from upax.server import BlockingServer
from xlattice import u, HashTypes, check_hashtype

RNG = rnglib.SimpleRNG(time.time())

DATA_PATH = 'myData'


class TestUpaxServer(unittest.TestCase):
    """ Test functions of a Upax server. """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _construct_from_nothing(self, hashtype):
        """ Test the constructor for a specific  SHA type. """

        check_hashtype(hashtype)
        # SETUP
        u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))
        while os.path.exists(u_path):
            u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))

        # we are guaranteed that uPath does _not_ exist
        string = BlockingServer(u_path, hashtype)
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
            if hashtype == HashTypes.SHA1:
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
        """ Test the constructor for the various SHA types. """
        for hashtype in [HashTypes.SHA1, HashTypes.SHA2, HashTypes.SHA3]:
            self._construct_from_nothing(hashtype)

#   # ---------------------------------------------------------------

    def make_some_files(self, hashtype):
        """
        Create a random number of unique data files of random length
          in myData/.

        Take hash of each as it is created, using this to verify uniqueness;
        add hash to list (or use hash as key to map).

        Return a map: hash -> path.
        """

        check_hashtype(hashtype)

        file_count = 3 + RNG.next_int16(16)
        files = {}             # a map hash->path
        for _ in range(file_count):
            d_key = None
            d_path = None
            # create a random file name                  maxLen   minLen
            (_, d_path) = RNG.next_data_file(DATA_PATH, 16 * 1024, 1)
            # perhaps more restrictions needed
            while d_path.endswith('.'):
                (_, d_path) = RNG.next_data_file(DATA_PATH, 16 * 1024, 1)
            if hashtype == HashTypes.SHA1:
                d_key = u.file_sha1hex(d_path)
            elif hashtype == HashTypes.SHA2:
                d_key = u.file_sha2hex(d_path)
            elif hashtype == HashTypes.SHA3:
                d_key = u.file_sha3hex(d_path)
            files[d_key] = d_path

        self.assertEqual(file_count, len(files))
        return files

    # ---------------------------------------------------------------

    def _put_to_empty(self, hashtype):
        """
        Test the put() function on what is initially an empty server
        using a specific hash type.
        """
        # SETUP
        u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))
        while os.path.exists(u_path):
            u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))

        string = BlockingServer(u_path, hashtype)
        try:
            file_map = self.make_some_files(hashtype)
            file_count = len(file_map)

            for key in file_map:
                string.put(file_map[key], key, 'test_put_to_empty')
                self.assertTrue(string.exists(key))
                with open(file_map[key], 'rb') as file:
                    data_in_file = file.read()
                data_in_u = string.get(key)
                self.assertEqual(data_in_file, data_in_u)

            log = string.log
            self.assertEqual(file_count, len(log))

            for key in file_map:
                string.exists(key)
                entry = log.get_entry(key)
                self.assertIsNotNone(entry)
                # STUB: shold examine properties of log entry
        finally:
            string.close()
        self.assertTrue(os.path.exists(os.path.join(u_path, 'L')))   # GEEP

    def test_put_to_empty(self):
        """
        Test the put() function on what is initially an empty server
        using the various supported hash types.
        """
        for hashtype in HashTypes:
            self._put_to_empty(hashtype)

    # ---------------------------------------------------------------

    def _put_close_reopen_and_put(self, hashtype):
        """
        Test put/close/reopen then put again to a previously empty
        upax server using a specific SHA types.
        """
        # SETUP
        u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))
        while os.path.exists(u_path):
            u_path = os.path.join(DATA_PATH, RNG.next_file_name(16))

        string = BlockingServer(u_path, hashtype)
        try:
            file_map1 = self.make_some_files(hashtype)
            file_count1 = len(file_map1)
            for key in file_map1:
                string.put(file_map1[key], key, 'testPut ... first phase')
        finally:
            string.close()

        string = BlockingServer(u_path, hashtype)
        try:
            file_map2 = self.make_some_files(hashtype)
            file_count2 = len(file_map2)
            for key in file_map2:
                string.put(file_map2[key], key, 'testPut ... SECOND PHASE')

            log = string.log
            self.assertEqual(file_count1 + file_count2, len(log))

            for key in file_map1:
                string.exists(key)
                entry = log.get_entry(key)
                self.assertIsNotNone(entry)

            for key in file_map2:
                string.exists(key)
                entry = log.get_entry(key)
                self.assertIsNotNone(entry)
        finally:
            string.close()
        self.assertTrue(os.path.exists(os.path.join(u_path, 'L')))

    def test_put_close_reopen_and_put(self):
        """
        Test put/close/reopen then put again to a previously empty
        upax server using the supported SHA types.
        """
        for hashtype in HashTypes:
            self._put_close_reopen_and_put(hashtype)          # GEEP


if __name__ == '__main__':
    unittest.main()
