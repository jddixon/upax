#!/usr/bin/env python3

# testImporter.py

import os
import time
import unittest
import rnglib
from xlattice import QQQ, check_using_sha
from xlattice.u import file_sha1hex, file_sha2hex, file_sha3hex
from upax import __version__, BlockingServer, Importer


RNG = rnglib.SimpleRNG(time.time())

DATA_PATH = 'myData'


class TestImporter(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_some_files(self, using_sha):
        """ return a map: hash=>path """

        # create a random number of unique data files of random length
        #   in myData/; take hash of each as it is created, using
        #   this to verify uniqueness; add hash to list (or use hash
        #   as key to map

        check_using_sha(using_sha)
        file_count = 17 + RNG.next_int16(128)
        files = {}             # a map hash->path
        for nnn in range(file_count):
            d_key = None
            # create a random file name                  maxLen   minLen
            (d_len, d_path) = RNG.next_data_file(DATA_PATH, 16 * 1024, 1)
            # perhaps more restrictions needed
            while '.' in d_path:
                (d_len, d_path) = RNG.next_data_file(DATA_PATH, 16 * 1024, 1)
            if using_sha == QQQ.USING_SHA1:
                d_key = file_sha1hex(d_path)
            elif using_sha == QQQ.USING_SHA2:
                d_key = file_sha2hex(d_path)
            elif using_sha == QQQ.USING_SHA3:
                d_key = file_sha3hex(d_path)
            files[d_key] = d_path

        self.assertEqual(file_count, len(files))
        return files

    def construct_empty_u_dir(self, u_path, using_sha):

        # we are guaranteed that uPath does NOT exist
        self.assertFalse(os.path.exists(u_path))

        string = BlockingServer(u_path, using_sha)
        self.assertIsNotNone(string)
        self.assertTrue(os.path.exists(string.u_path))
        self.assertEqual(string.using_sha, using_sha)

        # subdirectories
        self.assertTrue(os.path.exists(os.path.join(u_path, 'in')))
        self.assertTrue(os.path.exists(os.path.join(u_path, 'tmp')))

        # nodeID
        id_path = os.path.join(u_path, 'node_id')
        self.assertTrue(os.path.exists(id_path))
        with open(id_path, 'r') as file:
            node_id = file.read()
        if using_sha == QQQ.USING_SHA1:
            self.assertEqual(41, len(node_id))
            self.assertEqual('\n', node_id[40])
        else:
            self.assertEqual(65, len(node_id))
            self.assertEqual('\n', node_id[64])
        node_id = node_id[:-1]                    # drop terminating newline

        self.assertTrue(os.path.exists(os.path.join(u_path, 'L')))   # GEEP
        return string

    def populate_empty(self, string, file_map, using_sha):
        u_path = string.u_path
        file_count = len(file_map)

        for key in list(file_map.keys()):
            string.put(file_map[key], key, 'test_put_to_empty')
            self.assertTrue(string.exists(key))                  # FAILS
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
        self.assertTrue(os.path.exists(os.path.join(u_path, 'L')))   # GEEP

    def do_test_import(self, using_sha):
        check_using_sha(using_sha)

        src_path = os.path.join(DATA_PATH, RNG.next_file_name(16))
        while os.path.exists(src_path):
            src_path = os.path.join(DATA_PATH, RNG.next_file_name(16))

        dest_path = os.path.join(DATA_PATH, RNG.next_file_name(16))
        while os.path.exists(dest_path):
            dest_path = os.path.join(DATA_PATH, RNG.next_file_name(16))

        # create a collection of data files
        file_map = self.make_some_files(using_sha)
        file_count = len(file_map)

        # create an empty source directory, populate it, shut down the server
        try:
            u_dir0 = self.construct_empty_u_dir(src_path, using_sha)
            self.populate_empty(u_dir0, file_map, using_sha)
        finally:
            u_dir0.close()

        # create an empty destination dir
        u_dir1 = self.construct_empty_u_dir(dest_path, using_sha)
        u_dir1.close()

        # create and invoke the importer
        importer = Importer(src_path, dest_path,
                            'testImport ' + __version__, using_sha)
        importer.do_import_u_dir()

        # verify that the files got there
        server2 = BlockingServer(dest_path, using_sha)
        self.assertIsNotNone(server2)
        self.assertTrue(os.path.exists(server2.u_path))
        self.assertEqual(server2.using_sha, using_sha)
        log = server2.log

        for key in list(file_map.keys()):
            server2.exists(key)
            entry = log.get_entry(key)
            self.assertIsNotNone(entry)

        server2.close()
        self.assertTrue(os.path.exists(os.path.join(dest_path, 'L')))

    def test_import(self):
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_import(using)

if __name__ == '__main__':
    unittest.main()
