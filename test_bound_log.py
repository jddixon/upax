#!/usr/bin/env python3

# testBoundLog.py

import os
import time
import unittest
from xlattice import HashTypes, check_hashtype

from upax.ftlog import BoundLog, FileReader, LogEntry, StringReader


class TestBoundLog(unittest.TestCase):

    def setUp(self):
        self.u_dir = "dev0/U"
        self.path_to_log = "%s/L" % self.u_dir
        if os.path.exists(self.path_to_log):
            os.remove(self.path_to_log)

    def tearDown(self):
        pass

    def get_good(self, hashtype):
        if hashtype == HashTypes.SHA1:
            goodkey_1 = '0123456789012345678901234567890123456789'
            goodkey_2 = 'fedcba9876543210fedcba9876543210fedcba98'
            goodkey_3 = '1234567890123456789012345678901234567890'
            goodkey_4 = 'edcba9876543210fedcba9876543210fedcba98f'
            goodkey_5 = '2345678901234567890123456789012345678901'
            goodkey_6 = 'dcba9876543210fedcba9876543210fedcba98fe'
            goodkey_7 = '3456789012345678901234567890123456789012'
            goodkey_8 = 'cba9876543210fedcba9876543210fedcba98fed'
        else:
            # meaningless values, OK for sha2 or sha3
            goodkey_1 = '0123456789012345678901234567890123456789abcdef3330123456789abcde'
            goodkey_2 = 'fedcba9876543210fedcba9876543210fedcba98012345678901234567890123'
            goodkey_3 = '1234567890123456789012345678901234567890abcdef697698768696969696'
            goodkey_4 = 'edcba9876543210fedcba9876543210fedcba98f012345678901234567890123'
            goodkey_5 = '2345678901234567890123456789012345678901654654647645647654754757'
            goodkey_6 = 'dcba9876543210fedcba9876543210fedcba98fe453254323243253274754777'
            goodkey_7 = '3456789012345678901234567890123456789012abcdef696878687686999987'
            goodkey_8 = 'cba9876543210fedcba9876543210fedcba98fedfedcab698687669676999988'
        return (goodkey_1, goodkey_2, goodkey_3, goodkey_4,
                goodkey_5, goodkey_6, goodkey_7, goodkey_8,)

    def do_test_log_without_entries(self, hashtype):

        check_hashtype(hashtype)
        (goodkey_1, goodkey_2, goodkey_3, goodkey_4,
         goodkey_5, goodkey_6, goodkey_7, goodkey_8,) = self.get_good(hashtype)

        time0 = 1000 * (int(time.time()) - 10000)
        # the first line of an otherwise empty log file
        empty_log = "%013u %s %s\n" % (time0, goodkey_1, goodkey_2)
        reader = StringReader(empty_log, hashtype)
        log = BoundLog(
            reader,
            hashtype,
            self.u_dir,
            'L')  # will default to 'L'

        assert log is not None
        self.assertEqual(time0, log.timestamp)
        self.assertEqual(goodkey_1, log.prev_hash)
        self.assertEqual(goodkey_2, log.prev_master)

        # only first line should appear, because there are no entries
        expected = empty_log
        actual = log.__str__()
        self.assertEqual(expected, actual)
        self.assertEqual(0, len(log))

        # disk file must exist and must contain just the one line
        path_to_log = "%s/L" % "dev0/U/"
        assert os.path.exists(path_to_log)
        contents = ''
        with open(path_to_log, "r") as file:
            contents = file.read()
        self.assertEqual(empty_log, contents)
        log.close()

    def test_log_without_entries(self):
        for using in [HashTypes.SHA1, HashTypes.SHA2, HashTypes.SHA3, ]:
            self.do_test_log_without_entries(using)

    def setup_the_server(self, hashtype):

        (goodkey_1, goodkey_2, goodkey_3, goodkey_4,
         goodkey_5, goodkey_6, goodkey_7, goodkey_8,) = self.get_good(hashtype)

        time0 = int(time.time()) - 10000
        time1 = time0 + 100
        time2 = time1 + 100
        time3 = time2 + 100
        entry1 = LogEntry(time1, goodkey_3, goodkey_4, 'jdd', 'e@document1')
        entry2 = LogEntry(time2, goodkey_5, goodkey_6, 'jdd', 'e@document2')
        entry3 = LogEntry(time3, goodkey_7, goodkey_8, 'jdd', 'e@document3')
        empty_log = "%013u %s %s\n" % (time0, goodkey_1, goodkey_2)
        log_w_three = empty_log + str(entry1) + str(entry2) + str(entry3)
        return (time0, time1, time2, time3, entry1,
                entry2, entry3, empty_log, log_w_three)

    def do_test_multi_entry_log(self, hashtype):
        check_hashtype(hashtype)

        (goodkey_1, goodkey_2, goodkey_3, goodkey_4,
         goodkey_5, goodkey_6, goodkey_7, goodkey_8,) = self.get_good(hashtype)

        (time0, time1, time2, time3, entry1, entry2, entry3, empty_log, log_w_three) =\
            self.setup_the_server(hashtype)
        reader = StringReader(log_w_three, hashtype)
        log = BoundLog(reader, hashtype, self.u_dir, 'L')
        assert log is not None
        self.assertEqual(time0, log.timestamp)
        self.assertEqual(goodkey_1, log.prev_hash)
        self.assertEqual(goodkey_2, log.prev_master)
        self.assertEqual(3, len(log))

        self.assertTrue(goodkey_3 in log)
        entry = log.get_entry(goodkey_3)
        self.assertEqual(entry1, entry)

        self.assertTrue(goodkey_5 in log)
        entry = log.get_entry(goodkey_5)
        self.assertEqual(entry2, entry)

        self.assertTrue(goodkey_7 in log)
        entry = log.get_entry(goodkey_7)
        self.assertEqual(entry3, entry)

        with open(self.path_to_log, 'r') as file:
            log_contents = file.read()
        self.assertEqual(log_w_three, log_contents)
        log.close()

    def test_multi_entry_log(self):
        for using in [HashTypes.SHA1, HashTypes.SHA2, HashTypes.SHA3, ]:
            self.do_test_multi_entry_log(using)

    def do_test_add_entry(self, hashtype):

        check_hashtype(hashtype)
        (goodkey_1, goodkey_2, goodkey_3, goodkey_4,
         goodkey_5, goodkey_6, goodkey_7, goodkey_8,) = self.get_good(hashtype)

        (time0, time1, time2, time3, entry1, entry2, entry3, empty_log, log_w_three) =\
            self.setup_the_server(hashtype)
        reader = StringReader(empty_log, hashtype)
        log = BoundLog(reader, hashtype, self.u_dir, 'L')
        assert log is not None
        self.assertEqual(time0, log.timestamp)
        self.assertEqual(goodkey_1, log.prev_hash)
        self.assertEqual(goodkey_2, log.prev_master)
        self.assertEqual(0, len(log))

        #                   key     srcNodeID
        log.add_entry(time1, goodkey_3, goodkey_4, 'jdd', 'e@document1')
        self.assertEqual(1, len(log))
        entry = log.get_entry(goodkey_3)
        self.assertEqual(entry1, entry)
        self.assertTrue(goodkey_3 in log)
        self.assertFalse(goodkey_5 in log)

        log.add_entry(time2, goodkey_5, goodkey_6, 'jdd', 'e@document2')
        self.assertEqual(2, len(log))
        entry = log.get_entry(goodkey_5)
        self.assertEqual(entry2, entry)
        self.assertTrue(goodkey_5 in log)

        log.add_entry(time3, goodkey_7, goodkey_8, 'jdd', 'e@document3')
        self.assertEqual(3, len(log))
        entry = log.get_entry(goodkey_7)
        self.assertEqual(entry3, entry)
        self.assertTrue(goodkey_7 in log)

        log.close()                     # without this next test fails
        with open(self.path_to_log, 'r') as file:
            log_contents = file.read()
        self.assertEqual(log_w_three, log_contents)

    def test_add_entry(self):
        for using in [HashTypes.SHA1, HashTypes.SHA2, HashTypes.SHA3, ]:
            self.do_test_add_entry(using)

    def do_test_with_opens_and_closes(self, hashtype):

        check_hashtype(hashtype)

        (goodkey_1, goodkey_2, goodkey_3, goodkey_4,
         goodkey_5, goodkey_6, goodkey_7, goodkey_8,) = self.get_good(hashtype)

        (time0, time1, time2, time3, entry1, entry2, entry3, empty_log, log_w_three) =\
            self.setup_the_server(hashtype)
        reader = StringReader(empty_log, hashtype)
        log = BoundLog(reader, hashtype, self.u_dir)
        assert log is not None
        self.assertEqual(time0, log.timestamp)
        self.assertEqual(goodkey_1, log.prev_hash)
        self.assertEqual(goodkey_2, log.prev_master)
        self.assertEqual(0, len(log))
        log.close()

        reader = FileReader(self.u_dir, hashtype)
        log = BoundLog(reader, hashtype)
        log.add_entry(time1, goodkey_3, goodkey_4, 'jdd', 'e@document1')
        self.assertEqual(1, len(log))
        entry = log.get_entry(goodkey_3)
        self.assertEqual(entry1, entry)
        self.assertTrue(goodkey_3 in log)
        self.assertFalse(goodkey_5 in log)
        log.close()

        reader = FileReader(self.u_dir, hashtype)
        log = BoundLog(reader, hashtype)
        log.add_entry(time2, goodkey_5, goodkey_6, 'jdd', 'e@document2')
        self.assertEqual(2, len(log))
        entry = log.get_entry(goodkey_5)
        self.assertEqual(entry2, entry)
        self.assertTrue(goodkey_5 in log)
        log.close()

        reader = FileReader(self.u_dir, hashtype)
        log = BoundLog(reader, hashtype)
        log.add_entry(time3, goodkey_7, goodkey_8, 'jdd', 'e@document3')
        self.assertEqual(3, len(log))
        entry = log.get_entry(goodkey_7)
        self.assertEqual(entry3, entry)
        self.assertTrue(goodkey_7 in log)
        log.close()

        with open(self.path_to_log, 'r') as file:
            log_contents = file.read()
        self.assertEqual(log_w_three, log_contents)

    def test_with_opens_and_closes(self):
        for hashtype in HashTypes:
            self.do_test_with_opens_and_closes(hashtype)

if __name__ == '__main__':
    unittest.main()
