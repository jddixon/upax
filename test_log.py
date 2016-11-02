#!/usr/bin/env python3

# testLog.py

import time
import unittest
from xlattice import QQQ, check_using_sha

from upax.ftlog import Log, LogEntry, Reader, StringReader


class TestLog (unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def get_good(self, using_sha):
        check_using_sha(using_sha)
        if using_sha == QQQ.USING_SHA1:
            goodkey_1 = '0123456789012345678901234567890123456789'
            goodkey_2 = 'fedcba9876543210fedcba9876543210fedcba98'
            goodkey_3 = '1234567890123456789012345678901234567890'
            goodkey_4 = 'edcba9876543210fedcba9876543210fedcba98f'
            goodkey_5 = '2345678901234567890123456789012345678901'
            goodkey_6 = 'dcba9876543210fedcba9876543210fedcba98fe'
            goodkey_7 = '3456789012345678901234567890123456789012'
            goodkey_8 = 'cba9876543210fedcba9876543210fedcba98fed'
        else:
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

    def do_test_to_string_without_entries(self, using_sha):
        check_using_sha(using_sha)

        (goodkey_1, goodkey_2, goodkey_3, goodkey_4,
         goodkey_5, goodkey_6, goodkey_7, goodkey_8,) = self.get_good(using_sha)

        time0 = int(time.time()) - 10000
        empty_log = "%013u %s %s\n" % (time0, goodkey_1, goodkey_2)
        reader = StringReader(empty_log, using_sha)
        log = Log(reader, using_sha)
        assert log is not None
        self.assertEqual(time0, log.timestamp)
        self.assertEqual(goodkey_1, log.prev_hash)
        self.assertEqual(goodkey_2, log.prev_master)

        # only first line should appear, because there are no entries
        expected = empty_log
        actual = log.__str__()
        self.assertEqual(expected, actual)
        self.assertEqual(0, len(log))

    def test_str_without_entries(self):
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_to_string_without_entries(using)

    # ---------------------------------------------------------------

    def do_test_multi_entry_log(self, using_sha):

        check_using_sha(using_sha)

        (goodkey_1, goodkey_2, goodkey_3, goodkey_4,
         goodkey_5, goodkey_6, goodkey_7, goodkey_8,) = self.get_good(using_sha)

        time0 = int(time.time()) - 10000
        time1 = time0 + 100
        time2 = time1 + 100
        time3 = time2 + 100

        empty_log = "%013u %s %s\n" % (time0, goodkey_1, goodkey_2)
        entry1 = LogEntry(time1, goodkey_3, goodkey_4, 'jdd', 'document1')
        entry2 = LogEntry(time2, goodkey_5, goodkey_6, 'jdd', 'document2')
        entry3 = LogEntry(time3, goodkey_7, goodkey_8, 'jdd', 'document3')
        test_log = empty_log + str(entry1) + str(entry2) + str(entry3)

        reader = StringReader(test_log, using_sha)
        log = Log(reader, using_sha)
        assert log is not None

        # NOT IN testLog3 ---------------------------------
        self.assertEqual(3, len(log.entries))
        for entryA in log.entries:
            self.assertEqual(entryA, entryA)
            for entryB in log.entries:
                if entryB.key != entryA.key:
                    self.assertTrue(entryA != entryB)

        # END NOT IN --------------------------------------

        self.assertEqual(time0, log.timestamp)
        self.assertEqual(goodkey_1, log.prev_hash)
        self.assertEqual(goodkey_2, log.prev_master)
        self.assertEqual(3, len(log))

        self.assertTrue(goodkey_3 in log)
        entry = log.get_entry(goodkey_3)
        self.assertTrue(entry1.equals(entry))

        self.assertTrue(goodkey_5 in log)
        entry = log.get_entry(goodkey_5)
        self.assertTrue(entry2.equals(entry))

        self.assertTrue(goodkey_7 in log)
        entry = log.get_entry(goodkey_7)
        self.assertTrue(entry3.equals(entry))

        # NOT IN testLog3 ---------------------------------
        entry_count0 = len(log.entries)
        index_count0 = len(log.index)

        # add a duplicate entry: this should have no visible effect
        # log.addEntry(entry3)
        dupe3 = log.add_entry(time3, goodkey_7, goodkey_8, 'jdd', 'document3')
        self.assertEqual(entry_count0, len(log.entries))
        self.assertEqual(index_count0, len(log.index))

        # Add an entry with the same key but otherwise different: this
        # should increase the entry count by 1 but leave the size of
        # the index unchanged.  The key here is GOODKEY_7
        entry4 = log.add_entry(
            time3 + 100,
            goodkey_7,
            goodkey_8,
            'jdd',
            'document3')
        self.assertEqual(entry_count0 + 1, len(log.entries))
        self.assertEqual(index_count0, len(log.index))
        self.assertTrue(entry4 != entry3)
        # END NOT IN --------------------------------------

    def test_multi_entry_log(self):
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_multi_entry_log(using)

    # ---------------------------------------------------------------

    def do_test_add_entry(self, using_sha):
        check_using_sha(using_sha)

        (goodkey_1, goodkey_2, goodkey_3, goodkey_4,
         goodkey_5, goodkey_6, goodkey_7, goodkey_8,) = self.get_good(using_sha)

        time0 = int(time.time()) - 10000
        time1 = time0 + 100
        time2 = time1 + 100
        time3 = time2 + 100

        empty_log = "%013u %s %s\n" % (time0, goodkey_1, goodkey_2)
        entry1 = LogEntry(time1, goodkey_3, goodkey_4, 'jdd', 'document1')
        entry2 = LogEntry(time2, goodkey_5, goodkey_6, 'jdd', 'document2')
        entry3 = LogEntry(time3, goodkey_7, goodkey_8, 'jdd', 'document3')
        test_log = empty_log + str(entry1) + str(entry2) + str(entry3)
        reader = StringReader(empty_log, using_sha)
        log = Log(reader, using_sha)
        assert log is not None
        self.assertEqual(time0, log.timestamp)
        self.assertEqual(goodkey_1, log.prev_hash)
        self.assertEqual(goodkey_2, log.prev_master)
        self.assertEqual(0, len(log))

        log.add_entry(time1, goodkey_3, goodkey_4, 'jdd', 'document1')
        self.assertEqual(1, len(log))
        entry = log.get_entry(goodkey_3)
        self.assertTrue(entry1.equals(entry))
        self.assertTrue(goodkey_3 in log)
        self.assertFalse(goodkey_5 in log)

        log.add_entry(time2, goodkey_5, goodkey_6, 'jdd', 'document2')
        self.assertEqual(2, len(log))
        entry = log.get_entry(goodkey_5)
        self.assertTrue(entry2.equals(entry))
        self.assertTrue(goodkey_5 in log)

        log.add_entry(time3, goodkey_7, goodkey_8, 'jdd', 'document3')
        self.assertEqual(3, len(log))
        entry = log.get_entry(goodkey_7)
        self.assertTrue(entry3.equals(entry))
        self.assertTrue(goodkey_7 in log)

    def test_add_entry(self):
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_add_entry(using)

if __name__ == '__main__':
    unittest.main()
