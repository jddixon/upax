#!/usr/bin/env python3

# testLogReader.py
import time
import unittest

from upax.ftlog import LogEntry, Reader, StringReader, FileReader
from xlattice import QQQ, check_using_sha


class TestLogReader(unittest.TestCase):

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
            # 32-byte keys; values are arbitrary
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

    def do_test_string_reader(self, using_sha):
        check_using_sha(using_sha)

        (goodkey_1, goodkey_2, goodkey_3, goodkey_4,
         goodkey_5, goodkey_6, goodkey_7, goodkey_8,) = self.get_good(using_sha)

        time0 = int(time.time()) - 10000
        time1 = time0 + 100
        time2 = time1 + 100
        time3 = time2 + 100

        comment_line = "# This is a comment and can be ignored\n"
        empty_log = "%013u %s %s\n" % (time0, goodkey_1, goodkey_2)
        empty_log += comment_line
        reader = StringReader(empty_log, using_sha)
        assert reader is not None
        self.assertTrue(isinstance(reader, Reader))     # inheritance works
        self.assertTrue(isinstance(reader, StringReader))
        rdr = reader.read()  # NOTANDUM BENE
        # self.assertTrue(log is not None)  # also gets 'AssertionError'
        assert rdr is not None
        (timestamp, prev_log_hash, prev_master, entries, index) = rdr
        self.assertEqual(time0, timestamp)
        self.assertEqual(goodkey_1, prev_log_hash)
        self.assertEqual(goodkey_2, prev_master)
        self.assertEqual(0, len(entries))

        entry1 = LogEntry(time1, goodkey_3, goodkey_4, 'jdd', 'document1')
        entry2 = LogEntry(time2, goodkey_5, goodkey_6, 'jdd', 'document1')
        entry3 = LogEntry(time3, goodkey_7, goodkey_8, 'jdd', 'document1')

        test_log = empty_log + str(entry1) + str(entry2) + str(entry3)
        reader = StringReader(test_log, using_sha)
        assert reader is not None
        self.assertTrue(isinstance(reader, Reader))
        self.assertTrue(isinstance(reader, StringReader))
        rdr = reader.read()
        # self.assertTrue(log is not None)  # also gets 'AssertionError'
        assert rdr is not None
        (timestamp, prev_log_hash, prev_master, entries, index) = rdr
        self.assertEqual(3, len(entries))
        self.assertEqual(3, len(index))

    def test_string_reader(self):
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_string_reader(using)

    # ---------------------------------------------------------------

#   def testFileReader(self):
#       """
#       XXX Don't know why the log file is named Q, nor is it clear
#       why the file should have anything in it.
#       """
#        reader = FileReader('dev3/U', 'Q')
#        assert reader is not None
#        self.assertEqual('dev3/U',   reader.uPath)
#        self.assertEqual('dev3/U/Q', reader.logFile)
#        assert reader.entries is not None
#        self.assertEqual(0, len(reader.entries))
#        assert reader.index is not None
#        self.assertEqual(0, len(reader.index)) # GEEP

#       reader = FileReader('dev0/U')
#       assert reader is not None
#       self.assertEqual('dev0/U', reader.uPath)
#       self.assertEqual('dev0/U/L', reader.logFile)   # default name

    # ---------------------------------------------------------------

    def test_reading_first_line(self):
        pass

if __name__ == '__main__':
    unittest.main()
