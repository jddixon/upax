#!/usr/bin/python3

# testLogReader3.py
import time
import unittest

from upax.ftlog import LogEntry, Reader, StringReader, FileReader

#            ....x....1....x....2....x....3....x....4....x....5....x....6....
GOODKEY_1 = '0123456789012345678901234567890123456789abcdef3330123456789abcde'
GOODKEY_2 = 'fedcba9876543210fedcba9876543210fedcba98012345678901234567890123'
GOODKEY_3 = '1234567890123456789012345678901234567890abcdef697698768696969696'
GOODKEY_4 = 'edcba9876543210fedcba9876543210fedcba98f012345678901234567890123'
GOODKEY_5 = '2345678901234567890123456789012345678901654654647645647654754757'
GOODKEY_6 = 'dcba9876543210fedcba9876543210fedcba98fe453254323243253274754777'
GOODKEY_7 = '3456789012345678901234567890123456789012abcdef696878687686999987'
GOODKEY_8 = 'cba9876543210fedcba9876543210fedcba98fedfedcab698687669676999988'


class TestLogReader3 (unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testStringReader(self):
        t0 = int(time.time()) - 10000
        t1 = t0 + 100
        t2 = t1 + 100
        t3 = t2 + 100

        COMMENT_LINE = "# This is a comment and can be ignored\n"
        EMPTY_LOG = "%013u %s %s\n" % (t0, GOODKEY_1, GOODKEY_2)
        EMPTY_LOG += COMMENT_LINE
        reader = StringReader(EMPTY_LOG)
        assert reader is not None
        self.assertTrue(isinstance(reader, Reader))     # inheritance works
        self.assertTrue(isinstance(reader, StringReader))
        r = reader.read()
        # self.assertTrue(log is not None)  # also gets 'AssertionError'
        assert r is not None
        (timestamp, prevLogHash, prevMaster, entries, index) = r
        self.assertEqual(t0, timestamp)
        self.assertEqual(GOODKEY_1, prevLogHash)
        self.assertEqual(GOODKEY_2, prevMaster)
        self.assertEqual(0, len(entries))

        entry1 = LogEntry(t1, GOODKEY_3, GOODKEY_4, 'jdd', 'document1')
        entry2 = LogEntry(t2, GOODKEY_5, GOODKEY_6, 'jdd', 'document1')
        entry3 = LogEntry(t3, GOODKEY_7, GOODKEY_8, 'jdd', 'document1')

        TEST_LOG = EMPTY_LOG + str(entry1) + str(entry2) + str(entry3)
        reader = StringReader(TEST_LOG)
        assert reader is not None
        self.assertTrue(isinstance(reader, Reader))
        self.assertTrue(isinstance(reader, StringReader))
        r = reader.read()
        # self.assertTrue(log is not None)  # also gets 'AssertionError'
        assert r is not None
        (timestamp, prevLogHash, prevMaster, entries, index) = r
        self.assertEqual(3, len(entries))
        self.assertEqual(3, len(index))

    def testFileReader(self):
        """
        XXX Don't know why the log file is named Q, nor is it clear
        why the file should have anything in it.
        """
#        reader = FileReader('dev3/U', 'Q')
#        assert reader is not None
#        self.assertEqual('dev3/U',   reader.uDir)
#        self.assertEqual('dev3/U/Q', reader.logFile)
#        assert reader.entries is not None
#        self.assertEqual(0, len(reader.entries))
#        assert reader.index is not None
#        self.assertEqual(0, len(reader.index)) # GEEP

        reader = FileReader('dev0/U')
        assert reader is not None
        self.assertEqual('dev0/U', reader.uDir)
        self.assertEqual('dev0/U/L', reader.logFile)   # default name

    def testReadingFirstLine(self):
        pass

if __name__ == '__main__':
    unittest.main()
