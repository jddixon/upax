#!/usr/bin/env python3

# testLog3.py

import time
import unittest

from upax.ftlog import Log, LogEntry, Reader, StringReader

#            ....x....1....x....2....x....3....x....4....x....5....x....6....
GOODKEY_1 = '0123456789012345678901234567890123456789abcdef3330123456789abcde'
GOODKEY_2 = 'fedcba9876543210fedcba9876543210fedcba98012345678901234567890123'
GOODKEY_3 = '1234567890123456789012345678901234567890abcdef697698768696969696'
GOODKEY_4 = 'edcba9876543210fedcba9876543210fedcba98f012345678901234567890123'
GOODKEY_5 = '2345678901234567890123456789012345678901654654647645647654754757'
GOODKEY_6 = 'dcba9876543210fedcba9876543210fedcba98fe453254323243253274754777'
GOODKEY_7 = '3456789012345678901234567890123456789012abcdef696878687686999987'
GOODKEY_8 = 'cba9876543210fedcba9876543210fedcba98fedfedcab698687669676999988'


class TestLog3 (unittest.TestCase):
    """
    This does not test everything that TestLog1 does.
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__str__WithoutEntries(self):
        t0 = int(time.time()) - 10000
        EMPTY_LOG = "%013u %s %s\n" % (t0, GOODKEY_1, GOODKEY_2)
        reader = StringReader(EMPTY_LOG)
        log = Log(reader)
        assert log is not None
        self.assertEqual(t0, log.timestamp)
        self.assertEqual(GOODKEY_1, log.prevHash)
        self.assertEqual(GOODKEY_2, log.prevMaster)

        # only first line should appear, because there are no entries
        expected = EMPTY_LOG
        actual = log.__str__()
        self.assertEqual(expected, actual)
        self.assertEqual(0, len(log))

    def testMultiEntryLog(self):
        t0 = int(time.time()) - 10000
        t1 = t0 + 100
        t2 = t1 + 100
        t3 = t2 + 100

        EMPTY_LOG = "%013u %s %s\n" % (t0, GOODKEY_1, GOODKEY_2)
        entry1 = LogEntry(t1, GOODKEY_3, GOODKEY_4, 'jdd', 'document1')
        entry2 = LogEntry(t2, GOODKEY_5, GOODKEY_6, 'jdd', 'document2')
        entry3 = LogEntry(t3, GOODKEY_7, GOODKEY_8, 'jdd', 'document3')
        TEST_LOG = EMPTY_LOG + str(entry1) + str(entry2) + str(entry3)

        reader = StringReader(TEST_LOG)
        log = Log(reader)
        assert log is not None
        self.assertEqual(t0, log.timestamp)
        self.assertEqual(GOODKEY_1, log.prevHash)
        self.assertEqual(GOODKEY_2, log.prevMaster)
        self.assertEqual(3, len(log))

        self.assertTrue(GOODKEY_3 in log)
        entry = log.getEntry(GOODKEY_3)
        self.assertTrue(entry1.equals(entry))

        self.assertTrue(GOODKEY_5 in log)
        entry = log.getEntry(GOODKEY_5)
        self.assertTrue(entry2.equals(entry))

        self.assertTrue(GOODKEY_7 in log)
        entry = log.getEntry(GOODKEY_7)
        self.assertTrue(entry3.equals(entry))

    def testAddEntry(self):
        t0 = int(time.time()) - 10000
        t1 = t0 + 100
        t2 = t1 + 100
        t3 = t2 + 100

        EMPTY_LOG = "%013u %s %s\n" % (t0, GOODKEY_1, GOODKEY_2)
        entry1 = LogEntry(t1, GOODKEY_3, GOODKEY_4, 'jdd', 'document1')
        entry2 = LogEntry(t2, GOODKEY_5, GOODKEY_6, 'jdd', 'document2')
        entry3 = LogEntry(t3, GOODKEY_7, GOODKEY_8, 'jdd', 'document3')
        TEST_LOG = EMPTY_LOG + str(entry1) + str(entry2) + str(entry3)
        reader = StringReader(EMPTY_LOG)
        log = Log(reader)
        assert log is not None
        self.assertEqual(t0, log.timestamp)
        self.assertEqual(GOODKEY_1, log.prevHash)
        self.assertEqual(GOODKEY_2, log.prevMaster)
        self.assertEqual(0, len(log))

        log.addEntry(t1, GOODKEY_3, GOODKEY_4, 'jdd', 'document1')
        self.assertEqual(1, len(log))
        entry = log.getEntry(GOODKEY_3)
        self.assertTrue(entry1.equals(entry))
        self.assertTrue(GOODKEY_3 in log)
        self.assertFalse(GOODKEY_5 in log)

        log.addEntry(t2, GOODKEY_5, GOODKEY_6, 'jdd', 'document2')
        self.assertEqual(2, len(log))
        entry = log.getEntry(GOODKEY_5)
        self.assertTrue(entry2.equals(entry))
        self.assertTrue(GOODKEY_5 in log)

        log.addEntry(t3, GOODKEY_7, GOODKEY_8, 'jdd', 'document3')
        self.assertEqual(3, len(log))
        entry = log.getEntry(GOODKEY_7)
        self.assertTrue(entry3.equals(entry))
        self.assertTrue(GOODKEY_7 in log)


if __name__ == '__main__':
    unittest.main()
