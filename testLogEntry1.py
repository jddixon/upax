#!/usr/bin/env python3

# testLogEntry.py
import time
import unittest

# confuson between module and class
from upax.ftlog import LogEntry
from xlattice import Q


class TestLogEntry (unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def getKeys(self, usingSHA):
        if usingSHA == Q.USING_SHA1:
            GOODKEY_1 = '0123456789012345678901234567890123456789'
            GOODKEY_2 = 'fedcba9876543210fedcba9876543210fedcba98'
        else:
            # FIX ME FIX ME FIX ME
            GOODKEY_1 = '0123456789012345678901234567890123456789abcdefghi0123456789abcde'
            GOODKEY_2 = 'fedcba9876543210fedcba9876543210fedcba98012345678901234567890123'
        return (GOODKEY_1, GOODKEY_2)

    def doTestConstructor(self, usingSHA):

        (GOODKEY_1, GOODKEY_2) = self.getKeys(usingSHA)

        entry = LogEntry(time.time(), GOODKEY_1, GOODKEY_2, 'jdd', 'document1')

        assert entry is not None
        deltaT = time.time() - entry.timestamp
        self.assertTrue(deltaT >= 0 and deltaT <= 5)
        self.assertEqual(GOODKEY_1, entry.key)
        self.assertEqual(GOODKEY_2, entry.nodeID)
        self.assertEqual('jdd', entry.src)
        self.assertEqual('document1', entry.path)

    def testConstructor(self):
        self.doTestConstructor(True)
        self.doTestConstructor(False)

    def doTest__str__(self, usingSHA):
        (GOODKEY_1, GOODKEY_2) = self.getKeys(usingSHA)

        now = time.time()
        entry = LogEntry(now, GOODKEY_1, GOODKEY_2, 'jdd', 'document1')

        expected = '%013u %40s %40s "%s" %s\n' % (now, GOODKEY_1, GOODKEY_2,
                                                  'jdd', 'document1')
        actual = entry.__str__()
        self.assertEqual(expected, actual)

    def test__str__(self):
        self.doTest__str__(True)
        self.doTest__str__(False)

    def doTestEquals(self, usingSHA):
        (GOODKEY_1, GOODKEY_2) = self.getKeys(usingSHA)

        time1 = time.time() - 1000
        time2 = time1 + 500
        entry1 = LogEntry(time1, GOODKEY_1, GOODKEY_2, 'jdd', 'document1')
        entry2 = LogEntry(time2, GOODKEY_1, GOODKEY_2, 'jdd', 'document1')
        # same values as entry1
        entry3 = LogEntry(time1, GOODKEY_1, GOODKEY_2, 'jdd', 'document1')

        self.assertFalse(entry1.equals(1))
        self.assertTrue(entry1.equals(entry1))
        self.assertFalse(entry1.equals(entry2))     # times differ
        self.assertTrue(entry1.equals(entry3))

    def testEquals(self):
        self.doTestEquals(True)
        self.doTestEquals(False)

if __name__ == '__main__':
    unittest.main()
