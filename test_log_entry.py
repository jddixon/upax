#!/usr/bin/env python3
# testLogEntry.py

""" Test opertions on LogEntry. """

import time
import unittest

# confuson between module and class
from upax.ftlog import LogEntry
from xlattice import QQQ, check_using_sha


class TestLogEntry(unittest.TestCase):
    """ Test opertions on LogEntry. """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def get_keys(self, using_sha):
        """
        Return a pair of content keys useful in tests involving a specific
        SHA hash type.
        """
        check_using_sha(using_sha)

        if using_sha == QQQ.USING_SHA1:
            goodkey_1 = '0123456789012345678901234567890123456789'
            goodkey_2 = 'fedcba9876543210fedcba9876543210fedcba98'
        else:
            # dummy data good for either SHA2 or SHA3
            goodkey_1 = '0123456789012345678901234567890123456789abcdefghi0123456789abcde'
            goodkey_2 = 'fedcba9876543210fedcba9876543210fedcba98012345678901234567890123'
        return (goodkey_1, goodkey_2)

    def do_test_consructor(self, using_sha):
        """ Test the LogEntry constructor using a specific SHA hash type. """
        check_using_sha(using_sha)

        (goodkey_1, goodkey_2) = self.get_keys(using_sha)

        entry = LogEntry(time.time(), goodkey_1, goodkey_2, 'jdd', 'document1')

        assert entry is not None
        delta_t = time.time() - entry.timestamp
        self.assertTrue(delta_t >= 0 and delta_t <= 5)
        self.assertEqual(goodkey_1, entry.key)
        self.assertEqual(goodkey_2, entry.node_id)
        self.assertEqual('jdd', entry.src)
        self.assertEqual('document1', entry.path)

    def test_constructor(self):
        """
        Test the LogEntry constructor for the types of SHA hash supported.
        """
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_consructor(using)

    def do_test_to_string(self, using_sha):
        """
        Test conversion of a LogEntry to string for a specific hash type.
        """
        (goodkey_1, goodkey_2) = self.get_keys(using_sha)

        now = time.time()
        entry = LogEntry(now, goodkey_1, goodkey_2, 'jdd', 'document1')

        expected = '%013u %40s %40s "%s" %s\n' % (now, goodkey_1, goodkey_2,
                                                  'jdd', 'document1')
        actual = entry.__str__()
        self.assertEqual(expected, actual)

    def test__str__(self):
        """
        Test conversion of LogEntry to string form for the SHA hash
        types supported.
        """
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_to_string(using)

    def do_test_equals(self, using_sha):
        """ Test the LogEntry __eq__ function for a specific SHA hash type."""
        check_using_sha(using_sha)
        (goodkey_1, goodkey_2) = self.get_keys(using_sha)

        time1 = time.time() - 1000
        time2 = time1 + 500
        entry1 = LogEntry(time1, goodkey_1, goodkey_2, 'jdd', 'document1')
        entry2 = LogEntry(time2, goodkey_1, goodkey_2, 'jdd', 'document1')
        entry3 = LogEntry(time1, goodkey_1, goodkey_2, 'jdd', 'document1')

        self.assertTrue(entry1 == entry1)       # identical
        self.assertTrue(entry1 == entry3)       # same attributes
        self.assertFalse(entry1 == entry2)      # times differ
        self.assertFalse(entry2 == entry3)      # times differ

    def test_equals(self):
        """
        Test the LogEntry __eq__ function for the various SHA hash types
        supported.
        """
        for using in [QQQ.USING_SHA1, QQQ.USING_SHA2, QQQ.USING_SHA3, ]:
            self.do_test_equals(using)

if __name__ == '__main__':
    unittest.main()
