#!/usr/bin/env python3

# testZMQ.py
import time
import unittest
import zmq
from upax.ftlog import LogEntry

context = zmq.Context.instance()


class TestZMQ (unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_constructor(self):
        pass

if __name__ == '__main__':
    unittest.main()
