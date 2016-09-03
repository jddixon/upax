#!/usr/bin/env python3

# testUpaxServer.py

import os
import time
import unittest
import rnglib
import upax
from xlattice import u256 as u, Q

rng = rnglib.SimpleRNG(time.time())

DATA_PATH = 'myData'


class TestUpaxServer (unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def doTestConstructFromNothing(self, usingSHA):
        # SETUP
        uPath = os.path.join(DATA_PATH, rng.nextFileName(16))
        while os.path.exists(uPath):
            uPath = os.path.join(DATA_PATH, rng.nextFileName(16))

        # we are guaranteed that uPath does _not_ exist
        s = upax.BlockingServer(uPath, usingSHA)
        self.assertIsNotNone(s)
        self.assertTrue(os.path.exists(s.uPath))

        # subdirectories
        self.assertTrue(os.path.exists(os.path.join(uPath, 'in')))
        self.assertTrue(os.path.exists(os.path.join(uPath, 'tmp')))

        # nodeID
        idPath = os.path.join(uPath, 'nodeID')
        self.assertTrue(os.path.exists(idPath))
        with open(idPath, 'rb') as f:
            nodeID = f.read()
        if usingSHA == Q.USING_SHA1:
            self.assertEqual(41, len(nodeID))
            self.assertEqual(ord('\n'), nodeID[40])
        else:
            # FIX ME FIX ME FIX ME
            self.assertEqual(65, len(nodeID))
            self.assertEqual(ord('\n'), nodeID[64])
        nodeID = nodeID[:-1]

        s.close()
        self.assertTrue(os.path.exists(os.path.join(uPath, 'L')))   # GEEP

    def testConstructFromNothing(self):
        self.doTestConstructFromNothing(True)
        self.doTestConstructFromNothing(False)

    # ---------------------------------------------------------------

    def makeSomeFiles(self, usingSHA):
        """ return a map: hash=>path """

        # create a random number of unique data files of random length
        #   in myData/; take hash of each as it is created, using
        #   this to verify uniqueness; add hash to list (or use hash
        #   as key to map
        fileCount = 3 + rng.nextInt16(16)
        files = {}             # a map hash->path
        for n in range(fileCount):
            dKey = None
            dPath = None
            # create a random file name                  maxLen   minLen
            (dLen, dPath) = rng.nextDataFile(DATA_PATH, 16 * 1024, 1)
            # perhaps more restrictions needed
            while dPath.endswith('.'):
                (dLen, dPath) = rng.nextDataFile(DATA_PATH, 16 * 1024, 1)
            if usingSHA == Q.USING_SHA1:
                dKey = u.fileSHA1Hex(dPath)
            else:
                # FIX ME FIX ME FIX ME
                dKey = u.fileSHA2Hex(dPath)
            files[dKey] = dPath

        self.assertEqual(fileCount, len(files))
        return files

    # ---------------------------------------------------------------

    def doTestPutToEmpty(self, usingSHA):
        # SETUP
        uPath = os.path.join(DATA_PATH, rng.nextFileName(16))
        while os.path.exists(uPath):
            uPath = os.path.join(DATA_PATH, rng.nextFileName(16))

        s = upax.BlockingServer(uPath, usingSHA)
        fileMap = self.makeSomeFiles(usingSHA)
        fileCount = len(fileMap)

        for key in list(fileMap.keys()):
            s.put(fileMap[key], key, 'testPutToEmpty')
            self.assertTrue(s.exists(key))
            with open(fileMap[key], 'rb') as f:
                dataInFile = f.read()
            dataInU = s.get(key)
            self.assertEqual(dataInFile, dataInU)

        log = s.log
        self.assertEqual(fileCount, len(log))

        for key in list(fileMap.keys()):
            u.exists(uPath, key)
            entry = log.getEntry(key)
            self.assertIsNotNone(entry)
            # STUB: shold examine properties of log entry

        s.close()
        self.assertTrue(os.path.exists(os.path.join(uPath, 'L')))   # GEEP

    def testPutToEmpty(self):
        self.doTestPutToEmpty(True)
        self.doTestPutToEmpty(False)

    # ---------------------------------------------------------------

    def doTestPutCloseReopenAndPut(self, usingSHA):
        # SETUP
        uPath = os.path.join(DATA_PATH, rng.nextFileName(16))
        while os.path.exists(uPath):
            uPath = os.path.join(DATA_PATH, rng.nextFileName(16))

        s = upax.BlockingServer(uPath, usingSHA)
        fileMap1 = self.makeSomeFiles(usingSHA)
        fileCount1 = len(fileMap1)
        for key in list(fileMap1.keys()):
            s.put(fileMap1[key], key, 'testPut ... first phase')
        s.close()

        s = upax.BlockingServer(uPath, usingSHA)
        fileMap2 = self.makeSomeFiles(usingSHA)
        fileCount2 = len(fileMap2)
        for key in list(fileMap2.keys()):
            s.put(fileMap2[key], key, 'testPut ... SECOND PHASE')

        log = s.log
        self.assertEqual(fileCount1 + fileCount2, len(log))

        for key in list(fileMap1.keys()):
            u.exists(uPath, key)
            entry = log.getEntry(key)
            self.assertIsNotNone(entry)

        for key in list(fileMap2.keys()):
            u.exists(uPath, key)
            entry = log.getEntry(key)
            self.assertIsNotNone(entry)

        s.close()
        self.assertTrue(os.path.exists(os.path.join(uPath, 'L')))   # GEEP

    def testPutCloseReopenAndPut(self):
        self.doTestPutCloseReopenAndPut(True)
        self.doTestPutCloseReopenAndPut(False)

if __name__ == '__main__':
    unittest.main()
