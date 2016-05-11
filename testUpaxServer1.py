#!/usr/bin/python3

# testUpaxServer.py

import os
import time
import unittest
import rnglib
import upax
from xlattice import u256 as u

rng = rnglib.SimpleRNG(time.time())

DATA_PATH = 'myData'


class TestUpaxServer (unittest.TestCase):

    def setUp(self):
        uDir = os.path.join(DATA_PATH, rng.nextFileName(16))
        while os.path.exists(uDir):
            uDir = os.path.join(DATA_PATH, rng.nextFileName(16))
        self.uDir = uDir

    def tearDown(self):
        pass

    def testConstructFromNothing(self):
        # we are guaranteed that uDir does _not_ exist
        s = upax.BlockingServer(self.uDir, True)        # sha1
        self.assertIsNotNone(s)
        self.assertTrue(os.path.exists(s.uDir))

        # subdirectories
        self.assertTrue(os.path.exists(os.path.join(self.uDir, 'in')))
        self.assertTrue(os.path.exists(os.path.join(self.uDir, 'tmp')))

        # nodeID
        idPath = os.path.join(self.uDir, 'nodeID')
        self.assertTrue(os.path.exists(idPath))
        with open(idPath, 'rb') as f:
            nodeID = f.read()
        self.assertEqual(41, len(nodeID))
        self.assertEqual(ord('\n'), nodeID[40])
        nodeID = nodeID[:-1]

        s.close()
        self.assertTrue(os.path.exists(os.path.join(self.uDir, 'L')))   # GEEP

    def makeSomeFiles(self):
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
            dKey = u.fileSHA1Hex(dPath)
            files[dKey] = dPath

        self.assertEqual(fileCount, len(files))
        return files

    def testPutToEmpty(self):
        s = upax.BlockingServer(self.uDir, True)
        fileMap = self.makeSomeFiles()
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
            u.exists(self.uDir, key)
            entry = log.getEntry(key)
            self.assertIsNotNone(entry)
            # STUB: shold examine properties of log entry

        s.close()
        self.assertTrue(os.path.exists(os.path.join(self.uDir, 'L')))   # GEEP

    def testPutCloseReopenAndPut(self):
        s = upax.BlockingServer(self.uDir, True)
        fileMap1 = self.makeSomeFiles()
        fileCount1 = len(fileMap1)
        for key in list(fileMap1.keys()):
            s.put(fileMap1[key], key, 'testPut ... first phase')
        s.close()

        s = upax.BlockingServer(self.uDir, True)
        fileMap2 = self.makeSomeFiles()
        fileCount2 = len(fileMap2)
        for key in list(fileMap2.keys()):
            s.put(fileMap2[key], key, 'testPut ... SECOND PHASE')

        log = s.log
        self.assertEqual(fileCount1 + fileCount2, len(log))

        for key in list(fileMap1.keys()):
            u.exists(self.uDir, key)
            entry = log.getEntry(key)
            self.assertIsNotNone(entry)

        for key in list(fileMap2.keys()):
            u.exists(self.uDir, key)
            entry = log.getEntry(key)
            self.assertIsNotNone(entry)

        s.close()
        self.assertTrue(os.path.exists(os.path.join(self.uDir, 'L')))   # GEEP

if __name__ == '__main__':
    unittest.main()
