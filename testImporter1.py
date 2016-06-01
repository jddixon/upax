#!/usr/bin/env python3

# testImporter1.py

import os
import time
import unittest
import rnglib
from xlattice import u256 as u
from upax import *

rng = rnglib.SimpleRNG(time.time())

DATA_PATH = 'myData'


class TestImporter (unittest.TestCase):

    def setUp(self):
        srcDir = os.path.join(DATA_PATH, rng.nextFileName(16))
        while os.path.exists(srcDir):
            srcDir = os.path.join(DATA_PATH, rng.nextFileName(16))
        self.srcDir = srcDir

        destDir = os.path.join(DATA_PATH, rng.nextFileName(16))
        while os.path.exists(destDir):
            destDir = os.path.join(DATA_PATH, rng.nextFileName(16))
        self.destDir = destDir

    def tearDown(self):
        pass

    def makeSomeFiles(self):
        """ return a map: hash=>path """

        # create a random number of unique data files of random length
        #   in myData/; take hash of each as it is created, using
        #   this to verify uniqueness; add hash to list (or use hash
        #   as key to map
        fileCount = 17 + rng.nextInt16(128)
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
        return files                                                # FOO

    def constructEmptyUDir(self, uDir):
        # we are guaranteed that uDir does _not_ exist
        s = BlockingServer(uDir, True)          # True = sha1
        self.assertIsNotNone(s)
        self.assertTrue(os.path.exists(s.uDir))

        # subdirectories
        self.assertTrue(os.path.exists(os.path.join(uDir, 'in')))
        self.assertTrue(os.path.exists(os.path.join(uDir, 'tmp')))

        # nodeID
        idPath = os.path.join(uDir, 'nodeID')
        self.assertTrue(os.path.exists(idPath))
        with open(idPath, 'r') as f:
            nodeID = f.read()
        self.assertEqual(41, len(nodeID))
        self.assertEqual('\n', nodeID[40])
        nodeID = nodeID[:-1]

        self.assertTrue(os.path.exists(os.path.join(uDir, 'L')))   # GEEP
        return s

    def populateEmpty(self, s, fileMap):
        uDir = s.uDir
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
            u.exists(uDir, key)
            entry = log.getEntry(key)
            self.assertIsNotNone(entry)
            # STUB: shold examine properties of log entry
        self.assertTrue(os.path.exists(os.path.join(uDir, 'L')))   # GEEP

    def testImport(self):
        # create a collection of data files
        fileMap = self.makeSomeFiles()
        fileCount = len(fileMap)

        # create an empty source directory, populate it, shut down the server
        s0 = self.constructEmptyUDir(self.srcDir)
        self.populateEmpty(s0, fileMap)
        s0.close()

        # create an empty destination dir
        s1 = self.constructEmptyUDir(self.destDir)
        s1.close()

        # create and invoke the importer
        importer = Importer(self.srcDir, self.destDir,
                            'testImport ' + __version__, True)  # sha1
        importer.doImportUDir()

        # verify that the files got there
        s2 = BlockingServer(self.destDir, True)
        self.assertIsNotNone(s2)
        self.assertTrue(os.path.exists(s2.uDir))
        log = s2.log

        for key in list(fileMap.keys()):
            u.exists(self.destDir, key)
            entry = log.getEntry(key)
            self.assertIsNotNone(entry)

        s2.close()
        self.assertTrue(os.path.exists(os.path.join(self.destDir, 'L')))

if __name__ == '__main__':
    unittest.main()
