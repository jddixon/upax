# upax/__init__.py

import binascii
import os
import re
import time
import rnglib
from xlattice import u256 as u
import upax.ftlog

__version__ = '0.6.4'
__version_date__ = '2016-06-01'

__all__ = ['__version__', '__version_date__',
           'Importer',
           'Server', 'BlockingServer', 'NonBlockingServer',
           ]

# PATs AND REs ######################################################
DIR_NAME_PAT = '^[0-9a-fA-F]{2}$'
DIR_NAME_RE = re.compile(DIR_NAME_PAT)

FILE_NAME_1_PAT = '^[0-9a-fA-F]{40}$'
FILE_NAME_1_RE = re.compile(FILE_NAME_1_PAT)

FILE_NAME_2_PAT = '^[0-9a-fA-F]{64}$'
FILE_NAME_2_RE = re.compile(FILE_NAME_2_PAT)

# -- classes --------------------------------------------------------


class Importer(object):

    def __init__(self, srcDir, destDir, pgmNameAndVersion, usingSHA1=False,
                 verbose=False):
        self._srcDir = srcDir
        self._destDir = destDir
        self._pgmNameAndVersion = pgmNameAndVersion
        self._usingSHA1 = usingSHA1
        self._verbose = verbose

    @property
    def srcDir(self): return self._srcDir

    def desDir(self): return self._desDir

    def pgmNameAndVersion(self): return self._pgmNameAndVersion

    def verbose(self): return self._verbose

    @staticmethod
    def createImporter(args):
        return Importer(args.srcDir, args.destDir,
                        args.pgmNameAndVersion, args.usingSHA1,
                        args.verbose)

    def importLeafDir(self, subSub):
        src = self._pgmNameAndVersion
        s = self._server

        count = 0
        files = sorted(os.listdir(subSub))
        for file in files:
            # leaf file names should be the file's SHA hash, its content key
            pathTo = os.path.join(subSub, file)
            if self._usingSHA1:
                m = FILE_NAME_1_RE.match(file)
            else:
                m = FILE_NAME_2_RE.match(file)
            if m is not None:
                count += 1
                if self._verbose:
                    print(('      ' + pathTo))
                (length, actualHash) = s.put(pathTo, file, src)
                if actualHash != file:
                    print(("%s has content key %s" % (pathTo, actualHash)))
            else:
                print(("not a proper leaf file name: " + pathTo))
        self._count += count

    def importSubDir(self, subDir):
        files = sorted(os.listdir(subDir))
        for subSub in files:
            pathTo = os.path.join(subDir, subSub)
            if DIR_NAME_RE.match(subSub):
                if self._verbose:
                    print(('    ' + pathTo))
                self.importLeafDir(pathTo)
            else:
                print(("not a proper subsubdirectory: " + pathTo))

    def doImportUDir(self):
        srcDir = self._srcDir
        destDir = self._destDir
        verbose = self._verbose
        # os.umask(0222)       # CAN'T USE THIS

        self._server = BlockingServer(destDir, self._usingSHA1)
        log = self._server.log
        if verbose:
            print(("there were %7u files in %s at the beginning of the run" % (
                len(log), srcDir)))
        self._count = 0
        srcDir = self._srcDir
        if self._verbose:
            print(srcDir)
        try:
            files = sorted(os.listdir(srcDir))
            for subDir in files:
                pathTo = os.path.join(srcDir, subDir)
                if DIR_NAME_RE.match(subDir):
                    if self._verbose:
                        print(('  ' + pathTo))
                    self.importSubDir(pathTo)
                elif    subDir != 'L'       and \
                        subDir != 'in'      and \
                        subDir != 'nodeID'  and \
                        subDir != 'tmp':
                    print(("not a proper subdirectory: " + pathTo))
        finally:
            print(("processed %u files" % self._count))
            self._server.close()                                       # GEEP


class Server(object):

    #   __slots__ = ['_uDir', '_usingSHA1', ]

    def __init__(self, uDir, usingSHA1=False):
        """
        We expect uDir to contain two subdirectories, in/ and tmp/, and
        at least two files, L and nodeID.  L is the serialization of a
        BoundLog.  nodeID contains a 40- or 64-byte sequence of hex digits
        followed by a newline.  This should be unique.

        A non-empty uDir is a DIR256x256 structure, and so it also contains
        subdirectories whose names are two hex digits and each such
        subdirectory will contain subdirectories whose names are two hex
        digits.  Data is stored in these subdirectories; at this time
        data files are named by their SHA1 content keys and so file names
        consist of 40 or 64 hex digits.  The first two hex digits of the contena
        key select the uDir subdirectory holding the data file and the
        second two hex digits select the subsubdirectory.

        All files in uDir should be owned by upax.upax and are (at least
        at this time) world-readable but only owner-writeable.
        """
        self._usingSHA1 = usingSHA1
        if not uDir:
            raise ValueError('uDir must be specified')
        self._uDir = uDir

        _inDirPath = os.path.join(uDir, 'in')
        _logFilePath = os.path.join(uDir, 'L')
        _idFilePath = os.path.join(uDir, 'nodeID')
        _tmpDirPath = os.path.join(uDir, 'tmp')

        if not os.path.exists(uDir):
            os.makedirs(uDir)
        if not os.path.exists(_inDirPath):
            os.mkdir(_inDirPath)
        if not os.path.exists(_tmpDirPath):
            os.mkdir(_tmpDirPath)
        if not os.path.exists(_idFilePath):
            if self._usingSHA1:
                byteID = bytearray(20)
            else:
                byteID = bytearray(32)
            rng = rnglib.SimpleRNG(time.time())
            rng.nextBytes(byteID)       # a low-quality quasi-random number
            id = str(binascii.b2a_hex(byteID), 'utf-8')
            self._nodeID = id
            with open(_idFilePath, 'w') as f:
                f.write(id + '\n')
        else:
            # XXX many possible problems here!
            with open(_idFilePath, 'r') as f:
                self._nodeID = f.read()[:-1]

        if not os.path.exists(_logFilePath):
            self._log = ftlog.BoundLog(ftlog.Reader([], self._usingSHA1),
                                       self._usingSHA1, uDir)
        else:
            self._log = ftlog.BoundLog(ftlog.FileReader(uDir, self._usingSHA1),
                                       self._usingSHA1, uDir)

    def exists(self, key):
        return u.exists(self._uDir, key)

    def get(self, key):
        if self._usingSHA1:
            return u.getData1(self._uDir, key)
        else:
            return u.getData2(self._uDir, key)

    def put(self, pathToFile, key, source, loggedPath=None):
        """
        returns (len, hash)
        """

        #----------------------------------------------------
        # XXX THIS IS A HACK but perhaps suggests a way to go
        #----------------------------------------------------
        if loggedPath is None:
            loggedPath = 'z@' + pathToFile

        if self._usingSHA1:
            actualKey = u.fileSHA1Hex(pathToFile)
        else:
            actualKey = u.fileSHA2Hex(pathToFile)
        if actualKey != key:
            raise ValueError('actual hash %s, claimed hash %s' % (
                actualKey, key))

        if u.exists(self._uDir, key):
            return (-1, key)

        # XXX uses tempfile package, so not secure XXX
        if self._usingSHA1:
            (len, hash) = u.copyAndPut1(pathToFile, self._uDir, key)
        else:
            (len, hash) = u.copyAndPut2(pathToFile, self._uDir, key)

        # XXX should deal with exceptions
        self._log.addEntry(time.time(), key, self._nodeID, source, loggedPath)
        return (len, hash)

    def putData(self, data, key, source,
                loggedPath='z@__posted_data__'):
        """ returns (len, hash) """
        if self._usingSHA1:
            (len, hash) = u.putData1(data, self._uDir, key)
        else:
            (len, hash) = u.putData2(data, self._uDir, key)
        # XXX should deal with exceptions

        self._log.addEntry(time.time(), key, self._nodeID, source, loggedPath)
        return (len, hash)

    def close(self):
        self._log.close()

    # XXXthis is a reference to the actual log and so a major security risk
    @property
    def log(self): return self._log

    @property
    def nodeID(self): return self._nodeID

    @property
    def uDir(self): return self._uDir


class BlockingServer(Server):

    def __init__(self, uDir, usingSHA1=False):
        super(BlockingServer, self).__init__(uDir, usingSHA1)


class NonBlockingServer(Server):

    def __init__(self, uDir, usingSHA1=False):
        pass
