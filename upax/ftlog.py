# dev/py/upax3/upax3/ftlog.py

import os
import re
import sys
from collections import Container, Sized
from xlattice import Q, SHA1_HEX_NONE, SHA2_HEX_NONE, u
from upax.node import checkHexNodeID1, checkHexNodeID2

__all__ = ['ATEXT', 'AT_FREE',
           'PATH_RE',
           'BODY_LINE_1_RE', 'BODY_LINE_3_RE',
           'IGNORABLE_RE',

           # classes
           'Log', 'BoundLog', 'LogEntry',
           'Reader', 'FileReader', 'StringReader',
           ]
# -------------------------------------------------------------------
# CLASS LOG AND SUBCLASSES
# -------------------------------------------------------------------

# Take care: this pattern is used in xlmfilter, possibly elsewhere
# this is RFC2822's atext; *,+,?,- are escaped; needs to be enclosed in []+
ATEXT = "[a-z0-9!#$%&'\*\+/=\?^_`{|}~\-]+"
AT_FREE = ATEXT + '(?:\.' + ATEXT + ')*'

# this permits an RFC2822 message ID but is a little less restrictive
PATH_PAT = AT_FREE + '(?:@' + AT_FREE + ')?'
PATH_RE = re.compile(PATH_PAT, re.I)

BODY_LINE_1_PAT   = \
    '^(\d+) ([0-9a-f]{40}) ([0-9a-f]{40}) "([^"]*)" (%s)$' % PATH_PAT
BODY_LINE_1_RE = re.compile(BODY_LINE_1_PAT, re.I)

BODY_LINE_3_PAT   = \
    '^(\d+) ([0-9a-f]{64}) ([0-9a-f]{64}) "([^"]*)" (%s)$' % PATH_PAT
BODY_LINE_3_RE = re.compile(BODY_LINE_3_PAT, re.I)


IGNORABLE_PAT = '(^ *$)|^ *#'
IGNORABLE_RE = re.compile(IGNORABLE_PAT)


class Log(Container, Sized):
    """a fault-tolerant log"""

    #__slots__ = ['_entries', '_index', '_timestamp',
    #              '_prevHash', '_prevMaster', '_usingSHA', ]

    def __init__(self, reader, usingSHA):
        self._usingSHA = usingSHA
        (timestamp, prevLogHash, prevMaster, entries, index) = reader.read()
        self._timestamp = timestamp     # seconds from epoch
        self._prevHash = prevLogHash   # SHA1/3 hash of previous log
        if usingSHA == Q.USING_SHA1:
            checkHexNodeID1(self._prevHash)
        else:
            # FIX ME FIX ME FIX ME
            checkHexNodeID2(self._prevHash)
        self._prevMaster = prevMaster    # nodeID of master writing prev log
        if usingSHA == Q.USING_SHA1:
            checkHexNodeID1(self._prevMaster)
        else:
            # FIX ME FIX ME FIX ME
            checkHexNodeID2(self._prevMaster)

        self._entries = entries       # a list
        self._index = index         # a map, hash => entry

    def __contains__(self, key):
        return key in self._index

    def __len__(self):
        return len(self._entries)

    def __str__(self):
        """used for serialization, so includes newline"""

        # first line
        if self._usingSHA == Q.USING_SHA1:
            fmt = "%013u %40s %40s\n"
        else:
            # FIX ME FIX ME FIX ME
            fmt = "%013u %64s %64s\n"
        ret = fmt % (self._timestamp, self._prevHash, self._prevMaster)

        # list of entries
        for entry in self._entries:
            ret += str(entry)           # woefully inefficient :-)
        return ret

    def addEntry(self, t, key, nodeID, src, path):
        entry = LogEntry(t, key, nodeID, src, path)
        if key in self._index:
            existing = self._index[key]
            if entry == existing:
                return existing         # silently ignore duplicates
        self._entries.append(entry)     # increases size of list
        self._index[key] = entry        # overwrites any earlier duplicates
        return entry

    def getEntry(self, key):
        if not key in self._index:
            return None
        else:
            return self._index[key]

    @property
    def entries(self):
        return self._entries

    @property
    def index(self):
        return self._index

    @property
    def prevHash(self):
        return self._prevHash

    @property
    def prevMaster(self):
        return self._prevMaster

    @property
    def timestamp(self):
        return self._timestamp

# -------------------------------------------------------------------


class BoundLog(Log):

    def __init__(self, reader, usingSHA=Q.USING_SHA2,
                 uPath=None, baseName='L'):
        super(). __init__(reader, usingSHA)
        self.fd = -1
        self.isOpen = False     # for appending
        overwriting = False
        if uPath:
            self.uPath = uPath
            self.baseName = baseName
            overwriting = True
        else:
            if isinstance(reader, FileReader):
                self.uPath = reader.uPath
                self.baseName = reader.baseName
                overwriting = False
            else:
                msg = "no target uPath/baseName specified"
                raise RuntimeError(msg)
        self.pathToLog = "%s/%s" % (self.uPath, self.baseName)
        if overwriting:
            with open(self.pathToLog, 'w') as f:
                logContents = super(BoundLog, self).__str__()
                f.write(logContents)
                f.close()
        self.fd = open(self.pathToLog, 'a')
        self.isOpen = True

    def addEntry(self, t, key, nodeID, src, path):
        if not self.isOpen:
            msg = "log file %s is not open for appending" % self.pathToLog
            raise RuntimeError(msg)

        # XXX NEED TO THINK ABOUT THE ORDER OF OPERATIONS HERE
        entry = super(BoundLog, self).addEntry(t, key, nodeID, src, path)
        stringified = str(entry)
        self.fd.write(stringified)
        return entry

    def flush(self):
        self.fd.flush()

    def close(self):
        self.fd.close()
        self.isOpen = False


# -------------------------------------------------------------------
class LogEntry():

    __slots__ = ['_timestamp', '_key', '_nodeID', '_src', '_path', ]

    def __init__(self,
                 timestamp, key, nodeID, source, pathToDoc):
        self._timestamp = timestamp      # seconds from epoch

        if key is None:
            raise ValueError('LogEntry key may not be None')
        usingSHA = len(key) == 40
        self._key = key              # 40 or 64 hex digits, content hash
        if usingSHA == Q.USING_SHA1:
            # FIX ME FIX ME FIX ME
            checkHexNodeID1(self._key)
        else:
            checkHexNodeID2(self._key)

        if nodeID is None:
            raise ValueError('LogEntry nodeID may not be None')
        self._nodeID = nodeID           # 40/64 digits, node providing entry
        # XXX This is questionable.  Why can't a node with a SHA1 id store
        # a datum with a SHA3 key?
        if usingSHA == Q.USING_SHA1:
            # FIX ME FIX ME FIX ME
            checkHexNodeID1(self._nodeID)
        else:
            checkHexNodeID2(self._nodeID)

        self._src = source           # tool or person responsible
        self._path = pathToDoc        # file name

    @property
    def key(self):
        return self._key

    @property
    def nodeID(self):
        return self._nodeID

    @property
    def path(self):
        return self._path

    @property
    def src(self):
        return self._src

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def usingSHA(self):
        return len(self._key) == 40

    # used in serialization, so newlines are intended
    def __str__(self):
        if self.usingSHA == Q.USING_SHA1:
            fmt = '%013u %40s %40s "%s" %s\n'
        else:
            # FIX ME FIX ME FIX ME
            fmt = '%013u %64s %64s "%s" %s\n'
        return fmt % (self._timestamp, self._key,
                      self._nodeID, self._src, self._path)

    def __eq__(self, other):
        return self.equals(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def equals(self, other):
        """
        The function usualy known as __eq__.
        """
        if isinstance(other, LogEntry)              and \
                self._timestamp == other.timestamp  and \
                self._key       == other.key        and \
                self._nodeID    == other.nodeID     and \
                self._src       == other.src        and \
                self._path == other.path:
            return True
        else:
            return False

# -------------------------------------------------------------------
# CLASS READER AND SUBCLASSES
# -------------------------------------------------------------------
# Would prefer to be able to handle this through something like a Java
# Reader, so that we could test with a StringReader but then use a
# FileReader in production.  If it is a file, file.readlines(sizeHint)
# supposedly has very good preformance for larger sizeHint, say 100KB
# It appears that lines returned need to be rstripped, which wastefully
# requires copying
#
# For our purposes, string input can just be split on newlines, which
# has the benefit of effectively chomping at the same time


class Reader(object):
    #__slots__ = ['_entries', '_index', '_lines', '_usingSHA',
    #             'FIRST_LINE_RE', ]

    def __init__(self, lines, usingSHA):
        self._usingSHA = usingSHA
        if usingSHA == Q.USING_SHA1:
            FIRST_LINE_PAT = '^(\d{13}) ([0-9a-f]{40}) ([0-9a-f]{40})$'
        else:
            # FIX ME FIX ME FIX ME
            FIRST_LINE_PAT = '^(\d{13}) ([0-9a-f]{64}) ([0-9a-f]{64})$'
        self.FIRST_LINE_RE = re.compile(FIRST_LINE_PAT, re.I)

        # XXX verify that argument is an array of strings
        self._lines = lines

        ndxLast = len(self._lines) - 1
        # strip newline from last line if present
        if ndxLast >= 1:
            self._lines[ndxLast] = self._lines[ndxLast].rstrip('\n')

        # Entries are a collection, a list.  We also need a dictionary
        # that accesses each log entry using its hash.
        self._entries = []            # the empty list
        self._index = dict()        # mapping hash => entry

    @property
    def usingSHA(self): return self._usingSHA

    def read(self):

        # The first line contains timestamp, hash, nodeID for previous log.
        # Succeeding lines look like
        #  timestamp hash nodeID src path
        # In both cases timestamp is an unsigned int, the number of
        # milliseconds since the epoch.  It can be printed with %13u.
        # The current value (April 2011) is about 1.3 trillion (1301961973000).

        firstLine = None
        if self._lines and len(self._lines) > 0:
            firstLine = self._lines[0]
        if firstLine:
            m = re.match(self.FIRST_LINE_RE, firstLine)
            if not m:
                print("NO MATCH, FIRST LINE; usingSHA = %s" % self.usingSHA)
                print(("  FIRST LINE: '%s'" % firstLine))
                raise ValueError("no match on first line; giving up")
            timestamp = int(m.group(1))
            prevLogHash = m.group(2)
            prevMaster = m.group(3)
            del self._lines[0]        # so we can cleanly iterate
        else:
            # no first line
            timestamp = 0
            if self._usingSHA == Q.USING_SHA1:
                prevLogHash = SHA1_HEX_NONE
                prevMaster = SHA1_HEX_NONE
            else:
                # FIX ME FIX ME FIX ME
                prevLogHash = SHA2_HEX_NONE
                prevMaster = SHA2_HEX_NONE

        entries = []
        index = dict()

        for line in self._lines:
            # Read each successive line, creating an entry for each and
            # indexing each.  Ignore blank lines and those beginning with
            # a hash ('#')
            m = re.match(IGNORABLE_RE, line)
            if m:
                continue
            if self._usingSHA == Q.USING_SHA1:
                m = re.match(BODY_LINE_1_RE, line)
            else:
                # FIX ME FIX ME FIX ME
                m = re.match(BODY_LINE_3_RE, line)
            if m:
                t = int(m.group(1))
                key = m.group(2)
                nodeID = m.group(3)
                src = m.group(4)
                path = m.group(5)
                # constructor should catch invalid fields
                entry = LogEntry(t, key, nodeID, src, path)
                entries.append(entry)
                index[key] = entry
            else:
                msg = "not a valid log entry line: '%s'" % line
                raise RuntimeError(msg)

        return (timestamp, prevLogHash, prevMaster, entries, index)

# -------------------------------------------------------------------


class FileReader(Reader):
    """
    Accept uPath and optionally log file name, read entire file into
    a string array, pass to Reader.
    """
    __slots__ = ['_uPath', '_baseName', '_logFile', ]

    # XXX CHECK ORDER OF ARGUMENTS
    def __init__(self, uPath, usingSHA=False, baseName="L"):
        if not os.path.exists(uPath):
            raise RuntimeError("no such directory %s" % uPath)
        self._uPath = uPath
        self._baseName = baseName
        self._logFile = "%s/%s" % (self._uPath, baseName)
        with open(self._logFile, 'r') as f:
            contents = f.read()
        lines = contents.split('\n')
        super(FileReader, self).__init__(lines, usingSHA)

    @property
    def baseName(self):
        return self._baseName

    @property
    def logFile(self):
        return self._logFile

    @property
    def uPath(self):
        return self._uPath

# -------------------------------------------------------------------


class StringReader(Reader):
    """
    Accept a (big) string, convert to a string array, pass to Reader
    """

    def __init__(self, bigString, usingSHA=False):

        # split on newlines
        lines = bigString.split('\n')

        super().__init__(lines, usingSHA)
