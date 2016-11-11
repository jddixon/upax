# dev/py/upax3/upax3/ftlog.py

import os
import re
# import sys
from collections import Container, Sized
from xlattice import (QQQ, check_using_sha,     # u,
                      SHA1_HEX_NONE, SHA2_HEX_NONE, SHA3_HEX_NONE)
from upax.node import check_hex_node_id_160, check_hex_node_id_256

__all__ = ['ATEXT', 'AT_FREE',
           'PATH_RE',
           'BODY_LINE_1_RE', 'BODY_LINE_256_RE',
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
ATEXT = r"[a-z0-9!#$%&'\*\+/=\?^_`{|}~\-]+"
AT_FREE = ATEXT + r'(?:\.' + ATEXT + r')*'

# this permits an RFC2822 message ID but is a little less restrictive
PATH_PAT = AT_FREE + r'(?:@' + AT_FREE + ')?'
PATH_RE = re.compile(PATH_PAT, re.I)

BODY_LINE_1_PAT =\
    r'^(\d+) ([0-9a-f]{40}) ([0-9a-f]{40}) "([^"]*)" (%s)$' % PATH_PAT
BODY_LINE_1_RE = re.compile(BODY_LINE_1_PAT, re.I)

BODY_LINE_256_PAT =\
    r'^(\d+) ([0-9a-f]{64}) ([0-9a-f]{64}) "([^"]*)" (%s)$' % PATH_PAT
BODY_LINE_256_RE = re.compile(BODY_LINE_256_PAT, re.I)


IGNORABLE_PAT = '(^ *$)|^ *#'
IGNORABLE_RE = re.compile(IGNORABLE_PAT)


class Log(Container, Sized):
    """a fault-tolerant log"""

    #__slots__ = ['_entries', '_index', '_timestamp',
    #              '_prevHash', '_prevMaster', '_using_sha', ]

    def __init__(self, reader, using_sha):
        self._using_sha = using_sha
        (timestamp, prev_log_hash, prev_master, entries, index) = reader.read()
        self._timestamp = timestamp     # seconds from epoch
        self._prev_hash = prev_log_hash   # SHA1/3 hash of previous log
        if using_sha == QQQ.USING_SHA1:
            check_hex_node_id_160(self._prev_hash)
        else:
            check_hex_node_id_256(self._prev_hash)
        self._prev_master = prev_master    # nodeID of master writing prev log
        if using_sha == QQQ.USING_SHA1:
            check_hex_node_id_160(self._prev_master)
        else:
            check_hex_node_id_256(self._prev_master)

        self._entries = entries       # a list
        self._index = index         # a map, hash => entry

    def __contains__(self, key):
        return key in self._index

    def __len__(self):
        return len(self._entries)

    def __str__(self):
        """used for serialization, so includes newline"""

        # first line
        if self._using_sha == QQQ.USING_SHA1:
            fmt = "%013u %40s %40s\n"
        else:
            fmt = "%013u %64s %64s\n"
        ret = fmt % (self._timestamp, self._prev_hash, self._prev_master)

        # list of entries
        for entry in self._entries:
            ret += str(entry)           # woefully inefficient :-)
        return ret

    def add_entry(self, tstamp, key, node_id, src, path):
        entry = LogEntry(tstamp, key, node_id, src, path)
        if key in self._index:
            existing = self._index[key]
            if entry == existing:
                return existing         # silently ignore duplicates
        self._entries.append(entry)     # increases size of list
        self._index[key] = entry        # overwrites any earlier duplicates
        return entry

    def get_entry(self, key):
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
    def prev_hash(self):
        return self._prev_hash

    @property
    def prev_master(self):
        return self._prev_master

    @property
    def timestamp(self):
        return self._timestamp

# -------------------------------------------------------------------


class BoundLog(Log):

    def __init__(self, reader, using_sha=QQQ.USING_SHA2,
                 u_path=None, base_name='L'):
        super(). __init__(reader, using_sha)
        self.fd_ = None
        self.is_open = False     # for appending
        overwriting = False
        if u_path:
            self.u_path = u_path
            self.base_name = base_name
            overwriting = True
        else:
            if isinstance(reader, FileReader):
                self.u_path = reader.u_path
                self.base_name = reader.base_name
                overwriting = False
            else:
                msg = "no target uPath/baseName specified"
                raise RuntimeError(msg)
        self.path_to_log = "%s/%s" % (self.u_path, self.base_name)
        if overwriting:
            with open(self.path_to_log, 'w') as file:
                log_contents = super(BoundLog, self).__str__()
                file.write(log_contents)
                file.close()
        self.fd_ = open(self.path_to_log, 'a')
        self.is_open = True

    def add_entry(self, tstamp, key, node_id, src, path):
        if not self.is_open:
            msg = "log file %s is not open for appending" % self.path_to_log
            raise RuntimeError(msg)

        # XXX NEED TO THINK ABOUT THE ORDER OF OPERATIONS HERE
        entry = super(
            BoundLog,
            self).add_entry(tstamp, key, node_id, src, path)
        stringified = str(entry)
        self.fd_.write(stringified)
        return entry

    def flush(self):
        self.fd_.flush()

    def close(self):
        self.fd_.close()
        self.is_open = False


# -------------------------------------------------------------------
class LogEntry():

    __slots__ = ['_timestamp', '_key', '_node_id', '_src', '_path', ]

    def __init__(self,
                 timestamp, key, node_id, source, pathToDoc):
        self._timestamp = timestamp      # seconds from epoch

        if key is None:
            raise ValueError('LogEntry key may not be None')
        using_sha = len(key) == 40
        self._key = key              # 40 or 64 hex digits, content hash
        if using_sha == QQQ.USING_SHA1:
            check_hex_node_id_160(self._key)
        else:
            check_hex_node_id_256(self._key)

        if node_id is None:
            raise ValueError('LogEntry nodeID may not be None')
        self._node_id = node_id           # 40/64 digits, node providing entry
        # XXX This is questionable.  Why can't a node with a SHA1 id store
        # a datum with a SHA3 key?
        if using_sha == QQQ.USING_SHA1:
            check_hex_node_id_160(self._node_id)
        else:
            check_hex_node_id_256(self._node_id)

        self._src = source           # tool or person responsible
        self._path = pathToDoc        # file name

    @property
    def key(self):
        return self._key

    @property
    def node_id(self):
        return self._node_id

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
    def using_sha(self):
        return len(self._key) == 40

    # used in serialization, so newlines are intended
    def __str__(self):
        if self.using_sha == QQQ.USING_SHA1:
            fmt = '%013u %40s %40s "%s" %s\n'
        else:
            fmt = '%013u %64s %64s "%s" %s\n'
        return fmt % (self._timestamp, self._key,
                      self._node_id, self._src, self._path)

    def __eq__(self, other):
        return isinstance(other, LogEntry)          and\
            self._timestamp == other.timestamp  and\
            self._key == other.key              and\
            self._node_id == other.node_id      and\
            self._src == other.src              and\
            self._path == other.path

    def __ne__(self, other):
        return not self.__eq__(other)

    def equals(self, other):
        """
        The function usualy known as __eq__.  XXX DEPRECATED
        """
        return self.__eq__(other)

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
    #__slots__ = ['_entries', '_index', '_lines', '_using_sha',
    #             'FIRST_LINE_RE', ]

    def __init__(self, lines, using_sha):
        check_using_sha(using_sha)
        self._using_sha = using_sha
        if using_sha == QQQ.USING_SHA1:
            first_line_pat = r'^(\d{13}) ([0-9a-f]{40}) ([0-9a-f]{40})$'
        else:
            first_line_pat = r'^(\d{13}) ([0-9a-f]{64}) ([0-9a-f]{64})$'
        self.first_line_re = re.compile(first_line_pat, re.I)

        # XXX verify that argument is an array of strings
        self._lines = lines

        ndx_last = len(self._lines) - 1
        # strip newline from last line if present
        if ndx_last >= 1:
            self._lines[ndx_last] = self._lines[ndx_last].rstrip('\n')

        # Entries are a collection, a list.  We also need a dictionary
        # that accesses each log entry using its hash.
        self._entries = []            # the empty list
        self._index = dict()        # mapping hash => entry

    @property
    def using_sha(self):
        return self._using_sha

    def read(self):

        # The first line contains timestamp, hash, nodeID for previous log.
        # Succeeding lines look like
        #  timestamp hash nodeID src path
        # In both cases timestamp is an unsigned int, the number of
        # milliseconds since the epoch.  It can be printed with %13u.
        # The current value (April 2011) is about 1.3 trillion (1301961973000).

        first_line = None
        if self._lines and len(self._lines) > 0:
            first_line = self._lines[0]
        if first_line:
            match = re.match(self.first_line_re, first_line)
            if not match:
                print("NO MATCH, FIRST LINE; using_sha = %s" % self.using_sha)
                print(("  FIRST LINE: '%s'" % first_line))
                raise ValueError("no match on first line; giving up")
            timestamp = int(match.group(1))
            prev_log_hash = match.group(2)
            prev_master = match.group(3)
            del self._lines[0]        # so we can cleanly iterate
        else:
            # no first line
            timestamp = 0
            if self._using_sha == QQQ.USING_SHA1:
                prev_log_hash = SHA1_HEX_NONE
                prev_master = SHA1_HEX_NONE
            elif self._using_sha == QQQ.USING_SHA2:
                prev_log_hash = SHA2_HEX_NONE
                prev_master = SHA2_HEX_NONE
            elif self._using_sha == QQQ.USING_SHA3:
                prev_log_hash = SHA3_HEX_NONE
                prev_master = SHA3_HEX_NONE

        entries = []
        index = dict()

        for line in self._lines:
            # Read each successive line, creating an entry for each and
            # indexing each.  Ignore blank lines and those beginning with
            # a hash ('#')
            match = re.match(IGNORABLE_RE, line)
            if match:
                continue
            if self._using_sha == QQQ.USING_SHA1:
                match = re.match(BODY_LINE_1_RE, line)
            else:
                match = re.match(BODY_LINE_256_RE, line)
            if match:
                tstamp = int(match.group(1))
                key = match.group(2)
                node_id = match.group(3)
                src = match.group(4)
                path = match.group(5)
                # constructor should catch invalid fields
                entry = LogEntry(tstamp, key, node_id, src, path)
                entries.append(entry)
                index[key] = entry
            else:
                msg = "not a valid log entry line: '%s'" % line
                raise RuntimeError(msg)

        return (timestamp, prev_log_hash, prev_master, entries, index)

# -------------------------------------------------------------------


class FileReader(Reader):
    """
    Accept uPath and optionally log file name, read entire file into
    a string array, pass to Reader.
    """
    __slots__ = ['_u_path', '_base_name', '_log_file', ]

    # XXX CHECK ORDER OF ARGUMENTS
    def __init__(self, u_path, using_sha=False, base_name="L"):
        if not os.path.exists(u_path):
            raise RuntimeError("no such directory %s" % u_path)
        self._u_path = u_path
        self._base_name = base_name
        self._log_file = "%s/%s" % (self._u_path, base_name)
        with open(self._log_file, 'r') as file:
            contents = file.read()
        lines = contents.split('\n')
        super(FileReader, self).__init__(lines, using_sha)

    @property
    def base_name(self):
        return self._base_name

    @property
    def log_file(self):
        return self._log_file

    @property
    def u_path(self):
        return self._u_path

# -------------------------------------------------------------------


class StringReader(Reader):
    """
    Accept a (big) string, convert to a string array, pass to Reader
    """

    def __init__(self, bigString, using_sha=False):

        # split on newlines
        lines = bigString.split('\n')

        super().__init__(lines, using_sha)
