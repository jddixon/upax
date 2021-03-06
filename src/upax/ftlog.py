# dev/py/upax3/upax3/ftlog.py

""" Fault-tolerant log for a Upax node. """

import os
import re
# import sys
from collections import Container, Sized
from xlattice import (HashTypes, check_hashtype,     # u,
                      SHA1_HEX_NONE, SHA2_HEX_NONE, SHA3_HEX_NONE,
                      BLAKE2B_HEX_NONE)
from upax import UpaxError
from upax.node import check_hex_node_id_160, check_hex_node_id_256

__all__ = ['ATEXT', 'AT_FREE',
           'PATH_RE',
           'BODY_LINE_1_RE', 'BODY_LINE_256_RE',
           'IGNORABLE_RE',

           # classes
           'Log', 'BoundLog', 'LogEntry',
           'Reader', 'FileReader', 'StringReader', ]

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

    def __init__(self, reader, hashtype):
        self._hashtype = hashtype
        (timestamp, prev_log_hash, prev_master, entries, index) = reader.read()
        self._timestamp = timestamp     # seconds from epoch
        self._prev_hash = prev_log_hash   # SHA1/3 hash of previous Log
        if hashtype == HashTypes.SHA1:
            check_hex_node_id_160(self._prev_hash)
        else:
            check_hex_node_id_256(self._prev_hash)
        self._prev_master = prev_master    # nodeID of master writing prev log
        if hashtype == HashTypes.SHA1:
            check_hex_node_id_160(self._prev_master)
        else:
            check_hex_node_id_256(self._prev_master)

        self._entries = entries       # a list
        self._index = index         # a map, hash => entry

    def __contains__(self, key):
        """ Return whether this key is in the Log. """
        return key in self._index

    def __len__(self):
        """ Return the length of this Log. """
        return len(self._entries)

    def __str__(self):
        """used for serialization, so includes newline"""

        # first line
        if self._hashtype == HashTypes.SHA1:
            fmt = "%013u %40s %40s\n"
        else:
            fmt = "%013u %64s %64s\n"
        ret = fmt % (self._timestamp, self._prev_hash, self._prev_master)

        # list of entries
        for entry in self._entries:
            ret += str(entry)           # woefully inefficient :-)
        return ret

    def add_entry(self, tstamp, key, node_id, src, path):
        """
        Create a LogEntry with the given timestamp, key, nodeID, src, and path.

        If the LogEntry is already present in the Log, return a reference to
        the existing LogEntry.  Otherwise, add the LogEntry to the list and
        index it by key.
        """
        entry = LogEntry(tstamp, key, node_id, src, path)
        if key in self._index:
            existing = self._index[key]
            if entry == existing:
                return existing         # silently ignore duplicates
        self._entries.append(entry)     # increases size of list
        self._index[key] = entry        # overwrites any earlier duplicates
        return entry

    def get_entry(self, key):
        """ Given a key, return the corresponding LogEntry or None. """
        if key not in self._index:
            return None
        return self._index[key]

    @property
    def entries(self):
        """ Return the list of LogEntries. """
        return self._entries

    @property
    def index(self):
        """ Return the index by key into the list of LogEntries. """
        return self._index

    @property
    def prev_hash(self):
        """ Return the content hash of the previous Log. """
        return self._prev_hash

    @property
    def prev_master(self):
        """
        Return the ID of the master of the previous Log.
        """
        return self._prev_master

    @property
    def timestamp(self):
        """ Return the timestamp for this Log. """
        return self._timestamp


class BoundLog(Log):
    """ A fult tolerant log bound to a file. """

    def __init__(self, reader, hashtype=HashTypes.SHA2,
                 u_path=None, base_name='L'):
        super(). __init__(reader, hashtype)
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
                raise UpaxError(msg)
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
            raise UpaxError(msg)

        # XXX NEED TO THINK ABOUT THE ORDER OF OPERATIONS HERE
        entry = super(
            BoundLog,
            self).add_entry(tstamp, key, node_id, src, path)
        stringified = str(entry)
        self.fd_.write(stringified)
        return entry

    def flush(self):
        """
        Flush the log.

        This should write the contents of any internal buffers to disk,
        but no particular behavior is guaranteed.
        """
        self.fd_.flush()

    def close(self):
        """ Close the log. """
        self.fd_.close()
        self.is_open = False


# -------------------------------------------------------------------
class LogEntry():
    """
    The entry made upon adding a file to the Upax content-keyed data store.

    This consists of a timestamp; an SHA content key, the hash of the
    contents of the file, the NodeID identifying the contributor,
    its source (which may be a program name, and a UNIX/POSIX path
    associated with the file.  The path will normally be relative.
    """
    __slots__ = ['_timestamp', '_key', '_node_id', '_src', '_path', ]

    def __init__(self,
                 timestamp, key, node_id, source, pathToDoc):
        self._timestamp = timestamp      # seconds from epoch

        if key is None:
            raise UpaxError('LogEntry key may not be None')
        hashtype = len(key) == 40
        self._key = key              # 40 or 64 hex digits, content hash
        if hashtype == HashTypes.SHA1:
            check_hex_node_id_160(self._key)
        else:
            check_hex_node_id_256(self._key)

        if node_id is None:
            raise UpaxError('LogEntry nodeID may not be None')
        self._node_id = node_id           # 40/64 digits, node providing entry
        # XXX This is questionable.  Why can't a node with a SHA1 id store
        # a datum with a SHA3 key?
        if hashtype == HashTypes.SHA1:
            check_hex_node_id_160(self._node_id)
        else:
            check_hex_node_id_256(self._node_id)

        self._src = source           # tool or person responsible
        self._path = pathToDoc        # file name

    @property
    def key(self):
        """
        Return the 40- or 64-byte SHA hash associated with the entry.

        This is an SHA content hash.
        """
        return self._key

    @property
    def node_id(self):
        """ Return the 40- or 64-byte NodeID associated with the entry. """
        return self._node_id

    @property
    def path(self):
        """ Return the POSIX path associated with the LogEntry. """
        return self._path

    @property
    def src(self):
        """ Return the 'src' associated with the LogEntry. """
        return self._src

    @property
    def timestamp(self):
        """ Return the time at which the LogEntry was created. """
        return self._timestamp

    @property
    def hashtype(self):
        """ XXX WRONG should return key length, allowing 64 or 40. """
        return len(self._key) == 40

    # used in serialization, so newlines are intended
    def __str__(self):
        if self.hashtype == HashTypes.SHA1:
            fmt = '%013u %40s %40s "%s" %s\n'
        else:
            fmt = '%013u %64s %64s "%s" %s\n'
        return fmt % (self._timestamp, self._key,
                      self._node_id, self._src, self._path)

    def __eq__(self, other):
        return isinstance(other, LogEntry) and\
            self._timestamp == other.timestamp and\
            self._key == other.key and\
            self._node_id == other.node_id and\
            self._src == other.src and\
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


class Reader(object):
    """
    Would prefer to be able to handle this through something like a Java
    Reader, so that we could test with a StringReader but then use a
    FileReader in production.  If it is a file, file.readlines(sizeHint)
    supposedly has very good preformance for larger sizeHint, say 100KB
    It appears that lines returned need to be rstripped, which wastefully
    requires copying

    For our purposes, string input can just be split on newlines, which
    has the benefit of effectively chomping at the same time
    """

    # __slots__ = ['_entries', '_index', '_lines', '_hashtype',
    #             'FIRST_LINE_RE', ]

    def __init__(self, lines, hashtype):
        check_hashtype(hashtype)
        self._hashtype = hashtype
        if hashtype == HashTypes.SHA1:
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
    def hashtype(self):
        """ Return the type of SHA hash used. """
        return self._hashtype

    def read(self):
        """
        The first line contains timestamp, hash, nodeID for previous Log.
        Succeeding lines look like
           timestamp hash nodeID src path
        In both cases timestamp is an unsigned int, the number of
        milliseconds since the epoch.  It can be printed with %13u.
        The current value (April 2011) is about 1.3 trillion (1301961973000).
        """

        first_line = None
        if self._lines:
            first_line = self._lines[0]
        if first_line:
            match = re.match(self.first_line_re, first_line)
            if not match:
                print("NO MATCH, FIRST LINE; hashtype = %s" % self.hashtype)
                print(("  FIRST LINE: '%s'" % first_line))
                raise UpaxError("no match on first line; giving up")
            timestamp = int(match.group(1))
            prev_log_hash = match.group(2)
            prev_master = match.group(3)
            del self._lines[0]        # so we can cleanly iterate
        else:
            # no first line
            timestamp = 0
            if self._hashtype == HashTypes.SHA1:
                prev_log_hash = SHA1_HEX_NONE
                prev_master = SHA1_HEX_NONE
            elif self._hashtype == HashTypes.SHA2:
                prev_log_hash = SHA2_HEX_NONE
                prev_master = SHA2_HEX_NONE
            elif self._hashtype == HashTypes.SHA3:
                prev_log_hash = SHA3_HEX_NONE
                prev_master = SHA3_HEX_NONE
            elif self._hashtype == HashTypes.BLAKE2B:
                prev_log_hash = BLAKE2B_HEX_NONE
                prev_master = BLAKE2B_HEX_NONE
            else:
                raise NotImplementedError

        entries = []
        index = dict()

        for line in self._lines:
            # Read each successive line, creating an entry for each and
            # indexing each.  Ignore blank lines and those beginning with
            # a hash ('#')
            match = re.match(IGNORABLE_RE, line)
            if match:
                continue
            if self._hashtype == HashTypes.SHA1:
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
                raise UpaxError(msg)

        return (timestamp, prev_log_hash, prev_master, entries, index)

# -------------------------------------------------------------------


class FileReader(Reader):
    """
    Accept uPath and optionally log file name, read entire file into
    a string array, pass to Reader.
    """
    __slots__ = ['_u_path', '_base_name', '_log_file', ]

    # XXX CHECK ORDER OF ARGUMENTS
    def __init__(self, u_path, hashtype=False, base_name="L"):
        if not os.path.exists(u_path):
            raise UpaxError("no such directory %s" % u_path)
        self._u_path = u_path
        self._base_name = base_name
        self._log_file = "%s/%s" % (self._u_path, base_name)
        with open(self._log_file, 'r') as file:
            contents = file.read()
        lines = contents.split('\n')
        super(FileReader, self).__init__(lines, hashtype)

    @property
    def base_name(self):
        """ Return the base name of the log file. """
        return self._base_name

    @property
    def log_file(self):
        """ Return the path to the log file. """
        return self._log_file

    @property
    def u_path(self):
        """ Return the path to uDir, the content-keyed store. """
        return self._u_path

# -------------------------------------------------------------------


class StringReader(Reader):
    """
    Accept a (big) string, convert to a string array, pass to Reader
    """

    def __init__(self, bigString, hashtype=False):

        # split on newlines
        lines = bigString.split('\n')

        super().__init__(lines, hashtype)
