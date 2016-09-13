# ~/dev/py/upax/upax/node.py

import re
from xlattice import Q

# Classes in this module should inherit from XLattice classes, but
# currently the XLattice classes retain less desirable characteristics
# of the Java implementation such as the use of text strings to store
# byte arrays.  So get it right here and then backport to XLattice


def checkNodeID(b, usingSHA):
    if b is None:
        raise ValueError('nodeID may not be None')
    bLen = len(b)
    if (usingSHA == Q.USING_SHA1 and bLen != 20) or \
            (usingSHA != Q.USING_SHA1 and bLen != 32):
        raise ValueError('invalid nodeID length %u' % bLen)

NODE_ID_1_PAT = '^[A-Z0-9]{40}$'
NODE_ID_1_RE = re.compile(NODE_ID_1_PAT, re.I)
NODE_ID_2_PAT = '^[A-Z0-9]{64}$'
NODE_ID_2_RE = re.compile(NODE_ID_2_PAT, re.I)


def checkHexNodeID160(s):
    if s is None:
        raise ValueError('nodeID may not be None')
    m = NODE_ID_1_RE.match(s)
    if m is None:
        raise ValueError("not a valid 160-bit hex nodeID: ''%s'" % s)


def checkHexNodeID256(s):
    if s is None:
        raise ValueError('nodeID may not be None')
    m = NODE_ID_2_RE.match(s)
    if m is None:
        raise ValueError("not a valid 256-bit hex nodeID: ''%s'" % s)


class Peer(object):

    def __init__(self, nodeID, rsaPubKey, usingSHA=Q.USING_SHA2):
        self._usingSHA = usingSHA
        checkNodeID(nodeID, usingSHA)
        self._nodeID = nodeID    # fBytes20 or fBytes32
        # validate ?
        self._rsaPubKey = rsaPubKey
        self._nodeNdx = None      # will be uInt32

        self._cnx = []        # list of open connections
        self._ipAddr = []        # list of ipV4 addresses (uInt32)
        self._fqdn = []        # list of fully qualified domain names

    @property
    def nodeID(self): return self._nodeID

    @property
    def rsaPubKey(self): return self._rsaPubKey

    @property
    def nodeNdx(self): return self._nodeNdx

    @nodeNdx.setter
    def nodeNdx(self, value):
        if self._nodeNdx is not None:
            raise RuntimeError('nodeID may only be set once')
        if not isinstance(value, int):
            raise ValueError('nodeID must be an integer')
        if value < 0:
            raise ValueError('nodeID cannot be a negative number')
        self._nodeNdx = value

    @property
    def usingSHA(self): return self._usingSHA

    # These all return a reference to a list.  This is quite insecure
    # and nothing prevents users from adding arbitrary trash to a list.
    # A better API would expose get/set/del methods, with validation
    # on the getters.
    @property
    def cnx(self): return self._cnx

    @property
    def ipAddr(self): return self._ipAddr

    @property
    def fqdn(self): return self._fqdn


class UpaxNode(Peer):

    def __init__(self, nodeID=None, rsaPrivkey=None):

        if rsaPrivkey is None:
            # XXX STUB XXX generate one
            pass
        self._rsaPrivkey = rsaPrivkey
        # XXX STUB: extract the private key from the public key
        super(UpaxNode, self).__init__(nodeID, rsaPubKey)

        self._peers = {}    # nodeNdx (uInt32)    to Peer object
        self._lHashes = {}    # nodeNdx + timestamp to LHash object
        self._lMap = {}    # L hash + timestamp  to text (bytearray)


class LHash(object):

    def __init__(self, nodeNdx, t, hash):
        self._nodeNdx = nodeNdx
        self._timestamp = t
        # we assume the caller guarantees that the hash is correct and
        # refers to something stored somewhere
        self._hash = hash
