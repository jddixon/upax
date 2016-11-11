# ~/dev/py/upax/upax/node.py

"""
Classes in this module should inherit from XLattice classes, but
currently the XLattice classes retain less desirable characteristics
of the Java implementation such as the use of text strings to store
byte arrays.  So get it right here and then backport to XLattice.
"""

import re

from Crypto.PublicKey import RSA            # new 2016-11-10
from xlattice import QQQ

NODE_ID_1_PAT = '^[A-Z0-9]{40}$'
NODE_ID_1_RE = re.compile(NODE_ID_1_PAT, re.I)
NODE_ID_2_PAT = '^[A-Z0-9]{64}$'
NODE_ID_2_RE = re.compile(NODE_ID_2_PAT, re.I)


def check_node_id(b_val, using_sha):
    """ Verify that the nodeID is compatible with the SHA hash type. """
    if b_val is None:
        raise ValueError('nodeID may not be None')
    b_len = len(b_val)
    if (using_sha == QQQ.USING_SHA1 and b_len != 20) or\
            (using_sha != QQQ.USING_SHA1 and b_len != 32):
        raise ValueError('invalid nodeID length %u' % b_len)


def check_hex_node_id_160(string):
    """ Verify that the hex nodeID is appropriate for SHA1. """
    if string is None:
        raise ValueError('nodeID may not be None')
    match = NODE_ID_1_RE.match(string)
    if match is None:
        raise ValueError("not a valid 160-bit hex nodeID: ''%s'" % string)


def check_hex_node_id_256(string):
    """ Verify that the hex nodeID is appropriate for SHA2 (256 bits). """
    if string is None:
        raise ValueError('nodeID may not be None')
    match = NODE_ID_2_RE.match(string)
    if match is None:
        raise ValueError("not a valid 256-bit hex nodeID: ''%s'" % string)


class Peer(object):
    """
    Specifications for an XLattice Peer, a Node with which we communicate.
    """

    def __init__(self, node_id, rsa_pub_key, using_sha=QQQ.USING_SHA2):
        self._using_sha = using_sha
        check_node_id(node_id, using_sha)
        self._node_id = node_id     # fBytes20 or fBytes32
        # validate ?
        self._rsa_pub_key = rsa_pub_key
        self._node_ndx = None       # will be uInt32

        self._cnx = []              # list of open connections
        self._ip_addr = []          # list of ipV4 addresses (uInt32)
        self._fqdn = []             # list of fully qualified domain names

    @property
    def node_id(self):
        return self._node_id

    @property
    def rsa_pub_key(self):
        return self._rsa_pub_key

    @property
    def node_ndx(self):
        return self._node_ndx

    @node_ndx.setter
    def node_ndx(self, value):
        if self._node_ndx is not None:
            raise RuntimeError('nodeID may only be set once')
        if not isinstance(value, int):
            raise ValueError('nodeID must be an integer')
        if value < 0:
            raise ValueError('nodeID cannot be a negative number')
        self._node_ndx = value

    @property
    def using_sha(self):
        return self._using_sha

    @property
    def cnx(self):
        """
        Return a reference to a list.

        This is quite insecure and nothing prevents users from adding arbitrary
        trash to a list. A better API would expose get/set/del methods,
        with validation on the getters.
        """
        return self._cnx

    @property
    def ip_addr(self):
        """ Returns a reference to a list; warnings as above. """
        return self._ip_addr

    @property
    def fqdn(self):
        """ Returns a reference to a list; warnings as above. """
        return self._fqdn


class UpaxNode(Peer):

    def __init__(self, node_id=None, rsa_priv_key=None):

        if rsa_priv_key is None:
            rsa_priv_key = RSA.generate(2048)       # XXX key fixed at 2K bits
        self._rsa_priv_key = rsa_priv_key
        # extract the public key from the private key
        rsa_pub_key = rsa_priv_key.publickey()
        super(UpaxNode, self).__init__(node_id, rsa_pub_key)

        self._peers = {}    # nodeNdx (uInt32)    => Peer object
        self._l_hashes = {}  # nodeNdx + timestamp => LHash object
        self._l_map = {}    # L hash + timestamp  => text (bytearray)


class LHash(object):
    """ Presumably the hash of an object stored on the node. """

    def __init__(self, node_ndx, tstamp, hash_):
        """
        We assume the caller guarantees that the hash is correct and
        refers to something stored somewhere.
        """
        self._node_ndx = node_ndx
        self._timestamp = tstamp
        self._hash = hash_

    @property
    def node_ndx(self):
        return self._node_ndx

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def l_hash(self):
        return self._hash
