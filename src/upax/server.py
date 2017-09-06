# upax/__init__.py

import binascii
import time
import os
# try:
#    from os.scandir import scandir
# except:
#    from scandir import scandir

import rnglib
from xlattice import HashTypes, check_hashtype
from xlattice.u import (file_sha1hex, file_sha2hex, file_sha3hex,
                        DirStruc, UDir)
from upax.ftlog import BoundLog, FileReader, Reader

from upax import UpaxError

__all__ = ['Server', 'BlockingServer', 'NonBlockingServer', ]


# -- classes --------------------------------------------------------

class Server(object):
    """
    The Upax Server controlling access to a content-keyed store.

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

    def __init__(self, u_path, hashtype=HashTypes.SHA2):

        check_hashtype(hashtype)
        _in_dir_path = os.path.join(u_path, 'in')
        _log_file_path = os.path.join(u_path, 'L')
        _id_file_path = os.path.join(u_path, 'node_id')
        _tmp_dir_path = os.path.join(u_path, 'tmp')

        self._hashtype = hashtype
        u_dir = UDir.discover(u_path, DirStruc.DIR256x256, hashtype)

        self._u_dir = u_dir
        self._u_path = u_path

        if not os.path.exists(_in_dir_path):
            os.mkdir(_in_dir_path)
        if not os.path.exists(_tmp_dir_path):
            os.mkdir(_tmp_dir_path)
        if not os.path.exists(_id_file_path):
            if self._hashtype == HashTypes.SHA1:
                byte_id = bytearray(20)
            else:
                byte_id = bytearray(32)
            rng = rnglib.SimpleRNG(time.time())
            rng.next_bytes(byte_id)       # a low-quality quasi-random number
            id_ = str(binascii.b2a_hex(byte_id), 'utf-8')
            self._node_id = id_
            with open(_id_file_path, 'w') as file:
                file.write(id_ + '\n')
        else:
            # XXX many possible problems here!
            with open(_id_file_path, 'r') as file:
                self._node_id = file.read()[:-1]

        if os.path.exists(_log_file_path):
            self._log = BoundLog(FileReader(u_path, self._hashtype),
                                 self._hashtype, u_path)
        else:
            self._log = BoundLog(Reader([], self._hashtype),
                                 self._hashtype, u_path)

    @property
    def u_dir(self):
        """ Return the UDir object describing the content-keyed store. """
        return self._u_dir

    @property
    def u_path(self):
        """ Return the path to the content-keyed store. """
        return self._u_path

    @property
    def hashtype(self):
        """ Return the type of SHA hash used. """
        return self._hashtype

    @property
    def log(self):
        """ Return a reference to the Server's BoundLog."""

        return self._log

    @property
    def node_id(self):
        """ Return the Server's NodeID. """
        return self._node_id

    def exists(self, key):
        """ Return whehter uDir exists. """
        return self._u_dir.exists(key)

    def get(self, key):
        """
        Given a content key (SHA hash), return the contents of the
        corresponding file.
        """
        return self._u_dir.get_data(key)

    def put(self, path_to_file, key, source, logged_path=None):
        """
        returns (len, hash)
        """

        # ----------------------------------------------------
        # XXX THIS IS A HACK but perhaps suggests a way to go
        # ----------------------------------------------------
        if logged_path is None:
            logged_path = 'z@' + path_to_file

        if self._hashtype == HashTypes.SHA1:
            actual_key = file_sha1hex(path_to_file)
        elif self._hashtype == HashTypes.SHA2:
            actual_key = file_sha2hex(path_to_file)
        elif self._hashtype == HashTypes.SHA3:
            actual_key = file_sha3hex(path_to_file)
        if actual_key != key:
            raise UpaxError('actual hash %s, claimed hash %s' % (
                actual_key, key))

        if self._u_dir.exists(key):
            return (-1, key)

        # XXX uses tempfile package, so not secure XXX
        (len_, hash_) = self._u_dir.copy_and_put(path_to_file, key)

        # XXX should deal with exceptions
        self._log.add_entry(
            time.time(),
            key,
            self._node_id,
            source,
            logged_path)
        return (len_, hash_)

    def put_data(self, data, key, source, logged_path='z@__posted_data__'):
        """ returns (len_, hash_) """
        (len_, hash_) = self._u_dir.put_data(data, key)

        # XXX should deal with exceptions

        self._log.add_entry(
            time.time(),
            key,
            self._node_id,
            source,
            logged_path)
        return (len_, hash_)

    def close(self):
        """ Shut down the server, closing any open files. """
        self._log.close()


class BlockingServer(Server):
    """ Single-threaded Upax server. """

    def __init__(self, u_dir, hashtype=HashTypes.SHA2):
        super().__init__(u_dir, hashtype)


class NonBlockingServer(Server):
    """ Multi-threaded or otherwise non-blocking Upax server. """

    def __init__(self, u_dir, hashtype=HashTypes.SHA2):
        super().__init__(u_dir, hashtype)
