# upax/__init__.py

import binascii
import re
import time
import os
try:
    from os.scandir import scandir
except:
    from scandir import scandir

import rnglib
from xlattice import QQQ, check_using_sha
from xlattice.u import (file_sha1hex, file_sha2hex, file_sha3hex,
                        UDir)
from upax.ftlog import BoundLog, FileReader, Reader

__version__ = '0.8.5'
__version_date__ = '2016-11-26'

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

    def __init__(self, src_dir, dest_dir, pgm_name_and_version,
                 using_sha=QQQ.USING_SHA2, verbose=False):
        self._src_dir = src_dir
        self._dest_dir = dest_dir
        self._pgm_name_and_version = pgm_name_and_version
        self._server = None
        self._using_sha = using_sha
        self._verbose = verbose
        self._count = 0

    @property
    def src_dir(self):
        """
        Return the path to the source directory from which files are
        being loaded.
        """
        return self._src_dir

    def dest_dir(self):
        """
        Return the path to the destination directory into which files
        will be copied.
        """
        return self._dest_dir

    def pgm_name_and_version(self):
        """ Return the name of the program loading the data. """
        return self._pgm_name_and_version

    def verbose(self):
        """ Return whether to be chatty. """
        return self._verbose

    @staticmethod
    def create_importer(args):
        """ Create an Importer given a set of command line options. """
        return Importer(args.src_dir, args.dest_dir,
                        args.pgm_name_and_version, args.using_sha,
                        args.verbose)

    def import_bottom_dir(self, bottom_dir):
        """
        Import the files in the bottom directory of a content-keyed store.
        """
        src = self._pgm_name_and_version
        string = self._server

        count = 0
        for entry in scandir(bottom_dir):
            ok_ = False
            if entry.is_file():
                ok_ = True
                name = entry.name
                # leaf name names should be the file's SHA hash, its content
                # key
                if self._using_sha == QQQ.USING_SHA1:
                    match = FILE_NAME_1_RE.match(name)
                else:
                    match = FILE_NAME_2_RE.match(name)
                if match is not None:
                    count += 1
                    if self._verbose:
                        print('      ' + entry.path)
                    (_, actual_hash) = string.put(entry.path, name, src)
                    if actual_hash != name:
                        print(
                            "%s has content key %s" %
                            (entry.path, actual_hash))
                else:
                    ok_ = False
            if not ok_:
                print("not a proper leaf file: " + entry.path)
        self._count += count

    def import_sub_dir(self, sub_dir):
        """ Import the files in a subdirectory of a content-keyed store. """
        for entry in scandir(sub_dir):
            ok_ = False
            if entry.is_dir():
                ok_ = True
                if DIR_NAME_RE.match(entry.name):
                    if self._verbose:
                        print(('    ' + entry.path))
                    self.import_bottom_dir(entry.path)
            if not ok_:
                print(("not a proper subsubdirectory: " + entry.path))

    def do_import_u_dir(self):
        """
        Importation files in the source directory, which is a content-keyed
        store.
        """
        src_dir = self._src_dir
        dest_dir = self._dest_dir
        verbose = self._verbose
        # os.umask(0o222)       # CAN'T USE THIS

        self._server = BlockingServer(dest_dir, self._using_sha)
        log = self._server.log
        if verbose:
            print(("there were %7u files in %s at the beginning of the run" % (
                len(log), src_dir)))
        self._count = 0
        src_dir = self._src_dir
        if self._verbose:
            print(src_dir)
        try:
            for entry in scandir(src_dir):
                sub_dir = entry.name
                if sub_dir == 'L' or sub_dir == 'in' or \
                        sub_dir == 'node_id' or sub_dir == 'tmp':
                    continue
                ok_ = False
                if entry.is_dir():
                    if DIR_NAME_RE.match(sub_dir):
                        if self._verbose:
                            print(('  ' + entry.name))
                        ok_ = True
                        self.import_sub_dir(entry.path)

                if not ok_:
                    print(("not a proper subdirectory: " + entry.name))
        finally:
            self._server.close()                                       # GEEP


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

    def __init__(self, u_path, using_sha=QQQ.USING_SHA2):

        check_using_sha(using_sha)
        _in_dir_path = os.path.join(u_path, 'in')
        _log_file_path = os.path.join(u_path, 'L')
        _id_file_path = os.path.join(u_path, 'node_id')
        _tmp_dir_path = os.path.join(u_path, 'tmp')

        self._using_sha = using_sha
        u_dir = UDir.discover(u_path, UDir.DIR256x256, using_sha)

        self._u_dir = u_dir
        self._u_path = u_path

        if not os.path.exists(_in_dir_path):
            os.mkdir(_in_dir_path)
        if not os.path.exists(_tmp_dir_path):
            os.mkdir(_tmp_dir_path)
        if not os.path.exists(_id_file_path):
            if self._using_sha == QQQ.USING_SHA1:
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
            self._log = BoundLog(FileReader(u_path, self._using_sha),
                                 self._using_sha, u_path)
        else:
            self._log = BoundLog(Reader([], self._using_sha),
                                 self._using_sha, u_path)

    @property
    def u_dir(self):
        """ Return the UDir object describing the content-keyed store. """
        return self._u_dir

    @property
    def u_path(self):
        """ Return the path to the content-keyed store. """
        return self._u_path

    @property
    def using_sha(self):
        """ Return the type of SHA hash used. """
        return self._using_sha

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

        #----------------------------------------------------
        # XXX THIS IS A HACK but perhaps suggests a way to go
        #----------------------------------------------------
        if logged_path is None:
            logged_path = 'z@' + path_to_file

        if self._using_sha == QQQ.USING_SHA1:
            actual_key = file_sha1hex(path_to_file)
        elif self._using_sha == QQQ.USING_SHA2:
            actual_key = file_sha2hex(path_to_file)
        elif self._using_sha == QQQ.USING_SHA3:
            actual_key = file_sha3hex(path_to_file)
        if actual_key != key:
            raise ValueError('actual hash %s, claimed hash %s' % (
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

    def __init__(self, u_dir, using_sha=QQQ.USING_SHA2):
        super().__init__(u_dir, using_sha)


class NonBlockingServer(Server):
    """ Multi-threaded or otherwise non-blocking Upax server. """

    def __init__(self, u_dir, using_sha=QQQ.USING_SHA2):
        super().__init__(u_dir, using_sha)
