# upax/__init__.py

import re
try:
    from os.scandir import scandir
except:
    from scandir import scandir

from xlattice import HashTypes
from upax.server import BlockingServer

__all__ = ['Importer', ]

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
                 hashtype=HashTypes.SHA2, verbose=False):
        self._src_dir = src_dir
        self._dest_dir = dest_dir
        self._pgm_name_and_version = pgm_name_and_version
        self._server = None
        self._hashtype = hashtype
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
                        args.pgm_name_and_version, args.hashtype,
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
                if self._hashtype == HashTypes.SHA1:
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

        self._server = BlockingServer(dest_dir, self._hashtype)
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
