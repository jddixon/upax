# ~/dev/py/upax/upax/walker.py

"""
Walks a content-keyed directory structure.

XXX Can't handle DIR_FLAT or DIR16x16.

TAKE CARE: this was just hacked from cljutil.walker.
"""

import os
import re
import sys

from xlattice import u, QQQ, check_using_sha

HEX_DIR_PAT = '^[0-9a-fA-F]{2}$'
HEX_DIR_RE = re.compile(HEX_DIR_PAT)
__all__ = ['UWalker', ]

TWO_HEX_RE = re.compile('[0-9a-f]{2}')


class UWalker(object):
    """
    Walks a content-keyed directory structure, visiting up to `limit` nodes.

    XXX Can't handle DIR_FLAT or DIR16x16.  Needs to be modified to
    handle the nine combinations of SHA{1,2,3} and DIR{_FLAT,16x16,256x256.
    """

    def __init__(self, u_path='/var/U', limit=64, start_at='00',
                 just_keys=False, using_sha=QQQ.USING_SHA2, verbose=False):
        if not os.path.exists(u_path):
            raise ValueError("directory '%s' does not exist" % str(u_path))

        check_using_sha(using_sha)
        self._u_path = u_path
        self._count = 0
        if limit > 0:
            self._limit = limit
        else:
            self._limit = 64                # default
        # we constrain startAt to be two lowercase hex digits
        start_at = start_at.lower()
        if not TWO_HEX_RE.match(start_at):
            print("startAt = '%s' is not valid hex" % start_at)
            sys.exit(-1)
        self._just_keys = just_keys
        self._start_at = start_at
        self._using_sha = using_sha
        self._verbose = verbose

        self._keys = []

    @property
    def count(self):
        """ Return a count of nodes found during the walk. """
        return self._count

    @property
    def keys(self):
        """ Return the list of keys found by the walker."""
        return self._keys

    @property
    def limit(self):
        """ Return the maximum number of keys to be found. """
        return self._limit

    @property
    def u_path(self):
        """ Return path to content-keyed store. """
        return self._u_path

    def walk(self):
        """
        Returns a list of keys found.
        """
        limit = self._limit

        self._count = 0
        top_dirs = sorted(os.listdir(self._u_path))
        walking = True
        for top in top_dirs:
            if HEX_DIR_RE.match(top):
                if top < self._start_at:
                    continue
                top_dir_path = os.path.join(self._u_path, top)
                mid_dirs = sorted(os.listdir(top_dir_path))
                for mid in mid_dirs:
                    if HEX_DIR_RE.match(mid):
                        mid_dir_path = os.path.join(top_dir_path, mid)
                        files = sorted(os.listdir(mid_dir_path))
                        for file in files:
                            self._count += 1
#                           if self._verbose:
#                               print "%03u %s" % (self._count, file)
                            if self._count >= limit:
                                walking = False
                            self._keys.append(file)
                            if self._just_keys:
                                if not walking:
                                    break
                                continue

                            path_to_file = os.path.join(mid_dir_path, file)
                            if self._using_sha == QQQ.USING_SHA1:
                                content_key = u.file_sha1hex(path_to_file)
                            elif self._using_sha == QQQ.USING_SHA2:
                                content_key = u.file_sha2hex(path_to_file)
                            elif self._using_sha == QQQ.USING_SHA3:
                                content_key = u.file_sha3hex(path_to_file)

                            if file != content_key:
                                print('HASH MISMATCH: expected %s, actual %s ***'
                                      % (file, content_key))
                            if not walking:
                                break
                    if not walking:
                        break
            if not walking:
                break
        return self._keys
