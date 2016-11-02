# ~/dev/py/upax/upax/walker.py

# TAKE CARE: this was just hacked from cljutil.walker

import os
import re
import u
from upax import *
from upax.ftlog import *
from xlattice import QQQ, check_using_sha

HEX_DIR_PAT = '^[0-9a-fA-F]{2}$'
HEX_DIR_RE = re.compile(HEX_DIR_PAT)
__all__ = ['UWalker', ]

TWO_HEX_RE = re.compile('[0-9a-f]{2}')


class UWalker(object):

    def __init__(self, u_dir='/var/U', limit=64, start_at='00',
                 justKeys=False, using_sha=QQQ.USING_SHA2, verbose=False):
        if not os.path.exists(u_dir):
            raise ValueError("directory '%s' does not exist" % str(u_dir))

        check_using_sha(using_sha)
        self._u_dir = u_dir
        self._count = 0
        if limit > 0:
            self._limit = limit
        else:
            self._limit = 64                # default
        # we constrain startAt to be two lowercase hex digits
        start_at = start_at.lower()
        if not TWO_HEX_RE.match(start_at):
            print(("startAt = '%s' is not valid hex" % start_at))
            sys.exit(-1)
        self._just_keys = justKeys
        self._start_at = start_at
        self._using_sha = using_sha
        self._verbose = verbose

        self._keys = []

    @property
    def count(self): return self._count

    @property
    def keys(self): return self._keys

    @property
    def limit(self): return self._limit

    @property
    def u_dir(self): return self._u_dir

    def walk(self):
        """
        Returns a list of keys found.
        """
        limit = self._limit
        verbose = self._verbose

        self._count = 0
        top_dirs = sorted(os.listdir(self._u_dir))
        walking = True
        for top in top_dirs:
            if HEX_DIR_RE.match(top):
                if top < self._start_at:
                    continue
                top_dir_path = os.path.join(self._u_dir, top)
                mid_dirs = sorted(os.listdir(top_dir_path))
                for mid in mid_dirs:
                    if HEX_DIR_RE.match(mid):
                        mid_dir_path = os.path.join(top_dir_path, mid)
                        files = sorted(os.listdir(mid_dir_path))
                        for file in files:
                            self._count += 1
#                           if verbose:
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
                                content_key = u.fileSHA1(path_to_file)
                            elif self._using_sha == QQQ.USING_SHA2:
                                content_key = u.fileSHA2(path_to_file)
                            elif self._using_sha == QQQ.USING_SHA3:
                                content_key = u.fileSHA3(path_to_file)

                            if file != content_key:
                                print(('*** HASH MISMATCH: expected %s, actual %s ***'
                                       % (file, content_key)))
                            if not walking:
                                break
                    if not walking:
                        break
            if not walking:
                break
        return self._keys
