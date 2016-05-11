# ~/dev/py/upax/upax/walker.py

# TAKE CARE: this was just hacked from cljutil.walker

import os
import re
import u
from upax import *
from upax.ftlog import *

HEX_DIR_PAT = '^[0-9a-fA-F]{2}$'
HEX_DIR_RE = re.compile(HEX_DIR_PAT)
__all__ = ['UWalker', ]

TWO_HEX_RE = re.compile('[0-9a-f]{2}')


class UWalker(object):

    def __init__(self, uDir='/var/U', limit=64, startAt='00',
                 justKeys=False, usingSHA1=False, verbose=False):
        if not os.path.exists(uDir):
            raise ValueError("directory '%s' does not exist" % str(uDir))
        self._uDir = uDir
        self._count = 0
        if limit > 0:
            self._limit = limit
        else:
            self._limit = 64                # default
        # we constrain startAt to be two lowercase hex digits
        startAt = startAt.lower()
        if not TWO_HEX_RE.match(startAt):
            print(("startAt = '%s' is not valid hex" % startAt))
            sys.exit(-1)
        self._justKeys = justKeys
        self._startAt = startAt
        self._usingSHA1 = usingSHA1
        self._verbose = verbose

        self._keys = []

    @property
    def count(self): return self._count

    @property
    def keys(self): return self._keys

    @property
    def limit(self): return self._limit

    @property
    def uDir(self): return self._uDir

    def walk(self):
        """
        Returns a list of keys found.
        """
        limit = self._limit
        verbose = self._verbose

        self._count = 0
        topDirs = sorted(os.listdir(self._uDir))
        walking = True
        for top in topDirs:
            if HEX_DIR_RE.match(top):
                if top < self._startAt:
                    continue
                topDirPath = os.path.join(self._uDir, top)
                midDirs = sorted(os.listdir(topDirPath))
                for mid in midDirs:
                    if HEX_DIR_RE.match(mid):
                        midDirPath = os.path.join(topDirPath, mid)
                        files = sorted(os.listdir(midDirPath))
                        for file in files:
                            self._count += 1
#                           if verbose:
#                               print "%03u %s" % (self._count, file)
                            if self._count >= limit:
                                walking = False
                            self._keys.append(file)
                            if self._justKeys:
                                if not walking:
                                    break
                                continue

                            pathToFile = os.path.join(midDirPath, file)
                            if self._usingSHA1:
                                contentKey = u.fileSHA1(pathToFile)
                            else:
                                contentKey = u.fileSHA3(pathToFile)

                            if file != contentKey:
                                print(('*** HASH MISMATCH: expected %s, actual %s ***'
                                       % (file, contentKey)))
                            if not walking:
                                break
                    if not walking:
                        break
            if not walking:
                break
        return self._keys
