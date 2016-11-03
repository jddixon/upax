#!/usr/bin/python

# ~/dev/py/upax/upax/consistency.py

import os
import random
import re
import sys
import time

import u

import upax          # MUST LOCK uDir
# from upax import *
from upax.ftlog import BoundLog, FileReader, LogEntry
from upax.walker import UWalker

__all__ = ['check', ]

# -- implementation -------------------------------------------------


def setup_server(options):
    options.uServer = upax.BlockingServer(options.u_dir, options.using_sha)


def shutdown_server(options):
    if options.uServer:
        options.uServer.close()


def walk_u(options):
    """
    Returns a list of content keys in the selected region of U,
    the region being defined by a two hex digit start point and
    a maximum number of entries to be included.
    """
    www = UWalker(just_keys=options.just_keys,
                  limit=options.limit,
                  start_at=options.start_at,
                  u_dir=options.u_dir,
                  using_sha=options.using_sha,
                  verbose=options.verbose)
    keys = www.walk()
    return keys


def check(options):
    """
    Examines U and its log (U/L), reports inconsistencies, and
    possibly takes action to correct them.

    If the --repair argument is present, will locate any content files
    in U that are not in the log and add them with the int form of the
    time of the run as the timestamp, U/nodeID as the nodeID, this program
    as the source, and the content key as the path.  By default the program
    assumes that we are using SHA1 to calculate content keys.

    In this implementation no attempt is made to verify that the
    content key accurately reflects what is in the file.

    Also, whereas every file in U is examined, no attempt is made to
    verify that every entry in U corresponds to a file in U.  The
    not very coherent idea is that log entries may describe files on
    other machines and so may used to retrieve them.  Presumably a
    future version of this program will have a map from valid nodeIDs
    to end points (fully qualified domain names and port numbers for
    peers), allowing utilities to fetch files from the source host
    by content key.

    """
    repairing = options.repairing
    verbose = options.verbose
    options.uServer = None
    try:
        setup_server(options)       # gets locks on U and U0
    except ValueError as vvv:
        if options.uServer is None:
            print("have you set usingSHA correctly?")
        else:
            raise vvv
    if options.uServer is not None:
        try:
            # LOG: keyed by hash, later entries with same hash should
            # overwrite earlier
            options.reader = FileReader(options.u_dir, options.using_sha)
            options.log = BoundLog(options.reader, options.using_sha)
            log = options.log

            # U: sorted content keys
            keys = walk_u(options)

            # for now, just check whether each content key has a log
            # entry
            for key in keys:
                if key in log.index:
                    if verbose:
                        log_e = log.index[key]
                        print(("%s in ndx, src '%s'" % (key, log_e.src)))
                else:
                    if repairing:
                        entry = log.add_entry(options.timestamp, key,
                                              options.myNodeID, options.app_name, key)
                        print(("ADDED TO LOG: %s" % entry))
                    else:
                        print(("%s is not in the log" % key))

            # DEBUG -------------------------------------------------
            if verbose:
                print(("COUNT OF ITEMS CHECKED IN U: %s" % len(keys)))
                print(("NUMBER OF LOG ENTRIES:         %s" % len(options.log)))
            # END ---------------------------------------------------
        finally:
            try:
                if options.log is not None:
                    options.log.close()
            except AttributeError:
                pass
            shutdown_server(options)     # releases lock on U
