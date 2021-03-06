#!/usr/bin/python
# ~/dev/py/upax/upax/consistency.py

""" Funcions for verifying the internal consistency of a upax server. """

from upax import UpaxError
from upax.ftlog import BoundLog, FileReader     # , LogEntry
from upax.server import BlockingServer
from upax.walker import UWalker

__all__ = ['check', ]


def setup_server(options):
    """ Add server configuration info to the set of options. """
    options.uServer = BlockingServer(options.u_path, options.hashtype)


def shutdown_server(options):
    """ Shut down a upax server if it is running. """
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
                  u_path=options.u_path,
                  hashtype=options.hashtype,
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
    options.uServer = None
    try:
        setup_server(options)       # gets locks on U and U0
    except UpaxError:
        if options.uServer is None:
            print("have you set usingSHA correctly?")
        else:
            raise
    if options.uServer is not None:
        _do_server_shutdown(options)


def _do_server_shutdown(options):
    repairing = options.repairing
    verbose = options.verbose
    try:
        # LOG: keyed by hash, later entries with same hash should
        # overwrite earlier
        options.reader = FileReader(options.u_path, options.hashtype)
        options.log = BoundLog(options.reader, options.hashtype)
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
                                          options.myNodeID,
                                          options.app_name, key)
                    if verbose:
                        print(("ADDED TO LOG: %s" % entry))
                else:
                    if verbose:
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
