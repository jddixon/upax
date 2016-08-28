# upax_py

This is a Python 3 implementation of the
[Upax](https://jddixon.github.io/xlattice/upax.html)
distributed file system.

## Major System Components

A Upax implementation consists of at least

* a [fault-tolerant log](https://jddixon.github.io/xlattice/ftLog.html)
* a [Upax server](https://jddixon.github.io/xlattice/upaxServer.html)
* a [Upax client](https://jddixon.github.io/xlattice/upaxClient.html) and optionally
* a [Upax mirror](https://jddixon.github.io/xlattice/upaxMirror.html)


Servers involved use a
[consensus algorithm](https://en.wikipedia.org/wiki/Consensus_%28computer_science%29)
to ensure that their pictures of the state of the distributed file system
are consistent.

## Utilities

These include

* importUDir
* upaxBulkPoster
* upaxUpdateNodeID

### importUDir

This utility imports all of the files in a content-keyed U directory into
the Upax distribued file system.

    usage: importUDir [-h] [-1] [-d DESTDIR] [-j] [-s SRCDIR] [-t] [-T] [-V] [-v]

    optional arguments:
      -h, --help            show this help message and exit
      -1, --usingSHA1       use SHA1, and so 160-bit keys
      -d DESTDIR, --destDir DESTDIR
                            base name of destination U/ directory
      -j, --justShow        show args and exit
      -s SRCDIR, --srcDir SRCDIR
                            base name of source U/ directory
      -t, --showTimestamp   show run timestamp
      -T, --testing         test run - write to ./testU
      -V, --showVersion     show version number and date
      -v, --verbose         talk a lot

### upaxBulkPoster

Given a flat directory, imports any constituent files into the Upax
distributd file system.

    usage: post new files in a directory into Upax [-h] [-1] [-e] [-i INDIR] [-H]
                                                   [-j] [-N] [-t] [-u UDIR] [-T]
                                                   [-V] [-v] [-z]

    optional arguments:
      -h, --help            show this help message and exit
      -1, --usingSHA1       use SHA1, and so 160-bit keys
      -e, --ec2Host         set if machine is in EC2
      -i INDIR, --inDir INDIR
                            path to input directory (forced to ./ testIn if
                            testing)
      -H, --hostmaster      set if machine runs bindMgr
      -j, --justShow        show args and exit
      -N, --nameserver      set if machine is a name server and so runs
                            bindLocalMgr
      -t, --showTimestamp   show run timestamp
      -u UDIR, --uDir UDIR  base name of U/ directory, default to Upax
      -T, --testing         test run - write to ./testU
      -V, --showVersion     show version number and date
      -v, --verbose         talk a lot
      -z, --noChanges       don't actually write anything to disk

### upaxUpdateNodeID

The content-keyed store on a Upax system needs a unique ID, its **nodeID**.
This utility writes a randomly chosen 20- or 32-byte value into a file
called *nodeID** at the top level of uDir, so at `uDir/nodeID`.  This is
written as a 40- or 64-character hexadecimal value.

    usage: post new files in a directory into Upax [-h] [-1] [-e] [-i INDIR] [-H]
                                                   [-j] [-N] [-t] [-u UDIR] [-T]
                                                   [-V] [-v] [-z]

    optional arguments:
      -h, --help            show this help message and exit
      -1, --usingSHA1       use SHA1, and so 160-bit keys
      -e, --ec2Host         set if machine is in EC2
      -i INDIR, --inDir INDIR
                            path to .ssh/ (forced to ./ testIn if testing)
      -H, --hostmaster      set if machine runs bindMgr
      -j, --justShow        show args and exit
      -N, --nameserver      set if machine is a name server and so runs
                            bindLocalMgr
      -t, --showTimestamp   show run timestamp
      -u UDIR, --uDir UDIR  base name of U/ directory, default to Upax
      -T, --testing         test run - write to ./testU
      -V, --showVersion     show version number and date
      -v, --verbose         talk a lot
      -z, --noChanges       don't actually write anything to disk

## Dependencies

*   **xlattice_py**, the collection of Python3 XLattice supporrt classes

## Project Status

Incompletely specified, partially implemented.

## On-line Documentation

More information on the **upax_py** project can be found
[here](https://jddixon.github.io/upax_py)
