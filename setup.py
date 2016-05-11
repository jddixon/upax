#!/usr/bin/python3

# ~/dev/py/upax/setup.py

import re
from distutils.core import setup
__version__ = re.search("__version__\s*=\s*'(.*)'",
                        open('upax/__init__.py').read()).group(1)

# see http://docs.python.org/distutils/setupscript.html

setup(name='upax',
      version=__version__,
      author='Jim Dixon',
      author_email='jddixon@gmail.com',
      #
      # wherever we have a .py file that will be imported, we
      # list it here, without the .py extension but SQuoted
      py_modules=[],
      #
      # ftlog is not a package because it doesn't have its own
      # subdir and __init__.py
      packages=['upax', ],
      #
      # following could be in scripts/ subdir; SQuote
      scripts=['checkUConsistency', 'importUDir',
               'upaxBulkPoster', 'upaxUpdateNodeID', ],
      # MISSING url
      )
