#!/usr/bin/python3

# ~/dev/py/upax_py/setup.py

import re
from distutils.core import setup
__version__ = re.search(r"__version__\s*=\s*'(.*)'",
                        open('upax/__init__.py').read()).group(1)

# see http://docs.python.org/distutils/setupscript.html

setup(name='upax_py',
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
      scripts=['check_u_consistency', 'import_u_dir',
               'upax_bulk_poster', 'upax_update_node_id', ],
      description='full-mesh ring of U store servers',
      url='https://jddixon/github.io/upax_py',
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Natural Language :: English',
          'Programming Language :: Python 3',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],)
