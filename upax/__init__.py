# upax/__init__.py

__version__ = '0.9.2'
__version_date__ = '2017-01-28'

__all__ = ['__version__', '__version_date__', 'UpaxError', ]

## PATs AND REs ######################################################
#DIR_NAME_PAT = '^[0-9a-fA-F]{2}$'
#DIR_NAME_RE = re.compile(DIR_NAME_PAT)
#
#FILE_NAME_1_PAT = '^[0-9a-fA-F]{40}$'
#FILE_NAME_1_RE = re.compile(FILE_NAME_1_PAT)
#
#FILE_NAME_2_PAT = '^[0-9a-fA-F]{64}$'
#FILE_NAME_2_RE = re.compile(FILE_NAME_2_PAT)

# -- classes --------------------------------------------------------


class UpaxError(RuntimeError):
    """ General purpose exception for the package. """
