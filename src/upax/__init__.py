# upax/__init__.py

__version__ = '0.10.2'
__version_date__ = '2018-02-28'

__all__ = ['__version__', '__version_date__', 'UpaxError', ]


class UpaxError(RuntimeError):
    """ General purpose exception for the package. """
