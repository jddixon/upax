# upax/__init__.py

__version__ = '0.9.7'
__version_date__ = '2017-09-28'

__all__ = ['__version__', '__version_date__', 'UpaxError', ]


class UpaxError(RuntimeError):
    """ General purpose exception for the package. """
