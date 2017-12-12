# upax/__init__.py

__version__ = '0.9.8'
__version_date__ = '2017-12-11'

__all__ = ['__version__', '__version_date__', 'UpaxError', ]


class UpaxError(RuntimeError):
    """ General purpose exception for the package. """
