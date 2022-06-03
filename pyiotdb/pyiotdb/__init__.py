"""
Package private common utilities. Do not use directly.
"""

from .Connection import Connection as connect
from .Exceptions import Error

__all__ = [
    connect,
    Error
]
__version__ = "0.0.1"

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"