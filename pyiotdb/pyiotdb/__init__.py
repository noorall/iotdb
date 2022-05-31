"""
Package private common utilities. Do not use directly.
"""

from .Connection import Connection as connect
from .Exceptions import Error

__all__ = [
    connect,
    Error
]
