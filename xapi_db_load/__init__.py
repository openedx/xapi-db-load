"""
Scripts to generate fake xAPI data against various backends.
"""

from importlib.metadata import (
    PackageNotFoundError,
    version,
)

try:
    __version__ = version("xapi-db-load")
except PackageNotFoundError:
    __version__ = "unknown"
