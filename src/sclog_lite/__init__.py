"""sclog_lite: A lightweight logging extension based on loguru.

Provides console, file, and MySQL database logging with connection pooling,
asynchronous batch writing, and failure isolation.
"""

from .logger import add_mysql_sink, logger, setup_logging

__version__ = "0.2.0"
__all__ = ["logger", "add_mysql_sink", "setup_logging"]

