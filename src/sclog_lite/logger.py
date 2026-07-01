"""Logger Module for sclog_lite.

This module provides the primary user interface for logging. It wraps
loguru's native logger to ensure seamless compatibility with existing loguru
functionality while offering easy integration with our custom MySQL sink.
"""

import os
import sys
import types
from typing import Any, Callable, Dict, Optional, Tuple, Union

from loguru import logger

from .mysql_sink import MySQLSink

# Re-export loguru's logger directly so users have access to all native features
__all__ = ["logger", "add_mysql_sink", "setup_logging"]


def _get_project_root() -> str:
    """Finds the project root directory by searching for standard markers.

    If no marker is found, returns the current working directory.

    Returns:
        The absolute path to the project root directory.
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        current_dir = os.getcwd()

    while current_dir != os.path.dirname(current_dir):
        if any(os.path.exists(os.path.join(current_dir, marker)) for marker in ["pyproject.toml", ".git"]):
            return current_dir
        current_dir = os.path.dirname(current_dir)
    return os.getcwd()


def add_mysql_sink(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    table_name: str = "log_entries",
    auto_create_table: bool = True,
    batch_size: int = 100,
    batch_interval: float = 1.0,
    queue_maxsize: int = 10000,
    queue_block: bool = False,
    queue_timeout: Optional[float] = None,
    fallback_path: Optional[str] = None,
    custom_insert_sql: Optional[str] = None,
    custom_mapping_func: Optional[Callable[[Dict[str, Any]], Tuple[Any, ...]]] = None,
    pool_config: Optional[Dict[str, Any]] = None,
    level: str = "DEBUG",
    **kwargs: Any,
) -> int:
    """Adds a MySQL sink to the global loguru logger.

    Args:
        host: MySQL host.
        port: MySQL port.
        user: MySQL user.
        password: MySQL password.
        database: MySQL database name.
        table_name: Name of the log table. Defaults to "log_entries".
        auto_create_table: Whether to create the table if it does not exist.
            Defaults to True.
        batch_size: Maximum logs in a single SQL executemany batch.
            Defaults to 100.
        batch_interval: Seconds to wait before flushing. Defaults to 1.0.
        queue_maxsize: Max capacity of log queue. Defaults to 10000.
        queue_block: If True, blocks when queue is full. Defaults to False.
        queue_timeout: Seconds to wait if queue_block is True. Defaults to None.
        fallback_path: Log file path to dump logs if MySQL write fails.
            Defaults to None.
        custom_insert_sql: Custom INSERT SQL statement. Defaults to None.
        custom_mapping_func: Custom function to map a loguru record dict to
            a tuple for SQL execution. Defaults to None.
        pool_config: Optional configurations for the PooledDB instance.
        level: Minimum log level for the MySQL sink. Defaults to "DEBUG".
        **kwargs: Additional parameters passed to loguru's add() method (e.g. format, filter).

    Returns:
        The sink ID returned by loguru.add().
    """
    sink = MySQLSink(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        table_name=table_name,
        auto_create_table=auto_create_table,
        batch_size=batch_size,
        batch_interval=batch_interval,
        queue_maxsize=queue_maxsize,
        queue_block=queue_block,
        queue_timeout=queue_timeout,
        fallback_path=fallback_path,
        custom_insert_sql=custom_insert_sql,
        custom_mapping_func=custom_mapping_func,
        pool_config=pool_config,
    )

    # We pass the sink object directly to logger.add. Loguru will call
    # sink.write(message) for writing and automatically trigger sink.stop()
    # or sink.close() on termination or removal of this sink.
    return logger.add(
        sink,
        level=level,
        serialize=False,
        enqueue=False,
        **kwargs,
    )


def setup_logging(
    console: bool = True,
    console_level: str = "INFO",
    file_path: Union[str, bool, None] = None,
    file_level: str = "DEBUG",
    file_rotation: str = "10 MB",
    file_retention: str = "10 days",
    clear_existing: bool = True,
    **kwargs: Any,
) -> None:
    """Helper function to configure console and/or file logging easily.

    Args:
        console: Whether to enable console logging. Defaults to True.
        console_level: Minimum log level for the console. Defaults to "INFO".
        file_path: Optional file path or True/False.
            - If None (default): Automatically defaults to the "/logs/sclog.log"
              file path under the project root.
            - If a directory path (or ending with a slash): appends "/sclog.log"
              to the directory.
            - If a file path: writes logs to that specific file path.
            - If False or empty string "": disables file logging entirely.
        file_level: Minimum log level for file logging. Defaults to "DEBUG".
        file_rotation: Rotation condition for file logs. Defaults to "10 MB".
        file_retention: Retention condition for file logs. Defaults to "10 days".
        clear_existing: If True, clears existing sinks before adding new ones.
            Defaults to True.
        **kwargs: Additional parameters passed to loguru's add() method for
            both console and file logging.
    """
    if clear_existing:
        logger.remove()

    if console:
        logger.add(sys.stderr, level=console_level, **kwargs)

    resolved_file_path: Optional[str] = None
    if file_path is not False and file_path != "":
        if file_path is None or file_path is True:
            project_root = _get_project_root()
            resolved_file_path = os.path.join(project_root, "logs", "sclog.log")
        else:
            file_path_str = str(file_path)
            _, ext = os.path.splitext(file_path_str)
            if os.path.isdir(file_path_str) or file_path_str.endswith("/") or file_path_str.endswith("\\") or not ext:
                resolved_file_path = os.path.join(file_path_str, "sclog.log")
            else:
                resolved_file_path = file_path_str

    if resolved_file_path:
        log_dir = os.path.dirname(os.path.abspath(resolved_file_path))
        os.makedirs(log_dir, exist_ok=True)
        logger.add(
            resolved_file_path,
            level=file_level,
            rotation=file_rotation,
            retention=file_retention,
            **kwargs,
        )


# Monkey-patch loguru's logger to add add_mysql_sink and setup_logging
# as instance methods for extra user convenience.
def _add_mysql_sink_method(self: Any, *args: Any, **kwargs: Any) -> int:
    return add_mysql_sink(*args, **kwargs)


def _setup_logging_method(self: Any, *args: Any, **kwargs: Any) -> None:
    return setup_logging(*args, **kwargs)


logger.add_mysql_sink = types.MethodType(_add_mysql_sink_method, logger)  # type: ignore[attr-defined]
logger.setup_logging = types.MethodType(_setup_logging_method, logger)  # type: ignore[attr-defined]
