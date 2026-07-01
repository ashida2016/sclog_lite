"""PostgreSQL Sink Module for sclog_lite.

This module provides a custom sink for loguru that handles writing log records
to a PostgreSQL database asynchronously, in batches, using connection pooling
and with write failure isolation.
"""

import json
import queue
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from dbutils.pooled_db import PooledDB
import psycopg2


class PgSQLSink:
    """A custom loguru sink for writing logs to PostgreSQL in asynchronous batches.

    This class maintains a thread-safe queue where incoming log records are
    stored, and a background daemon thread that periodically flushes these
    records to a PostgreSQL database. It uses DBUtils PooledDB for connection pooling
    and psycopg2 as the underlying driver.

    Attributes:
        _host (str): PostgreSQL host.
        _port (int): PostgreSQL port.
        _user (str): PostgreSQL user.
        _password (str): PostgreSQL password.
        _database (str): PostgreSQL database name.
        _table_name (str): Name of the table where logs are written.
        _auto_create_table (bool): If True, automatically creates the table.
        _batch_size (int): Max number of logs to write in a single batch.
        _batch_interval (float): Max seconds to wait before flushing.
        _queue_maxsize (int): Max capacity of the thread-safe queue.
        _queue_block (bool): If True, blocks when the queue is full.
        _queue_timeout (Optional[float]): Timeout in seconds when blocking.
        _fallback_path (Optional[str]): File path to write logs if DB write fails.
        _custom_insert_sql (Optional[str]): Custom SQL statement for insert.
        _custom_mapping_func (Optional[Callable[[Dict[str, Any]], Tuple[Any, ...]]]): Custom mapping.
        _stop_event (threading.Event): Signals the worker thread to stop.
        _queue (queue.Queue): Thread-safe log queue.
        _pool (PooledDB): Connection pool.
        _worker_thread (threading.Thread): Background worker thread.
    """

    def __init__(
        self,
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
    ) -> None:
        """Initializes the PostgreSQL Sink.

        Args:
            host: PostgreSQL host.
            port: PostgreSQL port.
            user: PostgreSQL user.
            password: PostgreSQL password.
            database: PostgreSQL database name.
            table_name: Name of the log table. Defaults to "log_entries".
            auto_create_table: Whether to create the table if it does not exist.
                Defaults to True.
            batch_size: Maximum logs in a single SQL executemany batch.
                Defaults to 100.
            batch_interval: Seconds to wait before flushing. Defaults to 1.0.
            queue_maxsize: Max capacity of log queue. Defaults to 10000.
            queue_block: If True, blocks when queue is full. Defaults to False.
            queue_timeout: Seconds to wait if queue_block is True. Defaults to None.
            fallback_path: Log file path to dump logs if PostgreSQL write fails.
                Defaults to None.
            custom_insert_sql: Custom INSERT SQL statement. Defaults to None.
            custom_mapping_func: Custom function to map a loguru record dict to
                a tuple for SQL execution. Defaults to None.
            pool_config: Optional configurations for the PooledDB instance.
        """
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._table_name = table_name
        self._auto_create_table = auto_create_table
        self._batch_size = batch_size
        self._batch_interval = batch_interval
        self._queue_block = queue_block
        self._queue_timeout = queue_timeout
        self._fallback_path = fallback_path
        self._custom_insert_sql = custom_insert_sql
        self._custom_mapping_func = custom_mapping_func

        # Setup standard insert SQL
        if self._custom_insert_sql:
            self._insert_sql = self._custom_insert_sql
        else:
            self._insert_sql = (
                f"INSERT INTO {self._table_name} "
                "(level, message, created_at, logger_name, file_path, line_number, exception) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)"
            )

        # Setup queue and events
        self._stop_event = threading.Event()
        self._queue = queue.Queue(maxsize=queue_maxsize)

        # Setup connection pool
        p_config = {
            "mincached": 2,
            "maxcached": 5,
            "maxconnections": 10,
            "blocking": True,
        }
        if pool_config:
            p_config.update(pool_config)

        self._pool = PooledDB(
            creator=psycopg2,
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            autocommit=True,
            mincached=p_config.pop("mincached"),
            maxcached=p_config.pop("maxcached"),
            maxconnections=p_config.pop("maxconnections"),
            blocking=p_config.pop("blocking"),
            **p_config
        )

        # Handle automatic table creation
        if self._auto_create_table:
            self._create_table_if_not_exists()

        # Start background thread
        self._worker_thread = threading.Thread(target=self._worker, daemon=True)
        self._worker_thread.start()

    def _create_table_if_not_exists(self) -> None:
        """Creates the default log table if it doesn't exist."""
        conn = None
        cursor = None
        try:
            conn = self._pool.connection()
            cursor = conn.cursor()
            ddl = f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                id BIGSERIAL PRIMARY KEY,
                level VARCHAR(20) NOT NULL,
                message TEXT NOT NULL,
                created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                logger_name VARCHAR(255),
                file_path VARCHAR(255),
                line_number INT,
                exception TEXT
            );
            """
            cursor.execute(ddl)
            conn.commit()
        except Exception as e:
            self._handle_internal_error(e, "creating table during startup")
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def write(self, message: Any) -> None:
        """Receives a log message from loguru and puts it into the queue.

        Args:
            message: The loguru Message object which contains the formatted
                message and the original record dictionary.
        """
        record = getattr(message, "record", None)
        if record is None:
            return

        try:
            if self._queue_block:
                self._queue.put(record, block=True, timeout=self._queue_timeout)
            else:
                self._queue.put(record, block=False)
        except queue.Full:
            self._handle_queue_full(record)
        except Exception as e:
            self._handle_internal_error(e, "enqueuing log record")

    def _default_mapping_func(self, record: Dict[str, Any]) -> Tuple[Any, ...]:
        """Maps a loguru record dictionary to the default SQL parameters tuple.

        Args:
            record: The loguru record dictionary.

        Returns:
            A tuple containing database column values.
        """
        level = record["level"].name
        message = record["message"]
        created_at = record["time"].strftime("%Y-%m-%d %H:%M:%S.%f")
        logger_name = record["name"]
        file_path = record["file"].path
        line_number = record["line"]
        
        exception_text = ""
        if record.get("exception"):
            type_, value_, traceback_ = record["exception"]
            import traceback
            exception_text = "".join(traceback.format_exception(type_, value_, traceback_))

        return (level, message, created_at, logger_name, file_path, line_number, exception_text)

    def _handle_queue_full(self, record: Dict[str, Any]) -> None:
        """Handles queue full event by isolating error and writing to fallback.

        Args:
            record: The log record that failed to be enqueued.
        """
        err_msg = "[sclog_lite] Queue full! PostgreSQL Sink dropped log record.\n"
        try:
            sys.stderr.write(err_msg)
            sys.stderr.flush()
        except Exception:
            pass

        # Write to fallback if fallback_path is configured
        if self._fallback_path:
            self._write_to_fallback([record], RuntimeError("Queue full"))

    def _handle_internal_error(self, exception: Exception, context: str = "") -> None:
        """Logs internal errors to stderr without crashing the application.

        Args:
            exception: The exception that was raised.
            context: Context describing where the error occurred.
        """
        try:
            err_msg = f"[sclog_lite] PostgreSQL Sink Error during {context}: {exception}\n"
            sys.stderr.write(err_msg)
            sys.stderr.flush()
        except Exception:
            pass

    def _write_to_fallback(self, batch: List[Dict[str, Any]], exception: Exception) -> None:
        """Saves a batch of logs to the fallback file.

        Args:
            batch: A list of log records that failed to write to PostgreSQL.
            exception: The exception that caused the failure.
        """
        if not self._fallback_path:
            return

        try:
            with open(self._fallback_path, "a", encoding="utf-8") as f:
                for record in batch:
                    # Convert record values to serializable types
                    fallback_record = {
                        "level": record["level"].name,
                        "message": record["message"],
                        "time": record["time"].strftime("%Y-%m-%d %H:%M:%S.%f"),
                        "name": record["name"],
                        "file_path": record["file"].path,
                        "line_number": record["line"],
                        "exception": "",
                        "write_error": f"{type(exception).__name__}: {exception}"
                    }
                    if record.get("exception"):
                        type_, value_, traceback_ = record["exception"]
                        import traceback
                        fallback_record["exception"] = "".join(traceback.format_exception(type_, value_, traceback_))
                    
                    f.write(json.dumps(fallback_record, ensure_ascii=False) + "\n")
        except Exception as fe:
            self._handle_internal_error(fe, "writing to fallback file")

    def _flush_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Writes a batch of log records to the database.

        Args:
            batch: The list of log records to be written.
        """
        if not batch:
            return

        conn = None
        cursor = None
        try:
            conn = self._pool.connection()
            cursor = conn.cursor()
            
            # Map records to parameters
            params = []
            for record in batch:
                if self._custom_mapping_func:
                    param = self._custom_mapping_func(record)
                else:
                    param = self._default_mapping_func(record)
                params.append(param)

            cursor.executemany(self._insert_sql, params)
            conn.commit()
        except Exception as e:
            # Complete failure isolation: we capture the failure and write
            # to fallback if available without raising an exception.
            self._handle_internal_error(e, f"inserting batch of {len(batch)} logs")
            if self._fallback_path:
                self._write_to_fallback(batch, e)
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _worker(self) -> None:
        """Background thread worker method that batches and flushes logs."""
        last_flush_time = time.time()
        batch: List[Dict[str, Any]] = []

        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                # Calculate remaining time for batch_interval
                elapsed = time.time() - last_flush_time
                timeout = max(0.01, self._batch_interval - elapsed)

                try:
                    record = self._queue.get(timeout=timeout)
                    batch.append(record)
                    self._queue.task_done()
                except queue.Empty:
                    pass

                now = time.time()
                # Flush conditions:
                # 1. Batch size exceeded
                # 2. Batch interval expired and we have records
                # 3. Stop event set and we have remaining records
                if (
                    len(batch) >= self._batch_size
                    or (batch and (now - last_flush_time >= self._batch_interval))
                    or (self._stop_event.is_set() and batch)
                ):
                    self._flush_batch(batch)
                    batch = []
                    last_flush_time = time.time()

            except Exception as e:
                # Isolate any unexpected worker errors to keep thread alive
                self._handle_internal_error(e, "worker loop execution")

    def stop(self) -> None:
        """Stops the background worker and flushes remaining logs gracefully."""
        if self._stop_event.is_set():
            return
        
        self._stop_event.set()
        if self._worker_thread and self._worker_thread.is_alive():
            # Wait for background thread to empty queue and write remaining logs
            self._worker_thread.join(timeout=5.0)

        try:
            self._pool.close()
        except Exception as e:
            self._handle_internal_error(e, "closing database pool during shutdown")

    def close(self) -> None:
        """Compatibility alias for stop."""
        self.stop()
