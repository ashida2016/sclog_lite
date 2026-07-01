"""Unit tests for sclog_lite.

This module contains comprehensive test cases covering console/file logging,
MySQL sink integration, connection pooling, asynchronous batch writing,
and write failure/queue-full isolation.
"""

import json
import os
import queue
import time
from unittest.mock import MagicMock

import pytest
import pymysql

from sclog_lite import add_mysql_sink, logger, setup_logging
from sclog_lite.mysql_sink import MySQLSink


@pytest.fixture
def mock_db_pool(mocker):
    """Fixture to mock DBUtils PooledDB connection pool."""
    # Mock PooledDB class in sclog_lite.mysql_sink
    mock_pool_cls = mocker.patch("sclog_lite.mysql_sink.PooledDB")
    
    mock_pool_inst = MagicMock()
    mock_pool_cls.return_value = mock_pool_inst
    
    mock_conn = MagicMock()
    mock_pool_inst.connection.return_value = mock_conn
    
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    return {
        "pool_class": mock_pool_cls,
        "pool_instance": mock_pool_inst,
        "connection": mock_conn,
        "cursor": mock_cursor,
    }


def test_setup_logging(tmp_path):
    """Tests setup_logging helper with file and console logs."""
    log_file = tmp_path / "test_file.log"
    setup_logging(
        console=True,
        file_path=str(log_file),
        console_level="DEBUG",
        file_level="DEBUG",
        clear_existing=True,
    )
    
    logger.debug("Test console and file logging")
    
    # Verify the file was created and contains the log
    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")
    assert "Test console and file logging" in content
    
    # Cleanup loguru sinks
    logger.remove()


def test_mysql_sink_batching(mocker, mock_db_pool):
    """Tests MySQLSink batch size threshold triggering."""
    mock_cursor = mock_db_pool["cursor"]
    
    sink = MySQLSink(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db",
        batch_size=3,
        batch_interval=1.0,
        auto_create_table=True,
    )
    
    # Table creation check
    mock_cursor.execute.assert_called()
    assert "CREATE TABLE IF NOT EXISTS" in mock_cursor.execute.call_args[0][0]
    
    class MockMessage(str):
        def __init__(self, text):
            self.record = {
                "level": MagicMock(),
                "message": text,
                "time": MagicMock(),
                "name": "test_logger",
                "file": MagicMock(path="test.py"),
                "line": 42,
                "exception": None,
            }
            self.record["level"].name = "INFO"
            self.record["time"].strftime.return_value = "2026-07-01 12:00:00.000000"
            
    # Send 2 records. Since batch_size=3, it should not trigger flush immediately
    sink.write(MockMessage("msg1"))
    sink.write(MockMessage("msg2"))
    
    time.sleep(0.1)
    mock_cursor.executemany.assert_not_called()
    
    # Send 3rd record, which should trigger an immediate flush
    sink.write(MockMessage("msg3"))
    
    # Poll for batch execution
    start_time = time.time()
    while not mock_cursor.executemany.called and time.time() - start_time < 1.0:
        time.sleep(0.01)
        
    mock_cursor.executemany.assert_called_once()
    sql, params = mock_cursor.executemany.call_args[0]
    assert "INSERT INTO log_entries" in sql
    assert len(params) == 3
    assert params[0][1] == "msg1"
    assert params[1][1] == "msg2"
    assert params[2][1] == "msg3"
    
    sink.stop()


def test_mysql_sink_interval_flush(mocker, mock_db_pool):
    """Tests MySQLSink time-based threshold flushing."""
    mock_cursor = mock_db_pool["cursor"]
    
    sink = MySQLSink(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db",
        batch_size=10,
        batch_interval=0.1,  # Short interval for quick test
        auto_create_table=False,
    )
    
    class MockMessage(str):
        def __init__(self, text):
            self.record = {
                "level": MagicMock(),
                "message": text,
                "time": MagicMock(),
                "name": "test_logger",
                "file": MagicMock(path="test.py"),
                "line": 42,
                "exception": None,
            }
            self.record["level"].name = "INFO"
            self.record["time"].strftime.return_value = "2026-07-01 12:00:00.000000"
            
    # Write 1 record
    sink.write(MockMessage("interval_msg"))
    
    # Poll for time-based flush
    start_time = time.time()
    while not mock_cursor.executemany.called and time.time() - start_time < 1.0:
        time.sleep(0.01)
        
    mock_cursor.executemany.assert_called_once()
    assert len(mock_cursor.executemany.call_args[0][1]) == 1
    assert mock_cursor.executemany.call_args[0][1][0][1] == "interval_msg"
    
    sink.stop()


def test_mysql_sink_failure_isolation(mocker, mock_db_pool, tmp_path):
    """Tests MySQLSink failure isolation and JSONL fallback logging."""
    mock_cursor = mock_db_pool["cursor"]
    
    # Simulate DB error during insert
    mock_cursor.executemany.side_effect = pymysql.err.OperationalError(
        2003, "Can't connect to MySQL server"
    )
    
    fallback_file = tmp_path / "fallback.jsonl"
    
    sink = MySQLSink(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db",
        batch_size=1,
        batch_interval=1.0,
        fallback_path=str(fallback_file),
        auto_create_table=False,
    )
    
    class MockMessage(str):
        def __init__(self, text):
            self.record = {
                "level": MagicMock(),
                "message": text,
                "time": MagicMock(),
                "name": "test_logger",
                "file": MagicMock(path="test.py"),
                "line": 42,
                "exception": None,
            }
            self.record["level"].name = "ERROR"
            self.record["time"].strftime.return_value = "2026-07-01 12:00:00.000000"
            
    # Write a record
    sink.write(MockMessage("failed_msg"))
    
    # Wait for fallback file to be written
    start_time = time.time()
    while not fallback_file.exists() and time.time() - start_time < 1.0:
        time.sleep(0.01)
        
    assert fallback_file.exists()
    
    # Parse and verify fallback JSON
    with open(fallback_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["message"] == "failed_msg"
        assert data["level"] == "ERROR"
        assert "OperationalError" in data["write_error"]
        
    # Verify the background thread is still alive and didn't crash
    assert sink._worker_thread.is_alive()
    
    # Verify recovery: remove error and write another message
    mock_cursor.executemany.side_effect = None
    sink.write(MockMessage("recovered_msg"))
    
    start_time = time.time()
    while mock_cursor.executemany.call_count < 2 and time.time() - start_time < 1.0:
        time.sleep(0.01)
        
    assert mock_cursor.executemany.call_count == 2
    sink.stop()


def test_mysql_sink_queue_full_isolation(mocker, mock_db_pool, tmp_path):
    """Tests MySQLSink queue full error isolation and fallback logging."""
    fallback_file = tmp_path / "queue_full_fallback.jsonl"
    
    sink = MySQLSink(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db",
        queue_maxsize=1,
        queue_block=False,
        fallback_path=str(fallback_file),
        auto_create_table=False,
    )
    
    class MockMessage(str):
        def __init__(self, text):
            self.record = {
                "level": MagicMock(),
                "message": text,
                "time": MagicMock(),
                "name": "test_logger",
                "file": MagicMock(path="test.py"),
                "line": 42,
                "exception": None,
            }
            self.record["level"].name = "INFO"
            self.record["time"].strftime.return_value = "2026-07-01 12:00:00.000000"
            
    # Mock queue.put to raise queue.Full
    mocker.patch.object(sink._queue, "put", side_effect=queue.Full)
    
    # Log record. Should trigger queue full fallback on the calling thread
    sink.write(MockMessage("dropped_msg"))
    
    assert fallback_file.exists()
    
    with open(fallback_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["message"] == "dropped_msg"
        assert "Queue full" in data["write_error"]
        
    sink.stop()


def test_logger_add_mysql_sink(mocker, mock_db_pool):
    """Tests direct logger.add_mysql_sink and logger integration."""
    mock_cursor = mock_db_pool["cursor"]
    
    logger.remove()
    
    sink_id = add_mysql_sink(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db",
        batch_size=1,
        batch_interval=1.0,
        auto_create_table=False,
    )
    
    assert isinstance(sink_id, int)
    
    # Send log
    logger.info("Log through loguru wrapper")
    
    # Wait for flush
    start_time = time.time()
    while not mock_cursor.executemany.called and time.time() - start_time < 1.0:
        time.sleep(0.01)
        
    mock_cursor.executemany.assert_called_once()
    assert mock_cursor.executemany.call_args[0][1][0][1] == "Log through loguru wrapper"
    
    logger.remove(sink_id)


def test_setup_logging_defaults(tmp_path, mocker):
    """Tests setup_logging path resolution and defaults."""
    # Mock _get_project_root to point to our tmp_path
    mocker.patch("sclog_lite.logger._get_project_root", return_value=str(tmp_path))
    
    # Case 1: None (default log path)
    setup_logging(
        console=False,
        file_path=None,
        clear_existing=True,
    )
    logger.info("Default log file test")
    logger.remove()
    
    expected_default_file = tmp_path / "logs" / "sclog.log"
    assert expected_default_file.exists()
    assert "Default log file test" in expected_default_file.read_text(encoding="utf-8")
    
    # Case 2: Directory path (should append sclog.log)
    custom_dir = tmp_path / "custom_dir"
    custom_dir.mkdir()
    setup_logging(
        console=False,
        file_path=str(custom_dir),
        clear_existing=True,
    )
    logger.info("Custom dir test")
    logger.remove()
    
    expected_custom_file = custom_dir / "sclog.log"
    assert expected_custom_file.exists()
    assert "Custom dir test" in expected_custom_file.read_text(encoding="utf-8")

    # Case 3: False / disabled file logging
    setup_logging(
        console=False,
        file_path=False,
        clear_existing=True,
    )
    # Since there are no sinks left, logger.info shouldn't raise any file errors
    logger.info("Should not log to any file")
    logger.remove()


def test_mysql_sink_real_database():
    """Integration test using the actual test database mysql.lan."""
    
    host = "mysql.lan"
    '''
    try:
        import socket
        socket.gethostbyname(host)
    except socket.gaierror:
        # If DNS fails, fallback to direct IP 10.10.10.50
        host = "10.10.10.50"
    '''

    db_config = {
        "host": host,
        "port": 3306,
        "user": "test5001",
        "password": "Love2026",
        "database": "test_5001",
        "table_name": "test_log_entries_integration",
    }
    
    # Pre-clean the table if it exists
    try:
        conn = pymysql.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {db_config['table_name']}")
        conn.commit()
    except Exception as e:
        pytest.skip(f"Skip real DB test because connection failed: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

    # Configure logger with real DB sink
    logger.remove()
    sink_id = add_mysql_sink(
        **db_config,
        batch_size=2,
        batch_interval=0.5,
        auto_create_table=True,
    )
    
    try:
        # Write 3 logs to trigger batch write (size 2) and interval write (size 1)
        logger.info("Real DB test log 1")
        logger.info("Real DB test log 2")
        logger.warning("Real DB test log 3")
        
        # Give some time to write
        time.sleep(1.2)
        
        # Verify from database
        conn = pymysql.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT level, message FROM {db_config['table_name']} ORDER BY id ASC")
            rows = cursor.fetchall()
            
        assert len(rows) == 3
        assert rows[0] == ("INFO", "Real DB test log 1")
        assert rows[1] == ("INFO", "Real DB test log 2")
        assert rows[2] == ("WARNING", "Real DB test log 3")
        
        # Drop the table to clean up
        # with conn.cursor() as cursor:
        #    cursor.execute(f"DROP TABLE IF EXISTS {db_config['table_name']}")
        conn.commit()
        
    finally:
        logger.remove(sink_id)
        if 'conn' in locals() and conn:
            conn.close()

