"""
Tests for Snowflake connection and checkpoint utilities.

This module tests the Snowflake utilities including:
- Private key loading and encryption handling
- Connection management and caching
- Snowpark session creation
- Connection testing and validation
- Control table creation
- Checkpoint operations (insert/merge/retrieve)

All tests use mocks and do not connect to real Snowflake instances.
"""

import json
import re
from datetime import datetime, UTC
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, call
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from src.utils.config import SnowflakeConnectionConfig
import src.utils.snowflake as snowflake_utils


# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def sample_private_key():
    """Generate a sample RSA private key for testing."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    return private_key


@pytest.fixture
def unencrypted_key_file(tmp_path, sample_private_key):
    """Create a temporary unencrypted private key file."""
    key_file = tmp_path / "test_key.pem"
    key_pem = sample_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    key_file.write_bytes(key_pem)
    return str(key_file)


@pytest.fixture
def encrypted_key_file(tmp_path, sample_private_key):
    """Create a temporary encrypted private key file."""
    key_file = tmp_path / "test_key_encrypted.pem"
    password = b"test_password"
    key_pem = sample_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(password)
    )
    key_file.write_bytes(key_pem)
    return str(key_file), "test_password"


@pytest.fixture
def mock_snowflake_cursor():
    """Create a mock Snowflake cursor."""
    cursor = MagicMock()
    cursor.execute = MagicMock(return_value=None)
    cursor.fetchone = MagicMock(return_value=("8.0.0",))
    cursor.fetchall = MagicMock(return_value=[])
    cursor.close = MagicMock(return_value=None)
    return cursor


@pytest.fixture
def mock_snowflake_connection(mock_snowflake_cursor):
    """Create a mock Snowflake connection."""
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=mock_snowflake_cursor)
    conn.close = MagicMock(return_value=None)
    conn.is_closed = MagicMock(return_value=False)
    return conn


@pytest.fixture
def snowflake_config(unencrypted_key_file):
    """Create a sample Snowflake connection configuration."""
    return SnowflakeConnectionConfig(
        account="test-account",
        user="test_user",
        private_key_file=unencrypted_key_file,
        private_key_password=None,
        warehouse="TEST_WH",
        database="TEST_DB",
        schema_name="TEST_SCHEMA",
        role="TEST_ROLE",
        pipe_name="TEST_PIPE",
    )


@pytest.fixture(autouse=True)
def clear_connection_cache():
    """Clear connection cache before each test."""
    snowflake_utils._connection_cache.clear()
    snowflake_utils._session_cache.clear()
    yield
    snowflake_utils._connection_cache.clear()
    snowflake_utils._session_cache.clear()


# ============================================================================
# Test Private Key Loading
# ============================================================================


class TestPrivateKeyLoading:
    """Tests for private key loading functionality."""

    def test_load_unencrypted_private_key_returns_der_bytes(self, unencrypted_key_file):
        """Test loading an unencrypted private key returns DER format bytes."""
        # Act
        result = snowflake_utils.load_private_key(unencrypted_key_file)
        
        # Assert
        assert isinstance(result, bytes)
        assert len(result) > 0
        # DER format should not contain PEM markers
        assert b"-----BEGIN" not in result
        assert b"-----END" not in result

    def test_load_encrypted_private_key_with_password_succeeds(self, encrypted_key_file):
        """Test loading an encrypted private key with correct password."""
        # Arrange
        key_file, password = encrypted_key_file
        
        # Act
        result = snowflake_utils.load_private_key(key_file, password)
        
        # Assert
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_load_encrypted_private_key_without_password_raises_error(self, encrypted_key_file):
        """Test loading an encrypted private key without password fails."""
        # Arrange
        key_file, _ = encrypted_key_file
        
        # Act & Assert
        with pytest.raises(ValueError, match="Invalid private key file"):
            snowflake_utils.load_private_key(key_file, None)

    def test_load_private_key_with_invalid_file_raises_error(self, tmp_path):
        """Test loading an invalid key file raises ValueError."""
        # Arrange
        invalid_key_file = tmp_path / "invalid_key.pem"
        invalid_key_file.write_text("This is not a valid key file")
        
        # Act & Assert
        with pytest.raises(ValueError, match="Invalid private key file"):
            snowflake_utils.load_private_key(str(invalid_key_file))

    def test_load_private_key_with_missing_file_raises_error(self):
        """Test loading a non-existent key file raises ValueError."""
        # Act & Assert
        with pytest.raises(ValueError, match="Invalid private key file"):
            snowflake_utils.load_private_key("/nonexistent/key.pem")

    def test_load_private_key_expands_user_path(self, tmp_path, sample_private_key, monkeypatch):
        """Test that load_private_key expands ~ in file path."""
        # Arrange
        monkeypatch.setenv("HOME", str(tmp_path))
        key_dir = tmp_path / ".ssh"
        key_dir.mkdir()
        key_file = key_dir / "test_key.pem"
        
        key_pem = sample_private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        key_file.write_bytes(key_pem)
        
        # Act
        result = snowflake_utils.load_private_key("~/.ssh/test_key.pem")
        
        # Assert
        assert isinstance(result, bytes)
        assert len(result) > 0


# ============================================================================
# Test Connection Management
# ============================================================================


class TestConnectionManagement:
    """Tests for Snowflake connection creation and caching."""

    @patch("src.utils.snowflake.sc.connect")
    def test_get_connection_creates_new_connection(
        self, mock_connect, mock_snowflake_connection, snowflake_config
    ):
        """Test that get_connection creates a new Snowflake connection."""
        # Arrange
        mock_connect.return_value = mock_snowflake_connection
        
        # Act
        conn = snowflake_utils.get_connection(snowflake_config)
        
        # Assert
        assert conn is mock_snowflake_connection
        mock_connect.assert_called_once()
        
        # Verify connection parameters
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["account"] == "test-account"
        assert call_kwargs["user"] == "test_user"
        assert call_kwargs["warehouse"] == "TEST_WH"
        assert call_kwargs["database"] == "TEST_DB"
        assert call_kwargs["schema"] == "TEST_SCHEMA"
        assert call_kwargs["role"] == "TEST_ROLE"
        assert "private_key" in call_kwargs

    @patch("src.utils.snowflake.sc.connect")
    def test_get_connection_caches_connection(
        self, mock_connect, mock_snowflake_connection, snowflake_config
    ):
        """Test that get_connection caches connections for reuse."""
        # Arrange
        mock_connect.return_value = mock_snowflake_connection
        
        # Act
        conn1 = snowflake_utils.get_connection(snowflake_config, use_cache=True)
        conn2 = snowflake_utils.get_connection(snowflake_config, use_cache=True)
        
        # Assert
        assert conn1 is conn2
        mock_connect.assert_called_once()  # Should only connect once

    @patch("src.utils.snowflake.sc.connect")
    def test_get_connection_without_cache_creates_new_connection(
        self, mock_connect, mock_snowflake_connection, snowflake_config
    ):
        """Test that get_connection with use_cache=False creates new connection."""
        # Arrange
        mock_connect.return_value = mock_snowflake_connection
        
        # Act
        conn1 = snowflake_utils.get_connection(snowflake_config, use_cache=False)
        conn2 = snowflake_utils.get_connection(snowflake_config, use_cache=False)
        
        # Assert
        assert conn1 is conn2  # Same mock object returned
        assert mock_connect.call_count == 2  # But connected twice

    @patch("src.utils.snowflake.sc.connect")
    def test_get_connection_detects_stale_connection(
        self, mock_connect, mock_snowflake_cursor, snowflake_config
    ):
        """Test that get_connection detects and replaces stale connections."""
        # Arrange
        stale_conn = MagicMock()
        stale_cursor = MagicMock()
        stale_cursor.execute.side_effect = Exception("Connection lost")
        stale_conn.cursor.return_value = stale_cursor
        
        fresh_conn = MagicMock()
        fresh_cursor = MagicMock()
        fresh_cursor.execute.return_value = None
        fresh_cursor.close.return_value = None
        fresh_conn.cursor.return_value = fresh_cursor
        
        mock_connect.side_effect = [stale_conn, fresh_conn]
        
        # Act - First call caches stale connection
        conn1 = snowflake_utils.get_connection(snowflake_config, use_cache=True)
        
        # Second call should detect stale and create new one
        conn2 = snowflake_utils.get_connection(snowflake_config, use_cache=True)
        
        # Assert
        assert conn1 is stale_conn
        assert conn2 is fresh_conn
        assert mock_connect.call_count == 2

    @patch("src.utils.snowflake.sc.connect")
    def test_get_connection_handles_connection_error(self, mock_connect, snowflake_config):
        """Test that get_connection propagates connection errors."""
        # Arrange
        mock_connect.side_effect = Exception("Connection failed")
        
        # Act & Assert
        with pytest.raises(Exception, match="Connection failed"):
            snowflake_utils.get_connection(snowflake_config)

    @patch("src.utils.snowflake.sc.connect")
    def test_get_connection_without_role_omits_role_parameter(
        self, mock_connect, mock_snowflake_connection, unencrypted_key_file
    ):
        """Test that get_connection omits role parameter when not set."""
        # Arrange
        config = SnowflakeConnectionConfig(
            account="test-account",
            user="test_user",
            private_key_file=unencrypted_key_file,
            warehouse="TEST_WH",
            database="TEST_DB",
            schema_name="TEST_SCHEMA",
            role=None,
            pipe_name="TEST_PIPE",
        )
        mock_connect.return_value = mock_snowflake_connection
        
        # Act
        snowflake_utils.get_connection(config)
        
        # Assert
        call_kwargs = mock_connect.call_args[1]
        assert "role" not in call_kwargs


# ============================================================================
# Test Cache Management
# ============================================================================


class TestCacheManagement:
    """Tests for connection cache management."""

    def test_close_all_cached_connections_closes_connections(self):
        """Test that close_all_cached_connections closes all cached connections."""
        # Arrange
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_session1 = MagicMock()
        
        cache_key1 = ("account1", "user1", "db1", "schema1", "wh1", "role1")
        cache_key2 = ("account2", "user2", "db2", "schema2", "wh2", "role2")
        
        snowflake_utils._connection_cache[cache_key1] = mock_conn1
        snowflake_utils._connection_cache[cache_key2] = mock_conn2
        snowflake_utils._session_cache[cache_key1] = mock_session1
        
        # Act
        snowflake_utils.close_all_cached_connections()
        
        # Assert
        mock_conn1.close.assert_called_once()
        mock_conn2.close.assert_called_once()
        mock_session1.close.assert_called_once()
        assert len(snowflake_utils._connection_cache) == 0
        assert len(snowflake_utils._session_cache) == 0

    def test_close_all_cached_connections_handles_close_errors(self):
        """Test that close_all_cached_connections handles errors gracefully."""
        # Arrange
        mock_conn = MagicMock()
        mock_conn.close.side_effect = Exception("Close failed")
        
        cache_key = ("account", "user", "db", "schema", "wh", "role")
        snowflake_utils._connection_cache[cache_key] = mock_conn
        
        # Act - Should not raise exception
        snowflake_utils.close_all_cached_connections()
        
        # Assert
        assert len(snowflake_utils._connection_cache) == 0

    def test_cache_key_generation(self, snowflake_config):
        """Test that cache key is generated correctly from config."""
        # Act
        cache_key = snowflake_utils._get_cache_key(snowflake_config)
        
        # Assert
        assert cache_key == (
            "test-account",
            "test_user",
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_WH",
            "TEST_ROLE",
        )

    def test_is_connection_alive_returns_true_for_healthy_connection(self, mock_snowflake_connection):
        """Test that _is_connection_alive returns True for healthy connection."""
        # Act
        result = snowflake_utils._is_connection_alive(mock_snowflake_connection)
        
        # Assert
        assert result is True
        mock_snowflake_connection.cursor.assert_called_once()

    def test_is_connection_alive_returns_false_for_dead_connection(self):
        """Test that _is_connection_alive returns False for dead connection."""
        # Arrange
        dead_conn = MagicMock()
        dead_cursor = MagicMock()
        dead_cursor.execute.side_effect = Exception("Connection lost")
        dead_conn.cursor.return_value = dead_cursor
        
        # Act
        result = snowflake_utils._is_connection_alive(dead_conn)
        
        # Assert
        assert result is False


# ============================================================================
# Test Snowpark Sessions
# ============================================================================


class TestSnowparkSessions:
    """Tests for Snowpark session creation."""

    @patch("src.utils.snowflake.SNOWPARK_AVAILABLE", True)
    @patch("src.utils.snowflake.Session")
    def test_get_snowpark_session_creates_session_successfully(
        self, mock_session_class, snowflake_config
    ):
        """Test that get_snowpark_session creates a Snowpark session."""
        # Arrange
        mock_session = MagicMock()
        mock_builder = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        mock_session_class.builder = mock_builder
        
        mock_sql_result = MagicMock()
        mock_session.sql.return_value = mock_sql_result
        
        # Act
        session = snowflake_utils.get_snowpark_session(snowflake_config)
        
        # Assert
        assert session is mock_session
        mock_builder.configs.assert_called_once()
        
        # Verify warehouse activation
        mock_session.sql.assert_called_once_with("USE WAREHOUSE TEST_WH")
        mock_sql_result.collect.assert_called_once()

    @patch("src.utils.snowflake.SNOWPARK_AVAILABLE", False)
    def test_get_snowpark_session_raises_error_when_snowpark_not_available(self, snowflake_config):
        """Test that get_snowpark_session raises ImportError when snowpark is not installed."""
        # Act & Assert
        with pytest.raises(ImportError, match="snowflake-snowpark is not installed"):
            snowflake_utils.get_snowpark_session(snowflake_config)

    @patch("src.utils.snowflake.SNOWPARK_AVAILABLE", True)
    @patch("src.utils.snowflake.Session")
    def test_get_snowpark_session_handles_creation_error(
        self, mock_session_class, snowflake_config
    ):
        """Test that get_snowpark_session propagates session creation errors."""
        # Arrange
        mock_builder = MagicMock()
        mock_builder.configs.return_value.create.side_effect = Exception("Session creation failed")
        mock_session_class.builder = mock_builder
        
        # Act & Assert
        with pytest.raises(Exception, match="Session creation failed"):
            snowflake_utils.get_snowpark_session(snowflake_config)

    @patch("src.utils.snowflake.SNOWPARK_AVAILABLE", True)
    @patch("src.utils.snowflake.Session")
    def test_get_snowpark_session_includes_role_when_set(
        self, mock_session_class, snowflake_config
    ):
        """Test that get_snowpark_session includes role in connection parameters."""
        # Arrange
        mock_session = MagicMock()
        mock_builder = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        mock_session_class.builder = mock_builder
        mock_session.sql.return_value.collect.return_value = None
        
        # Act
        snowflake_utils.get_snowpark_session(snowflake_config)
        
        # Assert
        call_args = mock_builder.configs.call_args[0][0]
        assert "role" in call_args
        assert call_args["role"] == "TEST_ROLE"


# ============================================================================
# Test Connection Testing
# ============================================================================


class TestConnectionTesting:
    """Tests for connection testing functionality."""

    @patch("src.utils.snowflake.get_connection")
    def test_check_connection_returns_true_for_valid_connection(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that check_connection returns True for valid connection."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_cursor.fetchone.side_effect = [
            ("8.0.0",),  # Version query
            ("TEST_DB", "TEST_SCHEMA", "TEST_WH"),  # Context query
        ]
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act
        result = snowflake_utils.check_connection(snowflake_config)
        
        # Assert
        assert result is True
        mock_get_connection.assert_called_once_with(snowflake_config)
        assert mock_cursor.execute.call_count == 2
        mock_snowflake_connection.close.assert_called_once()

    @patch("src.utils.snowflake.get_connection")
    def test_check_connection_verifies_database_context(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that check_connection verifies database context."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_cursor.fetchone.side_effect = [
            ("8.0.0",),
            ("TEST_DB", "TEST_SCHEMA", "TEST_WH"),
        ]
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act
        result = snowflake_utils.check_connection(snowflake_config)
        
        # Assert
        assert result is True
        # Verify context query was executed
        calls = mock_cursor.execute.call_args_list
        assert any("CURRENT_DATABASE" in str(call) for call in calls)

    @patch("src.utils.snowflake.get_connection")
    def test_check_connection_warns_on_context_mismatch(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config, caplog
    ):
        """Test that check_connection warns when context doesn't match config."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_cursor.fetchone.side_effect = [
            ("8.0.0",),
            ("WRONG_DB", "WRONG_SCHEMA", "WRONG_WH"),  # Wrong context
        ]
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act
        result = snowflake_utils.check_connection(snowflake_config)
        
        # Assert
        assert result is True  # Still returns True
        # Check that warnings were logged
        assert "different database" in caplog.text.lower() or "WRONG_DB" in caplog.text

    @patch("src.utils.snowflake.get_connection")
    def test_check_connection_handles_connection_failure(
        self, mock_get_connection, snowflake_config
    ):
        """Test that check_connection propagates connection failures."""
        # Arrange
        mock_get_connection.side_effect = Exception("Connection failed")
        
        # Act & Assert
        with pytest.raises(Exception, match="Connection failed"):
            snowflake_utils.check_connection(snowflake_config)

    @patch("src.utils.snowflake.get_connection")
    def test_check_connection_returns_false_when_no_version(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that check_connection returns False when version query returns nothing."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_cursor.fetchone.return_value = None
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act
        result = snowflake_utils.check_connection(snowflake_config)
        
        # Assert
        assert result is False
        mock_snowflake_connection.close.assert_called_once()


# ============================================================================
# Test Control Table Creation
# ============================================================================


class TestControlTable:
    """Tests for control table creation."""

    @patch("src.utils.snowflake.get_connection")
    def test_create_control_table_creates_schema_and_table(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that create_control_table creates schema and hybrid table."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act
        result = snowflake_utils.create_control_table(
            target_db="CONTROL_DB",
            target_schema="PUBLIC",
            target_table="INGESTION_STATUS",
            config=snowflake_config
        )
        
        # Assert
        assert result is True
        
        # Verify schema creation
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any("CREATE SCHEMA IF NOT EXISTS" in call for call in execute_calls)
        
        # Verify table creation with HYBRID TABLE
        assert any("CREATE HYBRID TABLE IF NOT EXISTS" in call for call in execute_calls)
        
        # Verify PRIMARY KEY constraint
        table_ddl_call = [call for call in execute_calls if "CREATE HYBRID TABLE" in call][0]
        assert "PRIMARY KEY" in table_ddl_call
        assert "PARTITION_ID" in table_ddl_call
        
        mock_snowflake_connection.close.assert_called_once()

    @patch("src.utils.snowflake.get_connection")
    def test_create_control_table_validates_identifiers(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that create_control_table validates identifier names."""
        # Arrange
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act & Assert - Should raise ValueError for SQL injection attempt
        with pytest.raises(ValueError, match="Invalid Snowflake identifier"):
            snowflake_utils.create_control_table(
                target_db="DROP TABLE users; --",
                target_schema="PUBLIC",
                target_table="INGESTION_STATUS",
                config=snowflake_config
            )

    @patch("src.utils.snowflake.get_connection")
    def test_create_control_table_handles_creation_error(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that create_control_table propagates creation errors."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_cursor.execute.side_effect = Exception("Table creation failed")
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act & Assert
        with pytest.raises(Exception, match="Table creation failed"):
            snowflake_utils.create_control_table(
                target_db="CONTROL_DB",
                target_schema="PUBLIC",
                target_table="INGESTION_STATUS",
                config=snowflake_config
            )
        
        mock_snowflake_connection.close.assert_called_once()

    @patch("src.utils.snowflake.get_connection")
    def test_create_control_table_with_special_characters_in_valid_identifiers(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that create_control_table accepts valid identifiers with underscores and dollar signs."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act
        result = snowflake_utils.create_control_table(
            target_db="CONTROL_DB_2024",
            target_schema="PUBLIC_$SCHEMA",
            target_table="INGESTION_STATUS_V2",
            config=snowflake_config
        )
        
        # Assert
        assert result is True


# ============================================================================
# Test Checkpoint Operations
# ============================================================================


class TestCheckpointOperations:
    """Tests for checkpoint insert and retrieval operations."""

    @patch("src.utils.snowflake.get_connection")
    def test_insert_partition_checkpoint_inserts_new_checkpoint(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that insert_partition_checkpoint inserts a new checkpoint."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_get_connection.return_value = mock_snowflake_connection
        
        metadata = {"last_offset": "12345", "message_count": 100}
        
        # Act
        snowflake_utils.insert_partition_checkpoint(
            eventhub_namespace="test-namespace.servicebus.windows.net",
            eventhub="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            partition_id="0",
            waterlevel=1000,
            metadata=metadata,
            config=snowflake_config,
            control_db="CONTROL_DB",
            control_schema="PUBLIC",
            control_table="INGESTION_STATUS"
        )
        
        # Assert
        execute_calls = mock_cursor.execute.call_args_list
        
        # Verify MERGE query was executed
        merge_call = execute_calls[-1]  # Last execute call should be the MERGE
        merge_query = merge_call[0][0]
        assert "MERGE INTO" in merge_query
        assert "CONTROL_DB.PUBLIC.INGESTION_STATUS" in merge_query
        
        # Verify parameters
        params = merge_call[0][1]
        assert params[0] == "test-namespace.servicebus.windows.net"
        assert params[1] == "test-hub"
        assert params[2] == "TEST_DB"
        assert params[3] == "TEST_SCHEMA"
        assert params[4] == "TEST_TABLE"
        assert params[5] == "0"
        assert params[6] == 1000
        
        # Verify metadata JSON
        metadata_json = params[7]
        assert '"last_offset": "12345"' in metadata_json
        assert '"message_count": 100' in metadata_json

    @patch("src.utils.snowflake.get_connection")
    def test_insert_partition_checkpoint_uses_cached_connection(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that insert_partition_checkpoint uses cached connection and doesn't close it."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act
        snowflake_utils.insert_partition_checkpoint(
            eventhub_namespace="test-namespace.servicebus.windows.net",
            eventhub="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            partition_id="0",
            waterlevel=1000,
            config=snowflake_config
        )
        
        # Assert
        mock_get_connection.assert_called_once_with(snowflake_config, use_cache=True)
        # Connection should NOT be closed (it's cached for reuse)
        mock_snowflake_connection.close.assert_not_called()

    @patch("src.utils.snowflake.get_connection")
    def test_insert_partition_checkpoint_activates_warehouse(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that insert_partition_checkpoint activates warehouse before DML."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act
        snowflake_utils.insert_partition_checkpoint(
            eventhub_namespace="test-namespace.servicebus.windows.net",
            eventhub="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            partition_id="0",
            waterlevel=1000,
            config=snowflake_config
        )
        
        # Assert
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any("USE WAREHOUSE TEST_WH" in call for call in execute_calls)

    @patch("src.utils.snowflake.get_connection")
    def test_insert_partition_checkpoint_without_metadata(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that insert_partition_checkpoint handles None metadata."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act
        snowflake_utils.insert_partition_checkpoint(
            eventhub_namespace="test-namespace.servicebus.windows.net",
            eventhub="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            partition_id="0",
            waterlevel=1000,
            metadata=None,
            config=snowflake_config
        )
        
        # Assert
        merge_call = mock_cursor.execute.call_args_list[-1]
        params = merge_call[0][1]
        metadata_json = params[7]
        assert metadata_json is None

    @patch("src.utils.snowflake.get_connection")
    def test_insert_partition_checkpoint_uses_default_control_table_location(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that insert_partition_checkpoint uses config database/schema as default."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act
        snowflake_utils.insert_partition_checkpoint(
            eventhub_namespace="test-namespace.servicebus.windows.net",
            eventhub="test-hub",
            target_db="DATA_DB",
            target_schema="DATA_SCHEMA",
            target_table="DATA_TABLE",
            partition_id="0",
            waterlevel=1000,
            config=snowflake_config
            # No control_db, control_schema, control_table provided
        )
        
        # Assert
        merge_call = mock_cursor.execute.call_args_list[-1]
        merge_query = merge_call[0][0]
        # Should use config.database and config.schema_name
        assert f"{snowflake_config.database}.{snowflake_config.schema_name}.INGESTION_STATUS" in merge_query

    @patch("src.utils.snowflake.get_connection")
    def test_insert_partition_checkpoint_validates_identifiers(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that insert_partition_checkpoint validates identifiers."""
        # Arrange
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act & Assert
        with pytest.raises(ValueError, match="Invalid Snowflake identifier"):
            snowflake_utils.insert_partition_checkpoint(
                eventhub_namespace="test-namespace",
                eventhub="test-hub",
                target_db="'; DROP TABLE users; --",
                target_schema="PUBLIC",
                target_table="TEST_TABLE",
                partition_id="0",
                waterlevel=1000,
                config=snowflake_config
            )

    @patch("src.utils.snowflake.get_connection")
    def test_insert_partition_checkpoint_handles_merge_error(
        self, mock_get_connection, mock_snowflake_connection, snowflake_config
    ):
        """Test that insert_partition_checkpoint propagates merge errors."""
        # Arrange
        mock_cursor = mock_snowflake_connection.cursor.return_value
        mock_cursor.execute.side_effect = Exception("Merge failed")
        mock_get_connection.return_value = mock_snowflake_connection
        
        # Act & Assert
        with pytest.raises(Exception, match="Merge failed"):
            snowflake_utils.insert_partition_checkpoint(
                eventhub_namespace="test-namespace",
                eventhub="test-hub",
                target_db="TEST_DB",
                target_schema="TEST_SCHEMA",
                target_table="TEST_TABLE",
                partition_id="0",
                waterlevel=1000,
                config=snowflake_config
            )

    @patch("src.utils.snowflake.get_snowpark_session")
    def test_get_partition_checkpoints_handles_query_error(
        self, mock_get_session, snowflake_config
    ):
        """Test that get_partition_checkpoints propagates query errors."""
        # Arrange
        mock_session = MagicMock()
        mock_session.table.side_effect = Exception("Query failed")
        mock_get_session.return_value = mock_session
        
        # Act & Assert
        with pytest.raises(Exception, match="Query failed"):
            snowflake_utils.get_partition_checkpoints(
                eventhub_namespace="test-namespace",
                eventhub="test-hub",
                target_db="TEST_DB",
                target_schema="TEST_SCHEMA",
                target_table="TEST_TABLE",
                config=snowflake_config
            )
        
        mock_session.close.assert_called_once()
