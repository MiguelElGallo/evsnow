"""
Tests for Snowflake high-performance streaming client.

This module tests the SnowflakeHighPerformanceStreamingClient class including:
- Client lifecycle (initialization, start, stop)
- Connection profile building
- Channel management (create, reuse, statistics)
- Data ingestion (successful, empty batches, errors)
- Retry logic and error handling
- Private key handling (file loading, encryption)
- Statistics tracking
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from src.streaming.snowflake_high_performance import (
    SnowflakeHighPerformanceStreamingClient,
    create_snowflake_streaming_client,
)
from src.utils.config import SnowflakeConnectionConfig


class TestSnowflakeHighPerformanceStreamingClient:
    """Tests for SnowflakeHighPerformanceStreamingClient class."""

    # ========================================================================
    # Initialization Tests
    # ========================================================================

    def test_init_creates_client_with_config(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that client initializes correctly with configuration."""
        # Act
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            client_name_suffix="test-123",
        )

        # Assert
        assert client.snowflake_config == sample_snowflake_config
        assert client.connection_config == sample_snowflake_connection_config
        assert client.client_name_suffix == "test-123"
        assert client.streaming_client is None
        assert client.channels == {}
        assert client.stats["total_messages_sent"] == 0
        assert client.stats["total_batches_sent"] == 0
        assert client.stats["channels_created"] == 0

    def test_init_without_suffix_generates_uuid_suffix(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that client generates UUID suffix when none provided."""
        # Act
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Assert
        assert client.client_name_suffix is not None
        assert len(client.client_name_suffix) == 8  # UUID truncated to 8 chars

    def test_init_with_retry_manager_applies_decorator(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mocker,
    ):
        """Test that retry decorator is applied when retry_manager is provided."""
        # Arrange
        mock_retry_manager = mocker.MagicMock()
        mock_decorator = mocker.MagicMock(side_effect=lambda f: f)
        mock_retry_manager.get_retry_decorator.return_value = mock_decorator

        # Act
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            retry_manager=mock_retry_manager,
        )

        # Assert
        assert client.retry_manager == mock_retry_manager
        mock_retry_manager.get_retry_decorator.assert_called_once()

    def test_is_started_returns_false_before_start(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that is_started returns False before client is started."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Act & Assert
        assert not client.is_started

    # ========================================================================
    # Connection Profile Building Tests
    # ========================================================================

    def test_build_connection_profile_with_unencrypted_key(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mocker,
    ):
        """Test building connection profile with unencrypted private key."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Mock private key file content
        mock_key_content = b"-----BEGIN PRIVATE KEY-----\ntest_key_data\n-----END PRIVATE KEY-----"
        mock_private_key = mocker.MagicMock()
        mock_private_key.private_bytes.return_value = mock_key_content

        mocker.patch("pathlib.Path.open", mocker.mock_open(read_data=mock_key_content))
        mocker.patch(
            "cryptography.hazmat.primitives.serialization.load_pem_private_key",
            return_value=mock_private_key,
        )

        # Act
        profile = client._build_connection_profile()

        # Assert
        assert profile["user"] == sample_snowflake_connection_config.user
        assert profile["account"] == sample_snowflake_connection_config.account
        assert profile["url"] == f"https://{sample_snowflake_connection_config.account}.snowflakecomputing.com:443"
        assert "private_key" in profile
        assert profile["role"] == sample_snowflake_connection_config.role

    def test_build_connection_profile_with_encrypted_key(
        self,
        sample_snowflake_config,
        tmp_path,
        mocker,
    ):
        """Test building connection profile with encrypted private key."""
        # Arrange
        key_file = tmp_path / "encrypted_key.pem"
        key_file.write_text("-----BEGIN ENCRYPTED PRIVATE KEY-----\ntest\n-----END ENCRYPTED PRIVATE KEY-----")

        connection_config = SnowflakeConnectionConfig(
            account="test-account",
            user="test_user",
            private_key_file=str(key_file),
            private_key_password="test_password",
            warehouse="TEST_WH",
            database="TEST_DB",
            schema_name="TEST_SCHEMA",
            role="TEST_ROLE",
            pipe_name="TEST_PIPE",
        )

        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=connection_config,
        )

        # Mock private key decryption
        mock_key_content = b"-----BEGIN ENCRYPTED PRIVATE KEY-----\ntest\n-----END ENCRYPTED PRIVATE KEY-----"
        mock_private_key = mocker.MagicMock()
        mock_private_key.private_bytes.return_value = b"-----BEGIN PRIVATE KEY-----\ndecrypted\n-----END PRIVATE KEY-----"

        mocker.patch("pathlib.Path.open", mocker.mock_open(read_data=mock_key_content))
        mock_load_key = mocker.patch(
            "cryptography.hazmat.primitives.serialization.load_pem_private_key",
            return_value=mock_private_key,
        )

        # Act
        profile = client._build_connection_profile()

        # Assert
        assert "private_key" in profile
        # Verify password was passed to load_pem_private_key
        mock_load_key.assert_called_once()
        call_args = mock_load_key.call_args
        assert call_args[1]["password"] == b"test_password"

    def test_build_connection_profile_without_role(
        self,
        sample_snowflake_config,
        tmp_path,
        mocker,
    ):
        """Test building connection profile without optional role."""
        # Arrange
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")

        connection_config = SnowflakeConnectionConfig(
            account="test-account",
            user="test_user",
            private_key_file=str(key_file),
            private_key_password=None,
            warehouse="TEST_WH",
            database="TEST_DB",
            schema_name="TEST_SCHEMA",
            role=None,  # No role specified
            pipe_name="TEST_PIPE",
        )

        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=connection_config,
        )

        # Mock private key loading
        mock_key_content = b"-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----"
        mock_private_key = mocker.MagicMock()
        mock_private_key.private_bytes.return_value = mock_key_content

        mocker.patch("pathlib.Path.open", mocker.mock_open(read_data=mock_key_content))
        mocker.patch(
            "cryptography.hazmat.primitives.serialization.load_pem_private_key",
            return_value=mock_private_key,
        )

        # Act
        profile = client._build_connection_profile()

        # Assert
        assert "role" not in profile

    def test_build_connection_profile_with_invalid_key_file_raises_error(
        self,
        sample_snowflake_config,
        tmp_path,
        mocker,
    ):
        """Test that invalid private key file raises ValueError."""
        # Arrange - create a temp file that we'll delete to simulate missing file
        key_file = tmp_path / "temp_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")

        connection_config = SnowflakeConnectionConfig(
            account="test-account",
            user="test_user",
            private_key_file=str(key_file),
            private_key_password=None,
            warehouse="TEST_WH",
            database="TEST_DB",
            schema_name="TEST_SCHEMA",
            role="TEST_ROLE",
            pipe_name="TEST_PIPE",
        )

        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=connection_config,
        )

        # Mock file open to raise FileNotFoundError
        mocker.patch("pathlib.Path.open", side_effect=FileNotFoundError("File not found"))

        # Act & Assert
        with pytest.raises(ValueError, match="Cannot build Snowflake connection profile"):
            client._build_connection_profile()

    # ========================================================================
    # Client Lifecycle Tests (start/stop)
    # ========================================================================

    def test_start_initializes_client_successfully(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_snowflake_streaming_client,
        mock_logfire,
        mocker,
    ):
        """Test that start() initializes the streaming client successfully."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Mock private key handling
        mock_key_content = b"-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----"
        mock_private_key = mocker.MagicMock()
        mock_private_key.private_bytes.return_value = mock_key_content

        mocker.patch("pathlib.Path.open", mocker.mock_open(read_data=mock_key_content))
        mocker.patch(
            "cryptography.hazmat.primitives.serialization.load_pem_private_key",
            return_value=mock_private_key,
        )

        # Mock tempfile creation - return actual file descriptors
        mock_mkstemp = mocker.patch("tempfile.mkstemp")
        mock_mkstemp.side_effect = [
            (1, "/tmp/snowflake_key_test.pem"),    # First call for key file
            (2, "/tmp/snowflake_profile_test.json"),  # Second call for profile file
        ]

        # Mock file operations - use a real file-like object
        def mock_open_wrapper(fd_or_path, mode='r'):
            if isinstance(fd_or_path, int):
                # File descriptor - create a mock file object
                from io import StringIO
                return StringIO()
            else:
                # Regular path
                return mocker.mock_open()()

        mocker.patch("builtins.open", side_effect=mock_open_wrapper)
        mocker.patch("os.unlink")

        # Mock StreamingIngestClient to prevent real instantiation
        mock_streaming_client_class = mocker.patch(
            "src.streaming.snowflake_high_performance.StreamingIngestClient"
        )
        mock_streaming_client_class.return_value = mock_snowflake_streaming_client

        # Act
        client.start()

        # Assert
        assert client.is_started
        assert client.streaming_client is not None
        assert client.stats["client_created_at"] is not None
        mock_streaming_client_class.assert_called_once()

    def test_start_creates_temporary_files_and_cleans_up(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_snowflake_streaming_client,
        mock_logfire,
        mocker,
    ):
        """Test that start() creates and cleans up temporary files."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Mock private key handling
        mock_key_content = b"-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----"
        mock_private_key = mocker.MagicMock()
        mock_private_key.private_bytes.return_value = mock_key_content

        mocker.patch("pathlib.Path.open", mocker.mock_open(read_data=mock_key_content))
        mocker.patch(
            "cryptography.hazmat.primitives.serialization.load_pem_private_key",
            return_value=mock_private_key,
        )

        # Mock tempfile creation
        mock_mkstemp = mocker.patch("tempfile.mkstemp")
        mock_mkstemp.side_effect = [
            (1, "/tmp/snowflake_key_test2.pem"),
            (2, "/tmp/snowflake_profile_test2.json"),
        ]

        # Mock file operations
        def mock_open_wrapper(fd_or_path, mode='r'):
            if isinstance(fd_or_path, int):
                from io import StringIO
                return StringIO()
            else:
                return mocker.mock_open()()

        mocker.patch("builtins.open", side_effect=mock_open_wrapper)
        mock_unlink = mocker.patch("os.unlink")

        # Mock StreamingIngestClient
        mock_streaming_client_class = mocker.patch(
            "src.streaming.snowflake_high_performance.StreamingIngestClient"
        )
        mock_streaming_client_class.return_value = mock_snowflake_streaming_client

        # Act
        client.start()

        # Assert - verify temp files were created and cleaned up
        assert mock_mkstemp.call_count == 2
        assert mock_unlink.call_count == 2
        mock_unlink.assert_any_call("/tmp/snowflake_key_test2.pem")
        mock_unlink.assert_any_call("/tmp/snowflake_profile_test2.json")

    def test_start_with_error_calls_stop(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
        mocker,
    ):
        """Test that start() calls stop() if initialization fails."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Mock _build_connection_profile to raise an error
        mocker.patch.object(
            client,
            "_build_connection_profile",
            side_effect=ValueError("Connection error"),
        )

        # Mock stop method
        mock_stop = mocker.patch.object(client, "stop")

        # Act & Assert
        with pytest.raises(ValueError, match="Connection error"):
            client.start()

        # Verify stop was called
        mock_stop.assert_called_once()

    def test_stop_closes_all_channels(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that stop() closes all open channels."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Create mock channels
        mock_channel_1 = MagicMock()
        mock_channel_2 = MagicMock()
        client.channels = {
            "channel_1": mock_channel_1,
            "channel_2": mock_channel_2,
        }

        # Act
        client.stop()

        # Assert
        mock_channel_1.close.assert_called_once()
        mock_channel_2.close.assert_called_once()
        assert len(client.channels) == 0

    def test_stop_closes_streaming_client(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that stop() closes the streaming client."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Set up mock streaming client
        mock_client = MagicMock()
        client.streaming_client = mock_client

        # Act
        client.stop()

        # Assert
        mock_client.close.assert_called_once()
        assert client.streaming_client is None

    def test_stop_handles_channel_close_errors(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that stop() handles errors when closing channels gracefully."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Create mock channel that raises error on close
        mock_channel = MagicMock()
        mock_channel.close.side_effect = Exception("Close error")
        client.channels = {"channel_1": mock_channel}

        # Act - should not raise exception
        client.stop()

        # Assert
        assert len(client.channels) == 0

    # ========================================================================
    # Channel Management Tests
    # ========================================================================

    def test_get_or_create_channel_creates_new_channel(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
    ):
        """Test that _get_or_create_channel creates a new channel."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            client_name_suffix="test-123",
        )

        # Set up mock streaming client
        mock_client = MagicMock()
        mock_channel = MagicMock()
        mock_client.open_channel.return_value = (mock_channel, "OPEN")
        client.streaming_client = mock_client

        # Act
        channel = client._get_or_create_channel("partition_0")

        # Assert
        assert channel == mock_channel
        assert "TEST_TABLE_partition_partition_0_test-123" in client.channels
        assert client.stats["channels_created"] == 1
        mock_client.open_channel.assert_called_once()

    def test_get_or_create_channel_reuses_existing_channel(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
    ):
        """Test that _get_or_create_channel reuses existing channel."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            client_name_suffix="test-123",
        )

        # Set up mock streaming client and existing channel
        mock_client = MagicMock()
        mock_channel = MagicMock()
        channel_name = "TEST_TABLE_partition_partition_0_test-123"
        client.channels[channel_name] = mock_channel
        client.streaming_client = mock_client

        # Act
        channel = client._get_or_create_channel("partition_0")

        # Assert
        assert channel == mock_channel
        # Should not create a new channel
        mock_client.open_channel.assert_not_called()
        assert client.stats["channels_created"] == 0

    def test_get_or_create_channel_without_client_raises_error(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that _get_or_create_channel raises error if client not initialized."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Act & Assert
        with pytest.raises(RuntimeError, match="StreamingIngestClient not initialized"):
            client._get_or_create_channel("partition_0")

    def test_get_or_create_channel_handles_creation_error(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
    ):
        """Test that _get_or_create_channel handles channel creation errors."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Set up mock streaming client that fails
        mock_client = MagicMock()
        mock_client.open_channel.side_effect = Exception("Channel creation failed")
        client.streaming_client = mock_client

        # Act & Assert
        with pytest.raises(Exception, match="Channel creation failed"):
            client._get_or_create_channel("partition_0")

    # ========================================================================
    # Data Ingestion Tests
    # ========================================================================

    def test_ingest_batch_impl_ingests_successfully(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
        mocker,
    ):
        """Test that _ingest_batch_impl ingests data successfully."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Set up mock channel
        mock_channel = MagicMock()
        mock_channel.append_row.return_value = None
        mock_channel.get_latest_committed_offset_token.return_value = "offset_123"

        mocker.patch.object(client, "_get_or_create_channel", return_value=mock_channel)

        # Test data
        data_batch = [
            {"sequence_number": 1, "data": "row1"},
            {"sequence_number": 2, "data": "row2"},
            {"sequence_number": 3, "data": "row3"},
        ]

        # Act
        result = client._ingest_batch_impl("test_channel", data_batch, "partition_0")

        # Assert
        assert result is True
        assert mock_channel.append_row.call_count == 3
        assert client.stats["total_messages_sent"] == 3
        assert client.stats["total_batches_sent"] == 1
        assert client.stats["last_ingestion"] is not None

    def test_ingest_batch_impl_generates_unique_row_ids(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
        mocker,
    ):
        """Test that _ingest_batch_impl generates unique row IDs."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Set up mock channel
        mock_channel = MagicMock()
        mocker.patch.object(client, "_get_or_create_channel", return_value=mock_channel)

        # Test data
        data_batch = [
            {"sequence_number": 100, "data": "row1"},
            {"sequence_number": 101, "data": "row2"},
        ]

        # Act
        client._ingest_batch_impl("test_channel", data_batch, "partition_5")

        # Assert - verify row IDs use partition_id and sequence_number
        calls = mock_channel.append_row.call_args_list
        assert calls[0][0][1] == "partition_5_100"
        assert calls[1][0][1] == "partition_5_101"

    def test_ingest_batch_impl_with_empty_batch_returns_true(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
    ):
        """Test that _ingest_batch_impl handles empty batch correctly."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Act
        result = client._ingest_batch_impl("test_channel", [], "partition_0")

        # Assert
        assert result is True
        assert client.stats["total_messages_sent"] == 0
        assert client.stats["total_batches_sent"] == 0

    def test_ingest_batch_impl_with_channel_error_raises_exception(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
        mocker,
    ):
        """Test that _ingest_batch_impl raises exception on channel errors."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Set up mock channel that fails
        mock_channel = MagicMock()
        mock_channel.append_row.side_effect = Exception("Append failed")
        mocker.patch.object(client, "_get_or_create_channel", return_value=mock_channel)

        # Test data
        data_batch = [{"data": "row1"}]

        # Act & Assert
        with pytest.raises(Exception, match="Append failed"):
            client._ingest_batch_impl("test_channel", data_batch, "partition_0")

    def test_ingest_batch_impl_without_channel_raises_error(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
        mocker,
    ):
        """Test that _ingest_batch_impl raises error when channel is None."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Mock _get_or_create_channel to return None
        mocker.patch.object(client, "_get_or_create_channel", return_value=None)

        # Test data
        data_batch = [{"data": "row1"}]

        # Act & Assert
        with pytest.raises(RuntimeError, match="Failed to get channel"):
            client._ingest_batch_impl("test_channel", data_batch, "partition_0")

    def test_ingest_batch_calls_ingest_with_retry(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
        mocker,
    ):
        """Test that ingest_batch calls the retry-wrapped implementation."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Mock the retry-wrapped method
        mock_impl = mocker.patch.object(client, "_ingest_with_retry", return_value=True)

        # Test data
        data_batch = [{"data": "row1"}]

        # Act
        result = client.ingest_batch("test_channel", data_batch, "partition_0")

        # Assert
        assert result is True
        mock_impl.assert_called_once_with("test_channel", data_batch, "partition_0")

    def test_ingest_batch_with_retry_manager_retries_on_error(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
        mocker,
    ):
        """Test that ingest_batch retries on transient errors with retry manager."""
        # Arrange - Track how many times the underlying implementation is called
        attempt_count = [0]

        # Create client without retry manager first
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Mock successful channel operation
        mock_channel = MagicMock()
        mock_channel.append_row.return_value = None
        mock_channel.get_latest_committed_offset_token.return_value = "offset_123"
        mocker.patch.object(client, "_get_or_create_channel", return_value=mock_channel)

        # Store original implementation
        original_impl = client._ingest_batch_impl

        # Create a wrapper that fails once then succeeds
        def failing_impl(*args, **kwargs):
            attempt_count[0] += 1
            if attempt_count[0] < 2:
                raise Exception("Transient error")
            # On second attempt, succeed
            return original_impl(*args, **kwargs)

        # Now create a retry decorator that wraps the failing implementation
        def retry_decorator(func):
            """Simulates a retry decorator that retries on exception."""
            def wrapper(*args, **kwargs):
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        return func(*args, **kwargs)
                    except Exception:
                        if attempt < max_retries - 1:
                            continue  # Retry
                        else:
                            raise  # Out of retries
                return False
            return wrapper

        # Apply retry decorator to our failing implementation
        client._ingest_with_retry = retry_decorator(failing_impl)  # type: ignore[method-assign]

        # Test data
        data_batch = [{"data": "row1"}]

        # Act
        result = client.ingest_batch("test_channel", data_batch, "partition_0")

        # Assert - should succeed after retry
        assert result is True
        assert attempt_count[0] == 2  # Failed once, succeeded on second attempt
        # Verify the stats were updated (showing the successful ingestion)
        assert client.stats["total_messages_sent"] == 1
        assert client.stats["total_batches_sent"] == 1

    def test_ingest_batch_returns_false_on_permanent_failure(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mock_logfire,
        mocker,
    ):
        """Test that ingest_batch returns False on permanent failures."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Mock _ingest_with_retry to raise an exception
        mocker.patch.object(
            client,
            "_ingest_with_retry",
            side_effect=Exception("Permanent failure"),
        )

        # Test data
        data_batch = [{"data": "row1"}]

        # Act
        result = client.ingest_batch("test_channel", data_batch, "partition_0")

        # Assert
        assert result is False
        assert client.stats["retry_stats"]["failed_retries"] == 1

    # ========================================================================
    # Statistics Tests
    # ========================================================================

    def test_get_stats_returns_basic_stats(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that get_stats returns statistics correctly."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Set up some stats
        client.stats["client_created_at"] = datetime.now(UTC)
        client.stats["total_messages_sent"] = 100
        client.stats["total_batches_sent"] = 10

        # Act
        stats = client.get_stats()

        # Assert
        assert stats["total_messages_sent"] == 100
        assert stats["total_batches_sent"] == 10
        assert "runtime_seconds" in stats
        assert "messages_per_second" in stats

    def test_get_stats_calculates_messages_per_second(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mocker,
    ):
        """Test that get_stats calculates messages per second correctly."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Mock time: client created 10 seconds ago
        mock_now = datetime.now(UTC)
        mock_created = datetime.fromtimestamp(mock_now.timestamp() - 10, tz=UTC)

        client.stats["client_created_at"] = mock_created
        client.stats["total_messages_sent"] = 100

        # Act
        stats = client.get_stats()

        # Assert
        assert stats["runtime_seconds"] >= 9.0  # Allow for slight timing variation
        assert 9.0 <= stats["messages_per_second"] <= 11.0  # ~10 messages/sec

    def test_get_stats_without_created_at(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that get_stats handles missing created_at timestamp."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Act
        stats = client.get_stats()

        # Assert
        assert "runtime_seconds" not in stats
        assert "messages_per_second" not in stats

    # ========================================================================
    # Utility Method Tests
    # ========================================================================

    def test_create_channel_name_generates_correct_format(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that create_channel_name generates correct format."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            client_name_suffix="test-123",
        )

        # Act
        channel_name = client.create_channel_name(
            eventhub_name="my-hub",
            environment="prod",
            region="us-east",
        )

        # Assert
        assert channel_name == "my-hub-prod-us-east-test-123"

    def test_health_check_returns_status(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that health_check returns health status."""
        # Arrange
        client = SnowflakeHighPerformanceStreamingClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )

        # Add some channels
        client.channels = {"channel_1": MagicMock(), "channel_2": MagicMock()}

        # Act
        health = client.health_check()

        # Assert
        assert "client_status" in health
        assert "connection_active" in health
        assert health["channels_count"] == 2
        assert "errors" in health


class TestFactoryFunction:
    """Tests for create_snowflake_streaming_client factory function."""

    def test_factory_creates_client_instance(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that factory function creates client instance."""
        # Act
        client = create_snowflake_streaming_client(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            client_name_suffix="factory-test",
        )

        # Assert
        assert isinstance(client, SnowflakeHighPerformanceStreamingClient)
        assert client.client_name_suffix == "factory-test"
        assert client.snowflake_config == sample_snowflake_config
        assert client.connection_config == sample_snowflake_connection_config

    def test_factory_with_retry_manager(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mocker,
    ):
        """Test that factory function passes retry manager correctly."""
        # Arrange
        mock_retry_manager = mocker.MagicMock()
        mock_retry_manager.get_retry_decorator.return_value = lambda f: f

        # Act
        client = create_snowflake_streaming_client(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            retry_manager=mock_retry_manager,
        )

        # Assert
        assert client.retry_manager == mock_retry_manager
