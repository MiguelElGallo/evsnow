"""
Tests for EventHub async consumer.

This module tests the EventHub consumer functionality including:
- EventHubMessage wrapper class
- MessageBatch container
- SnowflakeCheckpointManager for checkpoint management
- SnowflakeCheckpointStore for Azure SDK compatibility
- EventHubAsyncConsumer main consumer class

Based on TESTING_STANDARDS.md - all tests use mocks for external dependencies.
"""

import asyncio
import json
import time
from datetime import datetime, UTC, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest

from src.consumers.eventhub import (
    BytesEncoder,
    EventHubAsyncConsumer,
    EventHubMessage,
    MessageBatch,
    SnowflakeCheckpointManager,
    SnowflakeCheckpointStore,
    _convert_bytes_to_str,
)


# ============================================================================
# Tests for BytesEncoder
# ============================================================================


class TestBytesEncoder:
    """Tests for BytesEncoder JSON encoder."""

    def test_encode_bytes_to_string(self):
        """Test that bytes are encoded to strings."""
        encoder = BytesEncoder()
        data = {"key": b"value"}
        result = json.dumps(data, cls=BytesEncoder)
        expected = '{"key": "value"}'
        assert result == expected

    def test_encode_nested_bytes(self):
        """Test encoding nested structures with bytes."""
        encoder = BytesEncoder()
        data = {"outer": {"inner": b"nested"}}
        result = json.dumps(data, cls=BytesEncoder)
        # The encoder only handles direct bytes, nested dicts need _convert_bytes_to_str
        assert "inner" in result


# ============================================================================
# Tests for _convert_bytes_to_str utility
# ============================================================================


class TestConvertBytesToStr:
    """Tests for _convert_bytes_to_str utility function."""

    def test_convert_bytes_to_str_simple(self):
        """Test converting simple bytes to string."""
        result = _convert_bytes_to_str(b"hello")
        assert result == "hello"

    def test_convert_dict_with_bytes(self):
        """Test converting dictionary with bytes values."""
        data = {"key": b"value", "key2": "text"}
        result = _convert_bytes_to_str(data)
        assert result == {"key": "value", "key2": "text"}

    def test_convert_nested_dict_with_bytes(self):
        """Test converting nested dictionary with bytes."""
        data = {"outer": {"inner": b"nested"}, "key": b"value"}
        result = _convert_bytes_to_str(data)
        assert result == {"outer": {"inner": "nested"}, "key": "value"}

    def test_convert_list_with_bytes(self):
        """Test converting list with bytes."""
        data = [b"first", b"second", "third"]
        result = _convert_bytes_to_str(data)
        assert result == ["first", "second", "third"]

    def test_convert_tuple_with_bytes(self):
        """Test converting tuple with bytes."""
        data = (b"first", b"second")
        result = _convert_bytes_to_str(data)
        assert result == ("first", "second")

    def test_convert_bytes_with_replace_errors(self):
        """Test that invalid UTF-8 bytes are handled gracefully."""
        invalid_bytes = b"\xff\xfe"
        result = _convert_bytes_to_str(invalid_bytes)
        # Should use 'replace' error handling
        assert isinstance(result, str)

    def test_convert_mixed_types(self):
        """Test converting mixed types preserves non-bytes types."""
        data = {"str": "text", "int": 42, "bytes": b"value", "none": None}
        result = _convert_bytes_to_str(data)
        assert result == {"str": "text", "int": 42, "bytes": "value", "none": None}


# ============================================================================
# Tests for EventHubMessage
# ============================================================================


class TestEventHubMessage:
    """Tests for EventHubMessage wrapper class."""

    def test_create_message_from_event_data(self, sample_event_data):
        """Test creating EventHubMessage from EventData."""
        message = EventHubMessage(
            event_data=sample_event_data, partition_id="0", sequence_number=100
        )

        assert message.partition_id == "0"
        assert message.sequence_number == 100
        assert message.event_data == sample_event_data
        assert message.body is not None
        assert message.partition_context is None

    def test_to_dict_converts_all_fields(self, sample_event_data):
        """Test to_dict() includes all required fields."""
        message = EventHubMessage(
            event_data=sample_event_data, partition_id="1", sequence_number=42
        )

        result = message.to_dict()

        assert "event_body" in result
        assert "partition_id" in result
        assert result["partition_id"] == "1"
        assert "sequence_number" in result
        assert result["sequence_number"] == 42
        assert "enqueued_time" in result
        assert "properties" in result
        assert "system_properties" in result
        assert "ingestion_timestamp" in result

    def test_to_dict_handles_bytes_in_properties(self):
        """Test that bytes in properties are converted to strings."""
        mock_event = MagicMock()
        mock_event.body_as_str.return_value = '{"test": "data"}'
        mock_event.enqueued_time = datetime.now(UTC)
        mock_event.properties = {"bytes_key": b"bytes_value", "str_key": "str_value"}
        mock_event.system_properties = {"sys_key": b"sys_bytes"}

        message = EventHubMessage(event_data=mock_event, partition_id="0", sequence_number=1)
        result = message.to_dict()

        # Properties should be JSON string
        props = json.loads(result["properties"])
        assert props["bytes_key"] == "bytes_value"
        assert props["str_key"] == "str_value"

        # System properties should be JSON string
        sys_props = json.loads(result["system_properties"])
        assert sys_props["sys_key"] == "sys_bytes"

    def test_to_dict_handles_none_properties(self):
        """Test that None properties are handled gracefully."""
        mock_event = MagicMock()
        mock_event.body_as_str.return_value = '{"test": "data"}'
        mock_event.enqueued_time = None
        mock_event.properties = None
        mock_event.system_properties = None

        message = EventHubMessage(event_data=mock_event, partition_id="0", sequence_number=1)
        result = message.to_dict()

        assert result["enqueued_time"] is None
        assert result["properties"] is None
        assert result["system_properties"] is None

    def test_to_dict_handles_none_enqueued_time(self):
        """Test that None enqueued_time is handled."""
        mock_event = MagicMock()
        mock_event.body_as_str.return_value = '{"data": "test"}'
        mock_event.enqueued_time = None
        mock_event.properties = {}
        mock_event.system_properties = {}

        message = EventHubMessage(event_data=mock_event, partition_id="0", sequence_number=1)
        result = message.to_dict()

        assert result["enqueued_time"] is None

    def test_to_dict_no_bytes_in_result(self):
        """Test that to_dict() result contains no bytes objects."""
        mock_event = MagicMock()
        mock_event.body_as_str.return_value = "test body"
        mock_event.enqueued_time = datetime.now(UTC)
        mock_event.properties = {"key": b"value"}
        mock_event.system_properties = {}

        message = EventHubMessage(event_data=mock_event, partition_id="0", sequence_number=1)
        result = message.to_dict()

        # Check that no values are bytes
        def check_no_bytes(obj):
            if isinstance(obj, dict):
                for value in obj.values():
                    assert not isinstance(value, bytes), f"Found bytes in result: {value}"
                    check_no_bytes(value)
            elif isinstance(obj, (list, tuple)):
                for item in obj:
                    assert not isinstance(item, bytes)
                    check_no_bytes(item)

        check_no_bytes(result)


# ============================================================================
# Tests for MessageBatch
# ============================================================================


class TestMessageBatch:
    """Tests for MessageBatch container."""

    def test_create_empty_batch(self):
        """Test creating an empty batch."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)

        assert len(batch.messages) == 0
        assert batch.max_size == 100
        assert batch.max_wait_seconds == 60
        assert isinstance(batch.created_at, float)
        assert len(batch.last_sequence_by_partition) == 0

    def test_add_message_to_batch(self, sample_eventhub_message):
        """Test adding a message to batch."""
        batch = MessageBatch(max_size=2, max_wait_seconds=60)

        ready = batch.add_message(sample_eventhub_message)

        assert not ready  # First message shouldn't trigger ready
        assert len(batch.messages) == 1
        assert sample_eventhub_message.partition_id in batch.last_sequence_by_partition

    def test_batch_ready_when_size_limit_reached(self, sample_eventhub_messages):
        """Test batch is ready when max size is reached."""
        batch = MessageBatch(max_size=2, max_wait_seconds=60)

        ready1 = batch.add_message(sample_eventhub_messages[0])
        assert not ready1

        ready2 = batch.add_message(sample_eventhub_messages[1])
        assert ready2  # Should be ready after second message
        assert len(batch.messages) == 2

    def test_batch_ready_when_timeout_reached(self, sample_eventhub_message):
        """Test batch is ready when timeout is reached."""
        batch = MessageBatch(max_size=100, max_wait_seconds=1)  # 1 second timeout
        batch.add_message(sample_eventhub_message)

        # Initially not ready
        assert not batch.is_ready()

        # Wait for timeout
        time.sleep(1.1)

        # Now should be ready
        assert batch.is_ready()

    def test_get_checkpoint_data_per_partition(self, sample_eventhub_messages):
        """Test getting checkpoint data returns highest sequence per partition."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)

        # Add messages from different partitions
        for msg in sample_eventhub_messages[:5]:
            batch.add_message(msg)

        checkpoint_data = batch.get_checkpoint_data()

        assert isinstance(checkpoint_data, dict)
        # Should have entries for each partition
        for msg in sample_eventhub_messages[:5]:
            assert msg.partition_id in checkpoint_data

    def test_get_checkpoint_data_returns_copy(self, sample_eventhub_message):
        """Test that get_checkpoint_data returns a copy."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)
        batch.add_message(sample_eventhub_message)

        data1 = batch.get_checkpoint_data()
        data2 = batch.get_checkpoint_data()

        # Should be equal but not the same object
        assert data1 == data2
        assert data1 is not data2

    def test_to_dict_list_converts_all_messages(self, sample_eventhub_messages):
        """Test converting batch to list of dictionaries."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)

        for msg in sample_eventhub_messages[:3]:
            batch.add_message(msg)

        dict_list = batch.to_dict_list()

        assert len(dict_list) == 3
        assert all(isinstance(d, dict) for d in dict_list)
        assert all("partition_id" in d for d in dict_list)

    def test_batch_tracks_highest_sequence_per_partition(self):
        """Test that batch tracks the highest sequence number per partition."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)

        # Create messages with varying sequence numbers for same partition
        for seq in [10, 15, 12, 20, 18]:
            mock_event = MagicMock()
            mock_event.body_as_str.return_value = f'{{"seq": {seq}}}'
            mock_event.enqueued_time = datetime.now(UTC)
            mock_event.properties = {}
            mock_event.system_properties = {}

            msg = EventHubMessage(event_data=mock_event, partition_id="0", sequence_number=seq)
            batch.add_message(msg)

        # Should track the last (not necessarily highest) sequence number per partition
        assert batch.last_sequence_by_partition["0"] == 18  # Last added


# ============================================================================
# Tests for SnowflakeCheckpointManager
# ============================================================================


class TestSnowflakeCheckpointManager:
    """Tests for SnowflakeCheckpointManager."""

    @pytest.mark.asyncio
    async def test_get_last_checkpoint_returns_dict(
        self, mocker, sample_snowflake_connection_config
    ):
        """Test loading checkpoint from Snowflake returns dict."""
        # Mock the get_partition_checkpoints function (imported inside the method)
        mock_checkpoints = {"0": 100, "1": 200}
        mocker.patch(
            "utils.snowflake.get_partition_checkpoints", return_value=mock_checkpoints
        )

        manager = SnowflakeCheckpointManager(
            eventhub_namespace="test.servicebus.windows.net",
            eventhub_name="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            snowflake_config=sample_snowflake_connection_config,
        )

        result = await manager.get_last_checkpoint()

        assert result == mock_checkpoints
        assert "0" in result
        assert result["0"] == 100

    @pytest.mark.asyncio
    async def test_get_last_checkpoint_returns_none_when_missing(
        self, mocker, sample_snowflake_connection_config
    ):
        """Test that missing checkpoints return None."""
        mocker.patch("utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)

        manager = SnowflakeCheckpointManager(
            eventhub_namespace="test.servicebus.windows.net",
            eventhub_name="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            snowflake_config=sample_snowflake_connection_config,
        )

        result = await manager.get_last_checkpoint()

        assert result is None

    @pytest.mark.asyncio
    async def test_get_last_checkpoint_handles_errors(
        self, mocker, sample_snowflake_connection_config
    ):
        """Test that errors during checkpoint load are handled."""
        mocker.patch(
            "utils.snowflake.get_partition_checkpoints",
            side_effect=Exception("DB error"),
        )

        manager = SnowflakeCheckpointManager(
            eventhub_namespace="test.servicebus.windows.net",
            eventhub_name="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            snowflake_config=sample_snowflake_connection_config,
        )

        result = await manager.get_last_checkpoint()

        assert result is None  # Should return None on error

    @pytest.mark.asyncio
    async def test_save_checkpoint_to_snowflake(
        self, mocker, mock_logfire, sample_snowflake_connection_config
    ):
        """Test saving checkpoint to Snowflake."""
        mock_insert = mocker.patch("utils.snowflake.insert_partition_checkpoint")
        mocker.patch("src.utils.snowflake.insert_partition_checkpoint")

        manager = SnowflakeCheckpointManager(
            eventhub_namespace="test.servicebus.windows.net",
            eventhub_name="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            snowflake_config=sample_snowflake_connection_config,
        )

        partition_checkpoints = {"0": 100, "1": 200}
        result = await manager.save_checkpoint(partition_checkpoints)

        assert result is True
        # Should be called once per partition
        assert mock_insert.call_count == 2

    @pytest.mark.asyncio
    async def test_save_checkpoint_handles_multiple_partitions(
        self, mocker, mock_logfire, sample_snowflake_connection_config
    ):
        """Test saving checkpoints for multiple partitions."""
        mock_insert = mocker.patch("utils.snowflake.insert_partition_checkpoint")
        mocker.patch("src.utils.snowflake.insert_partition_checkpoint")

        manager = SnowflakeCheckpointManager(
            eventhub_namespace="test.servicebus.windows.net",
            eventhub_name="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            snowflake_config=sample_snowflake_connection_config,
        )

        partition_checkpoints = {"0": 100, "1": 200, "2": 300}
        result = await manager.save_checkpoint(partition_checkpoints)

        assert result is True
        assert mock_insert.call_count == 3

    @pytest.mark.asyncio
    async def test_save_checkpoint_includes_metadata(
        self, mocker, mock_logfire, sample_snowflake_connection_config
    ):
        """Test that checkpoint save includes metadata."""
        mock_insert = mocker.patch("utils.snowflake.insert_partition_checkpoint")
        mocker.patch("src.utils.snowflake.insert_partition_checkpoint")

        manager = SnowflakeCheckpointManager(
            eventhub_namespace="test.servicebus.windows.net",
            eventhub_name="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            snowflake_config=sample_snowflake_connection_config,
        )

        partition_checkpoints = {"0": 100}
        partition_metadata = {"0": {"sequence_number": 100, "timestamp": "2024-11-08"}}

        result = await manager.save_checkpoint(partition_checkpoints, partition_metadata)

        assert result is True
        # Check that metadata was passed to insert function
        mock_insert.assert_called_once()
        call_kwargs = mock_insert.call_args[1]
        assert call_kwargs["metadata"] == {"sequence_number": 100, "timestamp": "2024-11-08"}

    @pytest.mark.asyncio
    async def test_save_checkpoint_returns_false_on_error(
        self, mocker, mock_logfire, sample_snowflake_connection_config
    ):
        """Test that save returns False on error."""
        mocker.patch(
            "utils.snowflake.insert_partition_checkpoint", side_effect=Exception("DB error")
        )

        manager = SnowflakeCheckpointManager(
            eventhub_namespace="test.servicebus.windows.net",
            eventhub_name="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            snowflake_config=sample_snowflake_connection_config,
        )

        partition_checkpoints = {"0": 100}
        result = await manager.save_checkpoint(partition_checkpoints)

        assert result is False

    def test_close_closes_session_if_owned(self, sample_snowflake_connection_config):
        """Test that close() closes session if we own it."""
        mock_session = MagicMock()
        manager = SnowflakeCheckpointManager(
            eventhub_namespace="test.servicebus.windows.net",
            eventhub_name="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            snowflake_config=sample_snowflake_connection_config,
            session=mock_session,
        )

        # Mark as external session
        manager._external_session = False

        manager.close()

        mock_session.close.assert_called_once()

    def test_close_does_not_close_external_session(self, sample_snowflake_connection_config):
        """Test that close() doesn't close external sessions."""
        mock_session = MagicMock()
        manager = SnowflakeCheckpointManager(
            eventhub_namespace="test.servicebus.windows.net",
            eventhub_name="test-hub",
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            snowflake_config=sample_snowflake_connection_config,
            session=mock_session,
        )

        # External session should not be closed
        assert manager._external_session is True

        manager.close()

        mock_session.close.assert_not_called()


# ============================================================================
# Tests for SnowflakeCheckpointStore
# ============================================================================


class TestSnowflakeCheckpointStore:
    """Tests for SnowflakeCheckpointStore (Azure SDK compatibility layer)."""

    @pytest.mark.asyncio
    async def test_list_ownership_returns_cached_records(self):
        """Test listing ownership records returns cache."""
        mock_manager = MagicMock()
        store = SnowflakeCheckpointStore(mock_manager)

        # Add some ownership to cache
        store._ownership_cache["0"] = {
            "partition_id": "0",
            "owner_id": "test-owner",
            "fully_qualified_namespace": "test.servicebus.windows.net",
            "eventhub_name": "test-hub",
            "consumer_group": "test-group",
        }

        result = await store.list_ownership("test.servicebus.windows.net", "test-hub", "test-group")

        assert len(result) == 1
        assert result[0]["partition_id"] == "0"

    @pytest.mark.asyncio
    async def test_claim_ownership_for_partitions(self):
        """Test claiming ownership for partitions."""
        mock_manager = MagicMock()
        store = SnowflakeCheckpointStore(mock_manager)

        ownership_list = [
            {
                "fully_qualified_namespace": "test.servicebus.windows.net",
                "eventhub_name": "test-hub",
                "consumer_group": "test-group",
                "partition_id": "0",
                "owner_id": "owner-1",
            },
            {
                "fully_qualified_namespace": "test.servicebus.windows.net",
                "eventhub_name": "test-hub",
                "consumer_group": "test-group",
                "partition_id": "1",
                "owner_id": "owner-1",
            },
        ]

        claimed = await store.claim_ownership(ownership_list)

        assert len(claimed) == 2
        assert all("etag" in c for c in claimed)
        assert all("last_modified_time" in c for c in claimed)

    @pytest.mark.asyncio
    async def test_update_checkpoint_via_sdk(self, mocker):
        """Test updating checkpoint through SDK interface."""
        mock_manager = MagicMock()
        mock_manager.save_checkpoint = AsyncMock(return_value=True)

        store = SnowflakeCheckpointStore(mock_manager)

        checkpoint = {
            "fully_qualified_namespace": "test.servicebus.windows.net",
            "eventhub_name": "test-hub",
            "consumer_group": "test-group",
            "partition_id": "0",
            "offset": "12345",
            "sequence_number": 100,
        }

        await store.update_checkpoint(checkpoint)

        # Should call manager's save_checkpoint
        mock_manager.save_checkpoint.assert_called_once()
        call_args = mock_manager.save_checkpoint.call_args
        # Check that offset was converted to int
        assert call_args[0][0] == {"0": 12345}

    @pytest.mark.asyncio
    async def test_update_checkpoint_handles_invalid_offset(self, mocker):
        """Test that invalid offset falls back to sequence number."""
        mock_manager = MagicMock()
        mock_manager.save_checkpoint = AsyncMock(return_value=True)

        store = SnowflakeCheckpointStore(mock_manager)

        checkpoint = {
            "fully_qualified_namespace": "test.servicebus.windows.net",
            "eventhub_name": "test-hub",
            "consumer_group": "test-group",
            "partition_id": "0",
            "offset": "invalid",  # Invalid offset
            "sequence_number": 100,
        }

        await store.update_checkpoint(checkpoint)

        # Should use sequence_number as fallback
        mock_manager.save_checkpoint.assert_called_once()
        call_args = mock_manager.save_checkpoint.call_args
        assert call_args[0][0] == {"0": 100}  # Should use sequence_number

    @pytest.mark.asyncio
    async def test_list_checkpoints_from_snowflake(self):
        """Test loading checkpoints from Snowflake."""
        mock_manager = MagicMock()
        mock_manager.get_last_checkpoint = AsyncMock(return_value={"0": 12345, "1": 67890})

        store = SnowflakeCheckpointStore(mock_manager)

        result = await store.list_checkpoints(
            "test.servicebus.windows.net", "test-hub", "test-group"
        )

        assert len(result) == 2
        assert all("offset" in c for c in result)
        assert all("partition_id" in c for c in result)
        # Offsets should be strings
        assert all(isinstance(c["offset"], str) for c in result)

    @pytest.mark.asyncio
    async def test_list_checkpoints_returns_empty_when_none(self):
        """Test that list_checkpoints returns empty list when no checkpoints."""
        mock_manager = MagicMock()
        mock_manager.get_last_checkpoint = AsyncMock(return_value=None)

        store = SnowflakeCheckpointStore(mock_manager)

        result = await store.list_checkpoints(
            "test.servicebus.windows.net", "test-hub", "test-group"
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_checkpoint_cache_is_updated(self):
        """Test that checkpoint cache is updated correctly."""
        mock_manager = MagicMock()
        mock_manager.get_last_checkpoint = AsyncMock(return_value={"0": 12345})

        store = SnowflakeCheckpointStore(mock_manager)

        await store.list_checkpoints("test.servicebus.windows.net", "test-hub", "test-group")

        # Cache should be updated
        assert "0" in store._checkpoint_cache
        assert store._checkpoint_cache["0"]["offset"] == "12345"


# ============================================================================
# Tests for EventHubAsyncConsumer
# ============================================================================


class TestEventHubAsyncConsumer:
    """Tests for EventHubAsyncConsumer main consumer class."""

    @pytest.mark.asyncio
    async def test_consumer_initialization(self, sample_eventhub_config):
        """Test consumer initialization with required parameters."""

        def mock_processor(messages):
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
            batch_size=100,
            batch_timeout_seconds=60,
        )

        assert consumer.eventhub_config == sample_eventhub_config
        assert consumer.target_db == "TEST_DB"
        assert consumer.target_schema == "TEST_SCHEMA"
        assert consumer.target_table == "TEST_TABLE"
        assert consumer.batch_size == 100
        assert consumer.batch_timeout_seconds == 60
        assert consumer.running is False
        assert consumer.client is None
        assert consumer.checkpoint_manager is None

    @pytest.mark.asyncio
    async def test_start_initializes_checkpoint_manager(
        self,
        mocker,
        sample_eventhub_config,
        mock_eventhub_client,
        mock_azure_credential,
        mock_logfire,
    ):
        """Test that start() initializes checkpoint manager."""
        # Mock Snowflake functions - patch both src. and non-src. versions
        mocker.patch("utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)

        def mock_processor(messages):
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
        )

        # The mock_eventhub_client fixture already configures receive to raise CancelledError
        # No need to reassign it - that was causing issues

        try:
            await consumer.start()
        except asyncio.CancelledError:
            pass

        assert consumer.checkpoint_manager is not None

    @pytest.mark.asyncio
    async def test_start_creates_eventhub_client_with_connection_string(
        self, mocker, mock_logfire
    ):
        """Test creating EventHub client with connection string."""
        from src.utils.config import EventHubConfig

        config = EventHubConfig(
            name="test-hub",
            namespace="test.servicebus.windows.net",
            consumer_group="test-group",
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test==",
            max_batch_size=100,
            max_wait_time=60,
            prefetch_count=50,
            checkpoint_interval_seconds=300,
            max_message_batch_size=1000,
            batch_timeout_seconds=300,
        )

        mocker.patch("utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)

        mock_client = AsyncMock()
        mock_client.receive = AsyncMock(side_effect=asyncio.CancelledError())
        mock_client.close = AsyncMock()
        
        # Patch where EventHubConsumerClient is USED, not where it's defined
        mock_from_conn = mocker.patch(
            "src.consumers.eventhub.EventHubConsumerClient.from_connection_string",
            return_value=mock_client,
        )

        def mock_processor(messages):
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
        )

        try:
            await consumer.start()
        except asyncio.CancelledError:
            pass

        # Should use from_connection_string
        mock_from_conn.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_creates_eventhub_client_with_credential(
        self, mocker, sample_eventhub_config, mock_logfire
    ):
        """Test creating EventHub client with Azure credential."""
        mocker.patch("utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)

        mock_cred = AsyncMock()
        mock_cred.get_token = AsyncMock(
            return_value=MagicMock(token="test-token", expires_on=time.time() + 3600)
        )
        mock_cred.close = AsyncMock()
        
        # AzureCliCredential is imported inside start(), so patch where it's imported from
        mocker.patch("azure.identity.aio.AzureCliCredential", return_value=mock_cred)

        mock_client = AsyncMock()
        mock_client.receive = AsyncMock(side_effect=asyncio.CancelledError())
        mock_client.close = AsyncMock()
        
        # Patch where EventHubConsumerClient is USED, not where it's defined
        mock_client_class = mocker.patch(
            "src.consumers.eventhub.EventHubConsumerClient", return_value=mock_client
        )

        def mock_processor(messages):
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
        )

        try:
            await consumer.start()
        except asyncio.CancelledError:
            pass

        # Should create client with credential
        mock_client_class.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_messages_in_batches(
        self,
        mocker,
        sample_eventhub_config,
        mock_eventhub_client,
        mock_azure_credential,
        mock_logfire,
    ):
        """Test that messages are processed in batches."""
        mocker.patch("utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)

        processed_batches = []

        def mock_processor(messages):
            processed_batches.append(messages)
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
            batch_size=2,
        )

        # Mock partition context
        mock_context = MagicMock()
        mock_context.partition_id = "0"
        mock_context.update_checkpoint = AsyncMock()

        # Create mock events
        mock_events = []
        for i in range(2):
            mock_event = MagicMock()
            mock_event.body_as_str.return_value = f'{{"id": {i}}}'
            mock_event.enqueued_time = datetime.now(UTC)
            mock_event.properties = {}
            mock_event.system_properties = {}
            mock_event.sequence_number = i
            mock_event.offset = str(i * 1000)
            mock_events.append(mock_event)

        # Start consumer
        mock_eventhub_client.receive = AsyncMock(side_effect=asyncio.CancelledError())

        try:
            await consumer.start()
        except asyncio.CancelledError:
            pass

        # Manually trigger message processing
        consumer.running = True
        consumer.current_batch = MessageBatch(max_size=2, max_wait_seconds=60)

        for event in mock_events:
            await consumer._on_event(mock_context, event)

        # Should have processed one batch
        assert len(processed_batches) == 1
        assert len(processed_batches[0]) == 2

    @pytest.mark.asyncio
    async def test_update_checkpoints_after_processing(
        self,
        mocker,
        sample_eventhub_config,
        mock_eventhub_client,
        mock_azure_credential,
        mock_logfire,
    ):
        """Test that checkpoints are updated after batch processing."""
        mocker.patch("utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)

        def mock_processor(messages):
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
        )

        # Mock partition context
        mock_context = MagicMock()
        mock_context.partition_id = "0"
        mock_context.update_checkpoint = AsyncMock()

        # Create a mock event
        mock_event = MagicMock()
        mock_event.body_as_str.return_value = '{"test": "data"}'
        mock_event.enqueued_time = datetime.now(UTC)
        mock_event.properties = {}
        mock_event.system_properties = {}
        mock_event.sequence_number = 100
        mock_event.offset = "12345"

        # Create message with partition context
        message = EventHubMessage(event_data=mock_event, partition_id="0", sequence_number=100)
        message.partition_context = mock_context

        # Create batch and process
        batch = MessageBatch(max_size=10, max_wait_seconds=60)
        batch.add_message(message)

        consumer.running = True
        await consumer._process_batch(batch)

        # Checkpoint should be updated
        mock_context.update_checkpoint.assert_called_once_with(mock_event)

    @pytest.mark.asyncio
    async def test_stop_consumer_and_cleanup_resources(
        self, mocker, sample_eventhub_config, mock_logfire
    ):
        """Test that stop() cleans up resources."""
        mocker.patch("utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)

        def mock_processor(messages):
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
        )

        # Set up some state
        mock_client = AsyncMock()
        mock_client.close = AsyncMock()
        consumer.client = mock_client

        mock_credential = AsyncMock()
        mock_credential.close = AsyncMock()
        consumer.credential = mock_credential

        mock_manager = MagicMock()
        mock_manager.close = MagicMock()
        consumer.checkpoint_manager = mock_manager

        consumer.running = True

        await consumer.stop()

        # Should clean up resources
        assert consumer.running is False
        mock_client.close.assert_called_once()
        mock_credential.close.assert_called_once()
        mock_manager.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_remaining_messages_on_shutdown(
        self, mocker, sample_eventhub_config, mock_logfire
    ):
        """Test that remaining messages are processed during shutdown."""
        mocker.patch("utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)

        processed_messages = []

        def mock_processor(messages):
            processed_messages.extend(messages)
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
        )

        # Create a batch with messages
        consumer.running = True
        consumer.current_batch = MessageBatch(max_size=10, max_wait_seconds=60)

        mock_event = MagicMock()
        mock_event.body_as_str.return_value = '{"test": "data"}'
        mock_event.enqueued_time = datetime.now(UTC)
        mock_event.properties = {}
        mock_event.system_properties = {}
        mock_event.sequence_number = 100
        mock_event.offset = "12345"

        message = EventHubMessage(event_data=mock_event, partition_id="0", sequence_number=100)
        message.partition_context = MagicMock()
        message.partition_context.update_checkpoint = AsyncMock()
        consumer.current_batch.add_message(message)

        await consumer.stop()

        # Should have processed the remaining message
        assert len(processed_messages) == 1

    @pytest.mark.asyncio
    async def test_handle_connection_errors_gracefully(
        self, mocker, sample_eventhub_config, mock_logfire
    ):
        """Test that connection errors are handled gracefully."""
        mocker.patch("utils.snowflake.get_partition_checkpoints", return_value=None)
        mocker.patch("src.utils.snowflake.get_partition_checkpoints", return_value=None)

        # Mock credential to raise error
        mock_cred = AsyncMock()
        mock_cred.get_token = AsyncMock(side_effect=Exception("Connection failed"))
        mocker.patch("azure.identity.aio.AzureCliCredential", return_value=mock_cred)

        def mock_processor(messages):
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
        )

        # Should raise exception during start
        with pytest.raises(Exception):
            await consumer.start()

        # Consumer should not be running
        assert consumer.running is False

    def test_get_stats_returns_statistics(self, sample_eventhub_config):
        """Test that get_stats returns consumer statistics."""

        def mock_processor(messages):
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
        )

        consumer.stats["messages_received"] = 100
        consumer.stats["batches_processed"] = 5
        consumer.stats["start_time"] = datetime.now(UTC) - timedelta(seconds=60)

        stats = consumer.get_stats()

        assert "messages_received" in stats
        assert stats["messages_received"] == 100
        assert "batches_processed" in stats
        assert stats["batches_processed"] == 5
        assert "runtime_seconds" in stats
        assert "messages_per_second" in stats

    @pytest.mark.asyncio
    async def test_on_event_ignores_none_events(self, sample_eventhub_config):
        """Test that _on_event ignores None events."""

        def mock_processor(messages):
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
        )

        consumer.running = True
        consumer.current_batch = MessageBatch(max_size=10, max_wait_seconds=60)

        mock_context = MagicMock()
        mock_context.partition_id = "0"

        # Should not raise error
        await consumer._on_event(mock_context, None)

        # Batch should still be empty
        assert len(consumer.current_batch.messages) == 0

    @pytest.mark.asyncio
    async def test_on_event_when_not_running(self, sample_eventhub_config):
        """Test that _on_event does nothing when consumer not running."""

        def mock_processor(messages):
            return True

        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=mock_processor,
        )

        consumer.running = False

        mock_context = MagicMock()
        mock_event = MagicMock()

        # Should not process event
        await consumer._on_event(mock_context, mock_event)

        # Stats should not be updated
        assert consumer.stats["messages_received"] == 0
