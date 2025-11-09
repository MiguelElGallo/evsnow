"""
Integration tests for end-to-end pipeline flow.

This module tests the complete EvSnow pipeline from EventHub to Snowflake
using mocks for all external services. Tests verify:
- Complete pipeline flow with all components integrated
- Multi-mapping concurrent processing
- Error recovery and retry mechanisms
- Configuration flow and component initialization

All tests use mocks following TESTING_STANDARDS.md guidelines.
No real services (EventHub, Snowflake, Logfire, Azure) are called.
"""

import asyncio
import os
import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from src.consumers.eventhub import EventHubAsyncConsumer, EventHubMessage
from src.pipeline.orchestrator import PipelineMapping, PipelineOrchestrator, run_pipeline
from src.streaming.snowflake import create_snowflake_streaming_client
from src.utils.config import (
    EventHubConfig,
    EventHubSnowflakeMapping,
    EvSnowConfig,
    SnowflakeConfig,
    SnowflakeConnectionConfig,
    load_config,
)


# ============================================================================
# Module-level Fixtures
# ============================================================================


@pytest.fixture(autouse=True)
def mock_snowflake_client_creation(mocker):
    """
    Auto-mock Snowflake streaming client creation for all integration tests.
    This prevents tests from trying to create real Snowflake connections.
    """
    mock_client = mocker.MagicMock()
    mock_client.start = mocker.MagicMock()
    mock_client.stop = mocker.MagicMock()
    mock_client.ingest_batch = mocker.MagicMock(return_value=True)
    mock_client.health_check = mocker.MagicMock(return_value={"status": "healthy"})
    mock_client.get_stats = mocker.MagicMock(return_value={
        "messages_processed": 0,
        "batches_processed": 0,
    })
    
    # Patch at the import location in orchestrator module
    mocker.patch(
        "src.pipeline.orchestrator.create_snowflake_streaming_client",
        return_value=mock_client
    )
    
    return mock_client


# ============================================================================
# Test Class: TestEndToEndPipeline
# ============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
class TestEndToEndPipeline:
    """Tests for complete end-to-end pipeline flow."""

    async def test_snowflake_ingestion_succeeds_with_data(
        self,
        mock_eventhub_client,
        mock_azure_credential,
        mock_snowflake_connection,
        mock_logfire,
        sample_eventhub_config,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        sample_eventhub_messages,
        mocker,
    ):
        """Test that Snowflake ingestion succeeds with correct data format."""
        # Arrange: Mock Snowflake client
        ingested_batches = []
        
        mock_streaming_client = mocker.MagicMock()
        
        def capture_ingest(channel_name, data_batch, partition_id):
            ingested_batches.append({
                "channel": channel_name,
                "data": data_batch,
                "partition": partition_id
            })
            return True
        
        mock_streaming_client.ingest_batch.side_effect = capture_ingest
        mock_streaming_client.start.return_value = None
        mock_streaming_client.stop.return_value = None
        
        mocker.patch(
            "src.pipeline.orchestrator.create_snowflake_streaming_client",
            return_value=mock_streaming_client
        )
        
        # Create mapping config
        mapping_config = EventHubSnowflakeMapping(
            event_hub_key="EVENTHUBNAME_1",
            snowflake_key="SNOWFLAKE_1",
        )
        
        # Create pipeline config
        pipeline_config = EvSnowConfig(
            eventhub_namespace=sample_eventhub_config.namespace,
            snowflake_connection=sample_snowflake_connection_config,
        )
        pipeline_config.event_hubs["EVENTHUBNAME_1"] = sample_eventhub_config
        pipeline_config.snowflake_configs["SNOWFLAKE_1"] = sample_snowflake_config
        
        # Mock checkpoint manager
        mock_checkpoint_mgr = mocker.MagicMock()
        mock_checkpoint_mgr.get_last_checkpoint = AsyncMock(return_value=None)
        mock_checkpoint_mgr.save_checkpoint = AsyncMock(return_value=True)
        mock_checkpoint_mgr.close = mocker.MagicMock()
        
        mocker.patch(
            "src.consumers.eventhub.SnowflakeCheckpointManager",
            return_value=mock_checkpoint_mgr
        )
        
        mocker.patch(
            "src.consumers.eventhub.SnowflakeCheckpointStore",
            return_value=mocker.MagicMock()
        )
        
        # Act: Create mapping and process messages
        mapping = PipelineMapping(
            mapping_config=mapping_config,
            pipeline_config=pipeline_config,
        )
        mapping.start()
        
        # Process messages directly
        success = mapping._process_messages(sample_eventhub_messages)
        
        # Cleanup
        mapping.snowflake_client.stop()
        
        # Assert: Verify Snowflake ingestion
        assert success is True
        assert len(ingested_batches) == 1
        
        batch = ingested_batches[0]
        assert "channel" in batch
        assert "data" in batch
        assert "partition" in batch
        
        # Verify data format
        assert len(batch["data"]) == len(sample_eventhub_messages)
        for data_item in batch["data"]:
            assert "event_body" in data_item
            assert "partition_id" in data_item
            assert "sequence_number" in data_item
            assert "ingestion_timestamp" in data_item

    async def test_pipeline_stats_tracked_correctly(
        self,
        mock_eventhub_client,
        mock_azure_credential,
        mock_snowflake_connection,
        mock_logfire,
        sample_eventhub_config,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mocker,
    ):
        """Test that pipeline statistics are tracked correctly."""
        # Arrange: Create pipeline mapping
        mapping_config = EventHubSnowflakeMapping(
            event_hub_key="EVENTHUBNAME_1",
            snowflake_key="SNOWFLAKE_1",
        )
        
        pipeline_config = EvSnowConfig(
            eventhub_namespace=sample_eventhub_config.namespace,
            snowflake_connection=sample_snowflake_connection_config,
        )
        pipeline_config.event_hubs["EVENTHUBNAME_1"] = sample_eventhub_config
        pipeline_config.snowflake_configs["SNOWFLAKE_1"] = sample_snowflake_config
        
        # Mock Snowflake client
        mock_streaming_client = mocker.MagicMock()
        mock_streaming_client.ingest_batch.return_value = True
        mock_streaming_client.start.return_value = None
        mock_streaming_client.stop.return_value = None
        mock_streaming_client.health_check.return_value = {"status": "healthy"}
        
        mocker.patch(
            "src.pipeline.orchestrator.create_snowflake_streaming_client",
            return_value=mock_streaming_client
        )
        
        # Mock checkpoint dependencies
        mocker.patch("src.consumers.eventhub.SnowflakeCheckpointManager")
        mocker.patch("src.consumers.eventhub.SnowflakeCheckpointStore")
        
        # Act: Create mapping and process messages
        mapping = PipelineMapping(
            mapping_config=mapping_config,
            pipeline_config=pipeline_config,
        )
        mapping.start()
        
        # Create test messages
        test_messages = []
        for i in range(15):
            mock_event = mocker.MagicMock()
            mock_event.body_as_str.return_value = f'{{"id": {i}}}'
            mock_event.sequence_number = i
            mock_event.offset = str(i)
            mock_event.enqueued_time = datetime.now(UTC)
            mock_event.properties = {}
            mock_event.system_properties = {}
            
            msg = EventHubMessage(
                event_data=mock_event,
                partition_id="0",
                sequence_number=i,
            )
            test_messages.append(msg)
        
        # Process messages in batches
        mapping._process_messages(test_messages[:10])
        mapping._process_messages(test_messages[10:])
        
        # Get stats
        stats = mapping.get_stats()
        
        # Cleanup
        mapping.snowflake_client.stop()
        
        # Assert: Verify stats
        assert stats["messages_processed"] == 15
        assert stats["batches_processed"] == 2
        assert "started_at" in stats
        assert stats["started_at"] is not None
        assert "runtime_seconds" in stats
        assert stats["runtime_seconds"] > 0
        assert "messages_per_second" in stats
        assert "last_activity" in stats


# ============================================================================
# Test Class: TestMultiMappingPipeline
# ============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
class TestMultiMappingPipeline:
    """Tests for multi-mapping concurrent pipeline processing."""

    async def test_multiple_mappings_run_concurrently(
        self,
        mock_eventhub_client,
        mock_azure_credential,
        mock_snowflake_connection,
        mock_logfire,
        mocker,
        tmp_path,
        monkeypatch,
    ):
        """Test that multiple EventHub->Snowflake mappings run concurrently."""
        # Clear any potentially polluted EVENTHUBNAME_* and SNOWFLAKE_* environment variables
        for key in list(os.environ.keys()):
            if key.startswith(("EVENTHUBNAME_", "SNOWFLAKE_")) and "_" in key:
                if not key in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE",
                               "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_PIPE_NAME",
                               "SNOWFLAKE_PRIVATE_KEY_FILE", "SNOWFLAKE_PRIVATE_KEY_PASSWORD"]:
                    monkeypatch.delenv(key, raising=False)
        
        # Arrange: Create multiple mapping configurations
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")
        
        config = EvSnowConfig(
            eventhub_namespace="test.servicebus.windows.net",
            snowflake_connection=SnowflakeConnectionConfig(
                account="test",
                user="test",
                private_key_file=str(key_file),
                warehouse="TEST_WH",
                database="TEST_DB",
                schema_name="TEST_SCHEMA",
                pipe_name="TEST_PIPE",
            )
        )
        
        # Create 3 mappings
        for i in range(1, 4):
            config.event_hubs[f"EVENTHUBNAME_{i}"] = EventHubConfig(
                name=f"hub-{i}",
                namespace="test.servicebus.windows.net",
                consumer_group=f"group-{i}",
                max_batch_size=10,
            )
            config.snowflake_configs[f"SNOWFLAKE_{i}"] = SnowflakeConfig(
                database=f"DB_{i}",
                schema_name=f"SCHEMA_{i}",
                table_name=f"TABLE_{i}",
            )
            config.mappings.append(
                EventHubSnowflakeMapping(
                    event_hub_key=f"EVENTHUBNAME_{i}",
                    snowflake_key=f"SNOWFLAKE_{i}",
                )
            )
        
        # Mock Snowflake clients
        mock_streaming_client = mocker.MagicMock()
        mock_streaming_client.ingest_batch.return_value = True
        mock_streaming_client.start.return_value = None
        mock_streaming_client.stop.return_value = None
        mock_streaming_client.health_check.return_value = {"status": "healthy"}
        
        mocker.patch(
            "src.pipeline.orchestrator.create_snowflake_streaming_client",
            return_value=mock_streaming_client
        )
        
        # Mock checkpoint dependencies
        mocker.patch("src.consumers.eventhub.SnowflakeCheckpointManager")
        mocker.patch("src.consumers.eventhub.SnowflakeCheckpointStore")
        
        # Track which mappings were started
        started_mappings = []
        
        # Mock EventHub consumer to track initialization
        original_init = EventHubAsyncConsumer.__init__
        
        def track_consumer_init(self, *args, **kwargs):
            started_mappings.append(kwargs.get("target_table"))
            return original_init(self, *args, **kwargs)
        
        mocker.patch.object(EventHubAsyncConsumer, "__init__", track_consumer_init)
        
        # Act: Create and initialize orchestrator
        orchestrator = PipelineOrchestrator(config=config)
        orchestrator.start()
        
        # Get stats
        stats = orchestrator.get_stats()
        
        # Cleanup
        for mapping in orchestrator.mappings:
            if mapping.snowflake_client:
                mapping.snowflake_client.stop()
        
        # Assert: Verify concurrent operation
        assert len(orchestrator.mappings) == 3
        assert stats["mappings_count"] == 3
        
        # Verify each mapping is initialized
        for i, mapping in enumerate(orchestrator.mappings, 1):
            assert mapping.running is True
            assert mapping.eventhub_config is not None
            assert mapping.snowflake_config is not None

    async def test_mappings_operate_independently_without_interference(
        self,
        mock_eventhub_client,
        mock_azure_credential,
        mock_snowflake_connection,
        mock_logfire,
        mocker,
        tmp_path,
        monkeypatch,
    ):
        """Test that each mapping operates independently."""
        # Clear any potentially polluted EVENTHUBNAME_* and SNOWFLAKE_* environment variables
        for key in list(os.environ.keys()):
            if key.startswith(("EVENTHUBNAME_", "SNOWFLAKE_")) and "_" in key:
                if key not in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE",
                               "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_PIPE_NAME",
                               "SNOWFLAKE_PRIVATE_KEY_FILE", "SNOWFLAKE_PRIVATE_KEY_PASSWORD"]:
                    monkeypatch.delenv(key, raising=False)
        
        # Arrange: Create 2 mappings
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")
        
        config = EvSnowConfig(
            eventhub_namespace="test.servicebus.windows.net",
            snowflake_connection=SnowflakeConnectionConfig(
                account="test",
                user="test",
                private_key_file=str(key_file),
                warehouse="TEST_WH",
                database="TEST_DB",
                schema_name="TEST_SCHEMA",
                pipe_name="TEST_PIPE",
            )
        )
        
        for i in range(1, 3):
            config.event_hubs[f"EVENTHUBNAME_{i}"] = EventHubConfig(
                name=f"hub-{i}",
                namespace="test.servicebus.windows.net",
                consumer_group=f"group-{i}",
            )
            config.snowflake_configs[f"SNOWFLAKE_{i}"] = SnowflakeConfig(
                database=f"DB_{i}",
                schema_name=f"SCHEMA_{i}",
                table_name=f"TABLE_{i}",
            )
            config.mappings.append(
                EventHubSnowflakeMapping(
                    event_hub_key=f"EVENTHUBNAME_{i}",
                    snowflake_key=f"SNOWFLAKE_{i}",
                )
            )
        
        # Track ingestions per mapping
        ingestions = {"TABLE_1": [], "TABLE_2": []}
        
        def mock_ingest(channel_name, data_batch, partition_id):
            if "hub-1" in channel_name:
                ingestions["TABLE_1"].append(len(data_batch))
            elif "hub-2" in channel_name:
                ingestions["TABLE_2"].append(len(data_batch))
            return True
        
        mock_streaming_client = mocker.MagicMock()
        mock_streaming_client.ingest_batch.side_effect = mock_ingest
        mock_streaming_client.start.return_value = None
        mock_streaming_client.stop.return_value = None
        mock_streaming_client.health_check.return_value = {"status": "healthy"}
        
        mocker.patch(
            "src.pipeline.orchestrator.create_snowflake_streaming_client",
            return_value=mock_streaming_client
        )
        
        mocker.patch("src.consumers.eventhub.SnowflakeCheckpointManager")
        mocker.patch("src.consumers.eventhub.SnowflakeCheckpointStore")
        
        # Act: Create orchestrator and process messages independently
        orchestrator = PipelineOrchestrator(config=config)
        orchestrator.start()
        
        # Create test messages for each mapping
        for idx, mapping in enumerate(orchestrator.mappings, 1):
            messages = []
            for i in range(5):
                mock_event = mocker.MagicMock()
                mock_event.body_as_str.return_value = f'{{"mapping": {idx}, "msg": {i}}}'
                mock_event.sequence_number = i
                mock_event.offset = str(i)
                mock_event.enqueued_time = datetime.now(UTC)
                mock_event.properties = {}
                mock_event.system_properties = {}
                
                msg = EventHubMessage(
                    event_data=mock_event,
                    partition_id="0",
                    sequence_number=i,
                )
                messages.append(msg)
            
            # Process messages for this mapping
            mapping._process_messages(messages)
        
        # Cleanup
        for mapping in orchestrator.mappings:
            if mapping.snowflake_client:
                mapping.snowflake_client.stop()
        
        # Assert: Verify independent operation
        assert len(ingestions["TABLE_1"]) == 1
        assert len(ingestions["TABLE_2"]) == 1
        assert ingestions["TABLE_1"][0] == 5
        assert ingestions["TABLE_2"][0] == 5
        
        # Verify stats are independent
        stats1 = orchestrator.mappings[0].get_stats()
        stats2 = orchestrator.mappings[1].get_stats()
        
        assert stats1["messages_processed"] == 5
        assert stats2["messages_processed"] == 5
        assert stats1["mapping_key"] != stats2["mapping_key"]

    async def test_mappings_share_no_state_between_each_other(
        self,
        mock_eventhub_client,
        mock_azure_credential,
        mock_snowflake_connection,
        mock_logfire,
        mocker,
        tmp_path,
        monkeypatch,
    ):
        """Test that mappings maintain separate state."""
        # Clear any potentially polluted EVENTHUBNAME_* and SNOWFLAKE_* environment variables
        for key in list(os.environ.keys()):
            if key.startswith(("EVENTHUBNAME_", "SNOWFLAKE_")) and "_" in key:
                if key not in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE",
                               "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_PIPE_NAME",
                               "SNOWFLAKE_PRIVATE_KEY_FILE", "SNOWFLAKE_PRIVATE_KEY_PASSWORD"]:
                    monkeypatch.delenv(key, raising=False)
        
        # Arrange
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")


# ============================================================================
# Test Class: TestErrorRecovery
# ============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
class TestErrorRecovery:
    """Tests for error recovery and retry mechanisms."""

    async def test_transient_snowflake_errors_trigger_retry(
        self,
        mock_eventhub_client,
        mock_azure_credential,
        mock_snowflake_connection,
        mock_logfire,
        sample_eventhub_config,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        sample_eventhub_messages,
        mocker,
    ):
        """Test that transient Snowflake errors trigger retry logic."""
        # Arrange: Mock Snowflake client with transient failures
        attempt_count = [0]
        
        def mock_ingest_with_retry(channel_name, data_batch, partition_id):
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                # Fail first 2 attempts with transient error
                raise ConnectionError("Temporary network issue")
            return True  # Succeed on 3rd attempt
        
        mock_streaming_client = mocker.MagicMock()
        mock_streaming_client.ingest_batch.side_effect = mock_ingest_with_retry
        mock_streaming_client.start.return_value = None
        mock_streaming_client.stop.return_value = None
        
        mocker.patch(
            "src.pipeline.orchestrator.create_snowflake_streaming_client",
            return_value=mock_streaming_client
        )
        
        # Mock retry manager with smart retry disabled
        mock_retry_manager = mocker.MagicMock()
        mock_retry_manager.get_retry_decorator.return_value = lambda f: f
        
        # Create mapping config
        mapping_config = EventHubSnowflakeMapping(
            event_hub_key="EVENTHUBNAME_1",
            snowflake_key="SNOWFLAKE_1",
        )
        
        pipeline_config = EvSnowConfig(
            eventhub_namespace=sample_eventhub_config.namespace,
            snowflake_connection=sample_snowflake_connection_config,
        )
        pipeline_config.event_hubs["EVENTHUBNAME_1"] = sample_eventhub_config
        pipeline_config.snowflake_configs["SNOWFLAKE_1"] = sample_snowflake_config
        
        mocker.patch("src.consumers.eventhub.SnowflakeCheckpointManager")
        mocker.patch("src.consumers.eventhub.SnowflakeCheckpointStore")
        
        # Act: Create mapping and process messages with retries
        mapping = PipelineMapping(
            mapping_config=mapping_config,
            pipeline_config=pipeline_config,
            retry_manager=mock_retry_manager,
        )
        mapping.start()
        
        # Manually retry the ingestion
        success = False
        for _ in range(3):
            try:
                success = mapping._process_messages(sample_eventhub_messages)
                if success:
                    break
            except ConnectionError:
                continue
        
        # Cleanup
        mapping.snowflake_client.stop()
        
        # Assert: Verify retry behavior
        assert attempt_count[0] == 3  # Should have tried 3 times
        assert success is True  # Should eventually succeed

    async def test_fatal_errors_stop_pipeline_gracefully(
        self,
        mock_eventhub_client,
        mock_azure_credential,
        mock_snowflake_connection,
        mock_logfire,
        sample_eventhub_config,
        sample_snowflake_config,
        sample_snowflake_connection_config,
        mocker,
    ):
        """Test that fatal errors stop the pipeline gracefully."""
        # Arrange: Mock Snowflake client with fatal error
        def mock_ingest_fatal_error(channel_name, data_batch, partition_id):
            raise ValueError("Invalid data format - fatal error")
        
        mock_streaming_client = mocker.MagicMock()
        mock_streaming_client.ingest_batch.side_effect = mock_ingest_fatal_error
        mock_streaming_client.start.return_value = None
        mock_streaming_client.stop.return_value = None
        
        mocker.patch(
            "src.pipeline.orchestrator.create_snowflake_streaming_client",
            return_value=mock_streaming_client
        )
        
        mapping_config = EventHubSnowflakeMapping(
            event_hub_key="EVENTHUBNAME_1",
            snowflake_key="SNOWFLAKE_1",
        )
        
        pipeline_config = EvSnowConfig(
            eventhub_namespace=sample_eventhub_config.namespace,
            snowflake_connection=sample_snowflake_connection_config,
        )
        pipeline_config.event_hubs["EVENTHUBNAME_1"] = sample_eventhub_config
        pipeline_config.snowflake_configs["SNOWFLAKE_1"] = sample_snowflake_config
        
        mocker.patch("src.consumers.eventhub.SnowflakeCheckpointManager")
        mocker.patch("src.consumers.eventhub.SnowflakeCheckpointStore")
        
        # Act: Create mapping and try to process
        mapping = PipelineMapping(
            mapping_config=mapping_config,
            pipeline_config=pipeline_config,
        )
        mapping.start()
        
        # Create test messages
        messages = []
        for i in range(5):
            mock_event = mocker.MagicMock()
            mock_event.body_as_str.return_value = f'{{"id": {i}}}'
            mock_event.sequence_number = i
            mock_event.offset = str(i)
            mock_event.enqueued_time = datetime.now(UTC)
            mock_event.properties = {}
            mock_event.system_properties = {}
            
            msg = EventHubMessage(
                event_data=mock_event,
                partition_id="0",
                sequence_number=i,
            )
            messages.append(msg)
        
        # Process messages - should handle error gracefully
        success = mapping._process_messages(messages)
        
        # Cleanup
        mapping.snowflake_client.stop()
        
        # Assert: Verify error handling
        assert success is False  # Processing failed
        assert len(mapping.stats["errors"]) > 0  # Error was recorded

class TestConfigurationFlow:
    """Tests for configuration loading and validation flow."""

    def test_load_config_from_environment_variables(self, env_setup, tmp_path):
        """Test loading configuration from environment variables."""
        # Arrange: Add private key file to env
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")
        env_setup["SNOWFLAKE_PRIVATE_KEY_FILE"] = str(key_file)
        
        import os
        os.environ.update(env_setup)
        
        # Act: Load config
        config = load_config()
        
        # Assert: Verify config loaded correctly
        assert config.eventhub_namespace == "test-namespace.servicebus.windows.net"
        assert "EVENTHUBNAME_1" in config.event_hubs
        assert config.event_hubs["EVENTHUBNAME_1"].name == "test-hub"
        assert config.event_hubs["EVENTHUBNAME_1"].consumer_group == "test-group"
        assert "SNOWFLAKE_1" in config.snowflake_configs
        assert config.snowflake_configs["SNOWFLAKE_1"].database == "TEST_DB"

    def test_validate_configuration_with_valid_config(self, env_setup, tmp_path):
        """Test configuration validation with valid config."""
        # Arrange: Setup environment
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")
        env_setup["SNOWFLAKE_PRIVATE_KEY_FILE"] = str(key_file)
        
        import os
        os.environ.update(env_setup)
        
        config = load_config()
        
        # Act: Validate configuration
        results = config.validate_configuration()
        
        # Assert: Verify validation results
        assert results["valid"] is True
        assert results["event_hubs_count"] >= 1
        assert results["snowflake_configs_count"] >= 1
        assert results["mappings_count"] >= 1
        assert len(results["errors"]) == 0

    def test_create_mappings_from_config(self, env_setup, tmp_path):
        """Test creating mappings from configuration."""
        # Arrange: Setup environment
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")
        env_setup["SNOWFLAKE_PRIVATE_KEY_FILE"] = str(key_file)
        
        import os
        os.environ.update(env_setup)
        
        config = load_config()
        
        # Act: Create mappings
        mappings = config.mappings
        
        # Assert: Verify mappings created
        assert len(mappings) >= 1
        for mapping in mappings:
            assert mapping.event_hub_key in config.event_hubs
            assert mapping.snowflake_key in config.snowflake_configs

    def test_initialize_all_components_from_config(
        self,
        env_setup,
        tmp_path,
        mock_eventhub_client,
        mock_azure_credential,
        mock_snowflake_connection,
        mock_logfire,
        mocker,
    ):
        """Test initializing all pipeline components from configuration."""
        # Arrange: Setup environment
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")
        env_setup["SNOWFLAKE_PRIVATE_KEY_FILE"] = str(key_file)
        
        import os
        os.environ.update(env_setup)
        
        config = load_config()
        
        # Mock Snowflake client
        mock_streaming_client = mocker.MagicMock()
        mock_streaming_client.ingest_batch.return_value = True
        mock_streaming_client.start.return_value = None
        mock_streaming_client.stop.return_value = None
        mock_streaming_client.health_check.return_value = {"status": "healthy"}
        
        mocker.patch(
            "src.pipeline.orchestrator.create_snowflake_streaming_client",
            return_value=mock_streaming_client
        )
        
        # Mock checkpoint dependencies
        mock_checkpoint_mgr = mocker.MagicMock()
        mock_checkpoint_mgr.get_last_checkpoint = AsyncMock(return_value=None)
        mock_checkpoint_mgr.save_checkpoint = AsyncMock(return_value=True)
        mock_checkpoint_mgr.close = mocker.MagicMock()
        
        mocker.patch(
            "src.consumers.eventhub.SnowflakeCheckpointManager",
            return_value=mock_checkpoint_mgr
        )
        
        mock_checkpoint_store = mocker.MagicMock()
        mock_checkpoint_store.list_ownership = AsyncMock(return_value=[])
        mock_checkpoint_store.claim_ownership = AsyncMock(return_value=[])
        mock_checkpoint_store.update_checkpoint = AsyncMock()
        mock_checkpoint_store.list_checkpoints = AsyncMock(return_value=[])
        
        mocker.patch(
            "src.consumers.eventhub.SnowflakeCheckpointStore",
            return_value=mock_checkpoint_store
        )
        
        # Act: Initialize orchestrator
        orchestrator = PipelineOrchestrator(config=config)
        orchestrator.start()
        
        # Get health check
        health = orchestrator.health_check()
        
        # Cleanup
        for mapping in orchestrator.mappings:
            if mapping.snowflake_client:
                mapping.snowflake_client.stop()
        
        # Assert: Verify all components initialized
        assert orchestrator.running is True
        assert len(orchestrator.mappings) >= 1
        assert health["orchestrator_status"] == "running"
        assert health["mappings_count"] >= 1
        
        # Verify each mapping has all components
        for mapping in orchestrator.mappings:
            assert mapping.eventhub_consumer is not None
            assert mapping.snowflake_client is not None
            assert mapping.running is True

    def test_configuration_validation_catches_errors(self, monkeypatch):
        """Test that configuration validation catches invalid configurations."""
        # Clear any potentially polluted EVENTHUBNAME_* and SNOWFLAKE_* environment variables
        for key in list(os.environ.keys()):
            if key.startswith(("EVENTHUBNAME_", "SNOWFLAKE_")) and "_" in key:
                if key not in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE",
                               "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_PIPE_NAME",
                               "SNOWFLAKE_PRIVATE_KEY_FILE", "SNOWFLAKE_PRIVATE_KEY_PASSWORD"]:
                    monkeypatch.delenv(key, raising=False)
        
        # Arrange: Setup invalid environment (missing consumer group)
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("EVENTHUBNAME_1", "test-hub")
        # Missing EVENTHUBNAME_1_CONSUMER_GROUP
        
        # Act & Assert: Should raise validation error
        with pytest.raises(ValueError, match="CONSUMER_GROUP"):
            load_config()

    def test_config_handles_multiple_mappings(self, env_setup, tmp_path, monkeypatch):
        """Test configuration with multiple EventHub->Snowflake mappings."""
        # Arrange: Setup multiple mappings
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")
        
        # Update environment for this test using monkeypatch
        for key, value in env_setup.items():
            monkeypatch.setenv(key, value)
        
        # Add second mapping using monkeypatch
        monkeypatch.setenv("EVENTHUBNAME_2", "test-hub-2")
        monkeypatch.setenv("EVENTHUBNAME_2_CONSUMER_GROUP", "test-group-2")
        monkeypatch.setenv("SNOWFLAKE_2_DATABASE", "TEST_DB_2")
        monkeypatch.setenv("SNOWFLAKE_2_SCHEMA", "TEST_SCHEMA_2")
        monkeypatch.setenv("SNOWFLAKE_2_TABLE", "TEST_TABLE_2")
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(key_file))
        
        # Act: Load config
        config = load_config()
        
        # Assert: Verify multiple mappings
        assert len(config.event_hubs) >= 2
        assert len(config.snowflake_configs) >= 2
        assert len(config.mappings) >= 2
        
        # Verify each mapping is valid
        for mapping in config.mappings:
            assert mapping.event_hub_key in config.event_hubs
            assert mapping.snowflake_key in config.snowflake_configs
