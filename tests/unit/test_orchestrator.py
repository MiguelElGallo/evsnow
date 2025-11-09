"""
Tests for pipeline orchestrator (src/pipeline/orchestrator.py).

This module tests the orchestrator that coordinates multiple EventHub consumers
and Snowflake streaming clients to provide a complete data pipeline.

Test Coverage:
- PipelineMapping: Single EventHub->Snowflake mapping management
- PipelineOrchestrator: Multi-mapping orchestration and lifecycle
- run_pipeline(): Main pipeline entry point function

All tests use mocks for external dependencies (EventHub, Snowflake, Logfire)
to ensure fast, isolated, and repeatable test execution.
"""

import asyncio
import signal
import sys
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest

# Add src to path for imports
sys.path.insert(0, '/home/runner/work/evsnow/evsnow/src')

from pipeline.orchestrator import (
    PipelineMapping,
    PipelineOrchestrator,
    run_pipeline,
)
from utils.config import (
    EventHubSnowflakeMapping,
    EvSnowConfig,
)


# ============================================================================
# Fixtures: Mock Components
# ============================================================================


@pytest.fixture
def mock_eventhub_consumer(mocker):
    """Mock EventHub async consumer."""
    mock_consumer = mocker.MagicMock()
    # Make start() an AsyncMock that returns immediately
    mock_consumer.start = mocker.AsyncMock(return_value=None)
    mock_consumer.stop = mocker.AsyncMock(return_value=None)
    mock_consumer.running = False
    
    # Create a mock class that returns our mock_consumer when instantiated with ANY arguments
    mock_class = mocker.MagicMock(return_value=mock_consumer)
    
    # Patch the consumer class where it's USED (in orchestrator)
    # Note: Due to sys.path.insert in this file, imports use "pipeline.orchestrator" not "src.pipeline.orchestrator"
    mocker.patch(
        "pipeline.orchestrator.EventHubAsyncConsumer",
        mock_class
    )
    
    return mock_consumer


@pytest.fixture
def mock_snowflake_client(mocker):
    """Mock Snowflake streaming client."""
    mock_client = mocker.MagicMock()
    mock_client.start = mocker.MagicMock()
    mock_client.stop = mocker.MagicMock()
    mock_client.ingest_batch = mocker.MagicMock(return_value=True)
    mock_client.health_check = mocker.MagicMock(return_value={
        "status": "healthy",
        "channels_open": 1
    })
    mock_client.get_stats = mocker.MagicMock(return_value={
        "messages_processed": 0,
        "batches_processed": 0
    })
    
    # Patch where the client class is USED (in factory), not where it's DEFINED
    mocker.patch(
        "streaming.factory.SnowflakeHighPerformanceStreamingClient",
        return_value=mock_client
    )
    
    return mock_client


@pytest.fixture
def complete_pipeline_config(
    sample_eventhub_config,
    sample_snowflake_config,
    sample_snowflake_connection_config,
    sample_mapping,
    sample_logfire_config,
    sample_smart_retry_config
):
    """Create a complete EvSnowConfig for testing."""
    config = EvSnowConfig(
        eventhub_namespace="test-namespace.servicebus.windows.net",
        event_hubs={"EVENTHUBNAME_1": sample_eventhub_config.model_dump()},
        snowflake_configs={"SNOWFLAKE_1": sample_snowflake_config.model_dump()},
        snowflake_connection=sample_snowflake_connection_config.model_dump(),
        mappings=[sample_mapping.model_dump()],
        target_db="CONTROL",
        target_schema="PUBLIC",
        target_table="INGESTION_STATUS",
        logfire=sample_logfire_config.model_dump(),
        smart_retry=sample_smart_retry_config.model_dump(),
    )
    return config


# ============================================================================
# Test Class: PipelineMapping
# ============================================================================


class TestPipelineMapping:
    """Tests for PipelineMapping class."""

    def test_init_with_valid_config_creates_mapping(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_logfire
    ):
        """Test that PipelineMapping initializes successfully with valid config."""
        # Act
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        
        # Assert
        assert mapping.mapping_config == sample_mapping
        assert mapping.pipeline_config == complete_pipeline_config
        assert mapping.eventhub_config is not None
        assert mapping.snowflake_config is not None
        assert not mapping.running
        assert mapping.eventhub_consumer is None
        assert mapping.snowflake_client is None
        assert mapping.stats["mapping_key"] == "EVENTHUBNAME_1->SNOWFLAKE_1"
        assert mapping.stats["messages_processed"] == 0
        assert mapping.stats["batches_processed"] == 0

    def test_init_with_invalid_eventhub_key_raises_error(
        self,
        complete_pipeline_config,
        mock_logfire
    ):
        """Test that PipelineMapping raises error for invalid EventHub key."""
        # Arrange
        invalid_mapping = EventHubSnowflakeMapping(
            event_hub_key="INVALID_KEY",
            snowflake_key="SNOWFLAKE_1",
        )
        
        # Act & Assert
        with pytest.raises(ValueError, match="Invalid mapping configuration"):
            PipelineMapping(
                mapping_config=invalid_mapping,
                pipeline_config=complete_pipeline_config,
            )

    def test_init_with_invalid_snowflake_key_raises_error(
        self,
        complete_pipeline_config,
        mock_logfire
    ):
        """Test that PipelineMapping raises error for invalid Snowflake key."""
        # Arrange
        invalid_mapping = EventHubSnowflakeMapping(
            event_hub_key="EVENTHUBNAME_1",
            snowflake_key="INVALID_KEY",
        )
        
        # Act & Assert
        with pytest.raises(ValueError, match="Invalid mapping configuration"):
            PipelineMapping(
                mapping_config=invalid_mapping,
                pipeline_config=complete_pipeline_config,
            )

    def test_start_initializes_components_successfully(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that start() initializes EventHub and Snowflake components."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        
        # Act
        mapping.start()
        
        # Assert
        assert mapping.running
        assert mapping.snowflake_client is not None
        assert mapping.eventhub_consumer is not None
        assert mapping.stats["started_at"] is not None
        mock_snowflake_client.start.assert_called_once()

    def test_start_when_already_running_logs_warning(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        caplog
    ):
        """Test that calling start() when already running logs a warning."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        mapping.start()
        
        # Act
        with caplog.at_level("WARNING"):
            mapping.start()
        
        # Assert
        assert "already running" in caplog.text.lower()

    def test_start_without_snowflake_connection_raises_error(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_logfire
    ):
        """Test that start() raises error when Snowflake connection is missing."""
        # Arrange
        complete_pipeline_config.snowflake_connection = None
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        
        # Act & Assert
        with pytest.raises(ValueError, match="Snowflake connection configuration is required"):
            mapping.start()

    @pytest.mark.asyncio
    async def test_start_async_starts_eventhub_consumer(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that start_async() starts the EventHub consumer."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        mapping.start()
        
        # Act
        await mapping.start_async()
        
        # Assert
        mock_eventhub_consumer.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_async_without_start_raises_error(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_logfire
    ):
        """Test that start_async() raises error if start() not called first."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        
        # Act & Assert
        with pytest.raises(RuntimeError, match="Mapping must be started"):
            await mapping.start_async()

    def test_process_messages_ingests_to_snowflake_successfully(
        self,
        complete_pipeline_config,
        sample_mapping,
        sample_eventhub_messages,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that _process_messages() successfully ingests messages to Snowflake."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        mapping.start()
        
        # Act
        result = mapping._process_messages(sample_eventhub_messages)
        
        # Assert
        assert result is True
        assert mapping.stats["messages_processed"] == len(sample_eventhub_messages)
        assert mapping.stats["batches_processed"] == 1
        assert mapping.stats["last_activity"] is not None
        mock_snowflake_client.ingest_batch.assert_called_once()

    def test_process_messages_without_snowflake_client_returns_false(
        self,
        complete_pipeline_config,
        sample_mapping,
        sample_eventhub_messages,
        mock_logfire
    ):
        """Test that _process_messages() returns False when Snowflake client is missing."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        # Don't call start() so client is None
        
        # Act
        result = mapping._process_messages(sample_eventhub_messages)
        
        # Assert
        assert result is False
        assert mapping.stats["messages_processed"] == 0

    def test_process_messages_handles_ingest_failure(
        self,
        complete_pipeline_config,
        sample_mapping,
        sample_eventhub_messages,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that _process_messages() handles Snowflake ingestion failure."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        mapping.start()
        mock_snowflake_client.ingest_batch.return_value = False
        
        # Act
        result = mapping._process_messages(sample_eventhub_messages)
        
        # Assert
        assert result is False
        assert mapping.stats["messages_processed"] == 0
        assert len(mapping.stats["errors"]) == 1

    def test_process_messages_handles_exception(
        self,
        complete_pipeline_config,
        sample_mapping,
        sample_eventhub_messages,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that _process_messages() handles exceptions gracefully."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        mapping.start()
        mock_snowflake_client.ingest_batch.side_effect = RuntimeError("Ingestion error")
        
        # Act
        result = mapping._process_messages(sample_eventhub_messages)
        
        # Assert
        assert result is False
        assert len(mapping.stats["errors"]) == 1
        assert "Ingestion error" in mapping.stats["errors"][0]["error"]

    @pytest.mark.asyncio
    async def test_stop_cleans_up_resources(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that stop() properly cleans up EventHub and Snowflake resources."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        mapping.start()
        
        # Act
        await mapping.stop()
        
        # Assert
        assert not mapping.running
        assert mapping.eventhub_consumer is None
        assert mapping.snowflake_client is None
        mock_eventhub_consumer.stop.assert_called_once()
        mock_snowflake_client.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_when_not_running_does_nothing(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_logfire
    ):
        """Test that stop() does nothing when mapping is not running."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        
        # Act (should not raise)
        await mapping.stop()
        
        # Assert
        assert not mapping.running

    def test_get_stats_returns_correct_statistics(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that get_stats() returns accurate statistics."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        mapping.start()
        mapping.stats["messages_processed"] = 100
        mapping.stats["batches_processed"] = 10
        
        # Act
        stats = mapping.get_stats()
        
        # Assert
        assert stats["mapping_key"] == "EVENTHUBNAME_1->SNOWFLAKE_1"
        assert stats["messages_processed"] == 100
        assert stats["batches_processed"] == 10
        assert "runtime_seconds" in stats
        assert "messages_per_second" in stats

    def test_get_stats_without_started_excludes_runtime(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_logfire
    ):
        """Test that get_stats() excludes runtime when not started."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        
        # Act
        stats = mapping.get_stats()
        
        # Assert
        assert "runtime_seconds" not in stats
        assert "messages_per_second" not in stats

    def test_health_check_returns_status(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that health_check() returns component health status."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        mapping.start()
        
        # Act
        health = mapping.health_check()
        
        # Assert
        assert health["mapping_key"] == "EVENTHUBNAME_1->SNOWFLAKE_1"
        assert health["running"] is True
        assert "snowflake" in health["components"]
        assert "eventhub" in health["components"]
        assert len(health["errors"]) == 0

    def test_health_check_reports_missing_components(
        self,
        complete_pipeline_config,
        sample_mapping,
        mock_logfire
    ):
        """Test that health_check() reports missing components."""
        # Arrange
        mapping = PipelineMapping(
            mapping_config=sample_mapping,
            pipeline_config=complete_pipeline_config,
        )
        # Don't start, so components are None
        
        # Act
        health = mapping.health_check()
        
        # Assert
        assert health["running"] is False
        assert len(health["errors"]) == 2
        assert any("Snowflake" in err for err in health["errors"])
        assert any("EventHub" in err for err in health["errors"])


# ============================================================================
# Test Class: PipelineOrchestrator
# ============================================================================


class TestPipelineOrchestrator:
    """Tests for PipelineOrchestrator class."""

    def test_init_creates_orchestrator(
        self,
        complete_pipeline_config,
        mock_logfire
    ):
        """Test that PipelineOrchestrator initializes successfully."""
        # Act
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        
        # Assert
        assert orchestrator.config == complete_pipeline_config
        assert len(orchestrator.mappings) == 0
        assert not orchestrator.running
        assert not orchestrator.shutdown_requested
        assert orchestrator.stats["mappings_count"] == 0

    def test_initialize_creates_all_mappings(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that initialize() creates all configured mappings."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        
        # Act
        orchestrator.initialize()
        
        # Assert
        assert len(orchestrator.mappings) == 1
        assert orchestrator.stats["mappings_count"] == 1
        assert orchestrator.mappings[0].running

    def test_initialize_with_multiple_mappings(
        self,
        complete_pipeline_config,
        sample_eventhub_config,
        sample_snowflake_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that initialize() handles multiple mappings."""
        # Arrange
        # Add second EventHub and Snowflake config
        complete_pipeline_config.event_hubs["EVENTHUBNAME_2"] = sample_eventhub_config
        complete_pipeline_config.snowflake_configs["SNOWFLAKE_2"] = sample_snowflake_config
        complete_pipeline_config.mappings.append(
            EventHubSnowflakeMapping(
                event_hub_key="EVENTHUBNAME_2",
                snowflake_key="SNOWFLAKE_2",
            )
        )
        
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        
        # Act
        orchestrator.initialize()
        
        # Assert
        assert len(orchestrator.mappings) == 2
        assert orchestrator.stats["mappings_count"] == 2

    def test_start_initializes_and_sets_running(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that start() initializes mappings and sets running flag."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        
        # Act
        orchestrator.start()
        
        # Assert
        assert orchestrator.running
        assert len(orchestrator.mappings) == 1

    def test_start_when_already_running_logs_warning(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        caplog
    ):
        """Test that start() logs warning when already running."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        orchestrator.start()
        
        # Act
        with caplog.at_level("WARNING"):
            orchestrator.start()
        
        # Assert
        assert "already running" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_run_async_starts_all_mapping_tasks(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker
    ):
        """Test that run_async() starts async tasks for all mappings."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        orchestrator.start()
        
        # Make gather return immediately to avoid hanging
        mock_gather = mocker.patch(
            "asyncio.gather",
            side_effect=asyncio.CancelledError()
        )
        
        # Act & Assert
        with pytest.raises(asyncio.CancelledError):
            await orchestrator.run_async()
        
        # Verify tasks were created
        assert len(orchestrator.tasks) == 1

    @pytest.mark.asyncio
    async def test_run_async_without_start_raises_error(
        self,
        complete_pipeline_config,
        mock_logfire
    ):
        """Test that run_async() raises error if start() not called."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        
        # Act & Assert
        with pytest.raises(RuntimeError, match="Orchestrator must be started"):
            await orchestrator.run_async()

    @pytest.mark.asyncio
    async def test_run_async_handles_cancelled_error(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker
    ):
        """Test that run_async() handles CancelledError properly."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        orchestrator.start()
        
        mocker.patch(
            "asyncio.gather",
            side_effect=asyncio.CancelledError()
        )
        
        # Act & Assert
        with pytest.raises(asyncio.CancelledError):
            await orchestrator.run_async()

    @pytest.mark.asyncio
    async def test_stop_cancels_all_tasks(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker
    ):
        """Test that stop() cancels all running tasks."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        orchestrator.start()
        
        # Create mock tasks
        mock_task1 = mocker.MagicMock()
        mock_task1.done.return_value = False
        mock_task1.cancel = mocker.MagicMock()
        orchestrator.tasks = [mock_task1]
        
        # Mock asyncio.gather to return immediately with completed future
        async def mock_gather(*args, **kwargs):
            return None
        mocker.patch("asyncio.gather", side_effect=mock_gather)
        
        # Act
        await orchestrator.stop()
        
        # Assert
        assert not orchestrator.running
        mock_task1.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_cleans_up_all_mappings(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker
    ):
        """Test that stop() properly stops all mappings."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        orchestrator.start()
        
        # Mock asyncio.gather to return immediately
        async def mock_gather(*args, **kwargs):
            return None
        mocker.patch("asyncio.gather", side_effect=mock_gather)
        
        # Spy on mapping.stop()
        for mapping in orchestrator.mappings:
            mapping.stop = AsyncMock()
        
        # Act
        await orchestrator.stop()
        
        # Assert
        assert len(orchestrator.mappings) == 0
        assert len(orchestrator.tasks) == 0

    @pytest.mark.asyncio
    async def test_stop_when_not_running_does_nothing(
        self,
        complete_pipeline_config,
        mock_logfire
    ):
        """Test that stop() does nothing when not running."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        
        # Act (should not raise)
        await orchestrator.stop()
        
        # Assert
        assert not orchestrator.running

    def test_get_stats_aggregates_mapping_stats(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that get_stats() aggregates statistics from all mappings."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        orchestrator.start()
        
        # Set some stats on mappings
        orchestrator.mappings[0].stats["messages_processed"] = 100
        orchestrator.mappings[0].stats["batches_processed"] = 10
        
        # Act
        stats = orchestrator.get_stats()
        
        # Assert
        assert stats["mappings_count"] == 1
        assert stats["total_messages_processed"] == 100
        assert stats["total_batches_processed"] == 10
        assert "runtime_seconds" in stats
        assert len(stats["mappings"]) == 1

    def test_health_check_aggregates_mapping_health(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire
    ):
        """Test that health_check() aggregates health from all mappings."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        orchestrator.start()
        
        # Act
        health = orchestrator.health_check()
        
        # Assert
        assert health["orchestrator_status"] == "running"
        assert health["mappings_count"] == 1
        assert len(health["mappings"]) == 1

    def test_setup_signal_handlers_registers_handlers(
        self,
        complete_pipeline_config,
        mock_logfire,
        mocker
    ):
        """Test that setup_signal_handlers() registers signal handlers."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        loop = asyncio.new_event_loop()
        
        mock_add_signal_handler = mocker.patch.object(loop, "add_signal_handler")
        
        # Act
        orchestrator.setup_signal_handlers(loop)
        
        # Assert
        # Should register handlers for SIGINT and SIGTERM
        assert mock_add_signal_handler.call_count == 2
        calls = mock_add_signal_handler.call_args_list
        registered_signals = [call[0][0] for call in calls]
        assert signal.SIGINT in registered_signals
        assert signal.SIGTERM in registered_signals

    def test_signal_handler_cancels_tasks_on_first_signal(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker
    ):
        """Test that signal handler cancels tasks on first signal."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        orchestrator.start()
        loop = asyncio.new_event_loop()
        
        # Create mock tasks
        mock_task = mocker.MagicMock()
        mock_task.done.return_value = False
        mock_task.get_name.return_value = "test_task"
        orchestrator.tasks = [mock_task]
        
        # Capture the signal handler
        signal_handlers = {}
        
        def capture_handler(sig, handler):
            signal_handlers[sig] = handler
        
        mocker.patch.object(loop, "add_signal_handler", side_effect=capture_handler)
        mocker.patch.object(loop, "create_task")
        
        orchestrator.setup_signal_handlers(loop)
        
        # Act - trigger SIGINT
        signal_handlers[signal.SIGINT]()
        
        # Assert
        assert orchestrator.shutdown_requested
        mock_task.cancel.assert_called_once()

    def test_signal_handler_forces_exit_on_second_signal(
        self,
        complete_pipeline_config,
        mock_logfire,
        mocker
    ):
        """Test that signal handler forces exit on second signal."""
        # Arrange
        orchestrator = PipelineOrchestrator(config=complete_pipeline_config)
        loop = asyncio.new_event_loop()
        
        # Capture the signal handler
        signal_handlers = {}
        
        def capture_handler(sig, handler):
            signal_handlers[sig] = handler
        
        mocker.patch.object(loop, "add_signal_handler", side_effect=capture_handler)
        mock_exit = mocker.patch("sys.exit")
        
        orchestrator.setup_signal_handlers(loop)
        
        # Act - trigger SIGINT twice
        orchestrator.shutdown_requested = True  # Simulate first signal
        signal_handlers[signal.SIGINT]()
        
        # Assert
        mock_exit.assert_called_once_with(1)


# ============================================================================
# Test Class: run_pipeline()
# ============================================================================


class TestRunPipeline:
    """Tests for run_pipeline() function."""

    @pytest.mark.asyncio
    async def test_run_pipeline_creates_orchestrator(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker
    ):
        """Test that run_pipeline() creates an orchestrator."""
        # Arrange
        # Make run_async raise CancelledError immediately
        mock_orchestrator = mocker.MagicMock()
        mock_orchestrator.start = mocker.MagicMock()
        mock_orchestrator.setup_signal_handlers = mocker.MagicMock()
        mock_orchestrator.run_async = mocker.AsyncMock(side_effect=asyncio.CancelledError())
        mock_orchestrator.stop = mocker.AsyncMock()
        
        mocker.patch(
            "pipeline.orchestrator.PipelineOrchestrator",
            return_value=mock_orchestrator
        )
        
        # Act
        await run_pipeline(config=complete_pipeline_config)
        
        # Assert
        mock_orchestrator.start.assert_called_once()
        mock_orchestrator.setup_signal_handlers.assert_called_once()
        mock_orchestrator.run_async.assert_called_once()
        mock_orchestrator.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_pipeline_handles_cancelled_error(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker,
        caplog
    ):
        """Test that run_pipeline() handles CancelledError gracefully."""
        # Arrange
        mock_orchestrator = mocker.MagicMock()
        mock_orchestrator.start = mocker.MagicMock()
        mock_orchestrator.setup_signal_handlers = mocker.MagicMock()
        mock_orchestrator.run_async = mocker.AsyncMock(side_effect=asyncio.CancelledError())
        mock_orchestrator.stop = mocker.AsyncMock()
        
        mocker.patch(
            "pipeline.orchestrator.PipelineOrchestrator",
            return_value=mock_orchestrator
        )
        
        # Act
        with caplog.at_level("INFO"):
            await run_pipeline(config=complete_pipeline_config)
        
        # Assert
        assert "cancelled" in caplog.text.lower() or "shutdown" in caplog.text.lower()
        mock_orchestrator.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_pipeline_handles_keyboard_interrupt(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker,
        caplog
    ):
        """Test that run_pipeline() handles KeyboardInterrupt gracefully."""
        # Arrange
        mock_orchestrator = mocker.MagicMock()
        mock_orchestrator.start = mocker.MagicMock()
        mock_orchestrator.setup_signal_handlers = mocker.MagicMock()
        mock_orchestrator.run_async = mocker.AsyncMock(side_effect=KeyboardInterrupt())
        mock_orchestrator.stop = mocker.AsyncMock()
        
        mocker.patch(
            "pipeline.orchestrator.PipelineOrchestrator",
            return_value=mock_orchestrator
        )
        
        # Act
        with caplog.at_level("INFO"):
            await run_pipeline(config=complete_pipeline_config)
        
        # Assert
        assert "keyboard interrupt" in caplog.text.lower() or "shutdown" in caplog.text.lower()
        mock_orchestrator.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_pipeline_handles_generic_exception(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker
    ):
        """Test that run_pipeline() handles generic exceptions and cleans up."""
        # Arrange
        mock_orchestrator = mocker.MagicMock()
        mock_orchestrator.start = mocker.MagicMock()
        mock_orchestrator.setup_signal_handlers = mocker.MagicMock()
        mock_orchestrator.run_async = mocker.AsyncMock(
            side_effect=RuntimeError("Pipeline error")
        )
        mock_orchestrator.stop = mocker.AsyncMock()
        
        mocker.patch(
            "pipeline.orchestrator.PipelineOrchestrator",
            return_value=mock_orchestrator
        )
        
        # Act & Assert
        with pytest.raises(RuntimeError, match="Pipeline error"):
            await run_pipeline(config=complete_pipeline_config)
        
        # Verify cleanup was called
        mock_orchestrator.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_pipeline_always_calls_stop(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker
    ):
        """Test that run_pipeline() always calls stop() in finally block."""
        # Arrange
        mock_orchestrator = mocker.MagicMock()
        mock_orchestrator.start = mocker.MagicMock()
        mock_orchestrator.setup_signal_handlers = mocker.MagicMock()
        mock_orchestrator.run_async = mocker.AsyncMock(side_effect=ValueError("Test error"))
        mock_orchestrator.stop = mocker.AsyncMock()
        
        mocker.patch(
            "pipeline.orchestrator.PipelineOrchestrator",
            return_value=mock_orchestrator
        )
        
        # Act & Assert
        with pytest.raises(ValueError):
            await run_pipeline(config=complete_pipeline_config)
        
        # Verify stop was called despite exception
        mock_orchestrator.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_pipeline_with_retry_manager(
        self,
        complete_pipeline_config,
        mock_eventhub_consumer,
        mock_snowflake_client,
        mock_logfire,
        mocker
    ):
        """Test that run_pipeline() accepts and uses retry_manager."""
        # Arrange
        mock_retry_manager = mocker.MagicMock()
        
        mock_orchestrator = mocker.MagicMock()
        mock_orchestrator.start = mocker.MagicMock()
        mock_orchestrator.setup_signal_handlers = mocker.MagicMock()
        mock_orchestrator.run_async = mocker.AsyncMock(side_effect=asyncio.CancelledError())
        mock_orchestrator.stop = mocker.AsyncMock()
        
        mock_orchestrator_class = mocker.patch(
            "pipeline.orchestrator.PipelineOrchestrator",
            return_value=mock_orchestrator
        )
        
        # Act
        await run_pipeline(
            config=complete_pipeline_config,
            retry_manager=mock_retry_manager
        )
        
        # Assert
        mock_orchestrator_class.assert_called_once_with(
            config=complete_pipeline_config,
            retry_manager=mock_retry_manager
        )
