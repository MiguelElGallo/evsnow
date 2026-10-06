"""Real message/mapping/consumer boundaries with only external services mocked."""

import asyncio
import json
import os
from concurrent.futures import Future
from datetime import UTC, datetime
from threading import Event
from unittest.mock import AsyncMock, MagicMock

import pytest
from snowflake.ingest.streaming import StreamingIngestClient, StreamingIngestElasticChannel
from typer.testing import CliRunner

from consumers.eventhub import EventHubAsyncConsumer, EventHubMessage, MessageBatch
from main import app
from pipeline.orchestrator import PipelineMapping, PipelineOrchestrator, run_pipeline
from streaming.snowflake_elastic import SnowflakeElasticStreamingClient
from utils.config import EvSnowConfig, SnowflakeConnectionConfig


@pytest.fixture
def pipeline_boundary(
    sample_eventhub_config,
    sample_snowflake_config,
    sample_snowflake_connection_config,
    sample_mapping,
    mock_logfire,
    mocker,
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    for name in list(os.environ):
        if name.startswith(("SNOWFLAKE_", "EVENTHUB", "CONTROL", "TARGET")):
            monkeypatch.delenv(name, raising=False)
    connection = SnowflakeConnectionConfig(
        **(sample_snowflake_connection_config.model_dump() | {"channel_mode": "elastic"})
    )
    config = EvSnowConfig(
        eventhub_namespace=sample_eventhub_config.namespace,
        event_hubs={"EVENTHUBNAME_1": sample_eventhub_config.model_dump()},
        snowflake_configs={"SNOWFLAKE_1": sample_snowflake_config.model_dump()},
        snowflake_connection=connection,
        mappings=[sample_mapping.model_dump()],
        _source_env={},
    )
    mapping = PipelineMapping(sample_mapping, config)
    sdk = MagicMock(spec=StreamingIngestClient)
    channel = MagicMock(spec=StreamingIngestElasticChannel)
    sdk.get_elastic_channel.return_value = channel
    mocker.patch("streaming.snowflake_elastic.StreamingIngestClient", return_value=sdk)
    adapter = SnowflakeElasticStreamingClient(
        config.snowflake_configs["SNOWFLAKE_1"],
        connection.model_copy(update={"ack_timeout_seconds": 0.01}),
    )
    adapter.start()
    mapping.snowflake_client = adapter
    consumer = EventHubAsyncConsumer(
        eventhub_config=sample_eventhub_config,
        target_db="TEST_DB",
        target_schema="TEST_SCHEMA",
        target_table="TEST_TABLE",
        message_processor=mapping._process_messages,
        snowflake_config=connection,
    )
    return consumer, mapping, adapter, sdk, channel


def source_batch(*partitions):
    batch = MessageBatch(max_size=10, max_wait_seconds=60)
    contexts = []
    for index, partition in enumerate(partitions):
        event = MagicMock()
        event.body_as_str.return_value = json.dumps({"event_id": f"event-{index}", "value": index})
        event.enqueued_time = datetime(2026, 10, 5, tzinfo=UTC)
        event.properties = {b"producer": b"test"}
        event.system_properties = {b"sequence-number": index}
        event.sequence_number = index
        event.offset = str(index * 10)
        event.content_type = "application/json"
        message = EventHubMessage(
            event_data=event,
            partition_id=partition,
            sequence_number=index,
            eventhub_namespace="test.servicebus.windows.net",
            eventhub_name="source-hub",
            consumer_group="$Default",
        )
        context = MagicMock()
        context.update_checkpoint = AsyncMock()
        message.partition_context = context
        contexts.append(context)
        batch.add_message(message)
    return batch, contexts


def acknowledged():
    future = Future()
    future.set_result(None)
    return future


@pytest.mark.asyncio
async def test_durable_ack_blocks_checkpoint_until_original_future_completes(pipeline_boundary):
    consumer, _, adapter, _, channel = pipeline_boundary
    adapter.connection_config = adapter.connection_config.model_copy(
        update={"ack_timeout_seconds": 2}
    )
    batch, contexts = source_batch("0")
    pending = Future()
    submitted = Event()

    def append(rows, append_token):
        submitted.set()
        return pending

    channel.append_rows_with_wait.side_effect = append
    processing = asyncio.create_task(consumer._process_batch(batch))
    assert await asyncio.to_thread(submitted.wait, 1)
    contexts[0].update_checkpoint.assert_not_called()
    assert adapter.stats["total_messages_sent"] == 0
    pending.set_result(None)
    assert await processing is True
    contexts[0].update_checkpoint.assert_awaited_once_with(batch.messages[0].event_data)
    assert adapter.get_stats()["total_messages_sent"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["exception", "wait_timeout"])
async def test_partial_partition_ack_never_checkpoints_and_final_drain_reuses_receipt(
    pipeline_boundary, failure
):
    consumer, _, adapter, _, channel = pipeline_boundary
    batch, contexts = source_batch("0", "1")
    second_future = Future()
    if failure == "exception":
        second_future.set_exception(RuntimeError("second partition failed"))
    channel.append_rows_with_wait.side_effect = [acknowledged(), second_future, acknowledged()]
    consumer.running = True
    consumer._detached_batches.append(batch)
    assert await consumer._process_detached_batch(batch) is False
    for context in contexts:
        context.update_checkpoint.assert_not_called()
    assert consumer.current_batch is batch
    assert len(batch.messages) == 2
    assert adapter.get_stats()["total_messages_sent"] == 1
    assert consumer.stats["last_checkpoint"] is None
    assert channel.append_rows_with_wait.call_count == 2

    if failure == "wait_timeout":
        second_future.set_result(None)
    assert await consumer._process_batch(consumer.current_batch) is True
    for message, context in zip(batch.messages, contexts, strict=True):
        context.update_checkpoint.assert_awaited_once_with(message.event_data)
    assert adapter.get_stats()["total_messages_sent"] == 2
    assert channel.append_rows_with_wait.call_count == (3 if failure == "exception" else 2)
    submitted_partitions = [
        call.args[0][0]["partition_id"] for call in channel.append_rows_with_wait.call_args_list
    ]
    assert submitted_partitions.count("0") == 1
    assert consumer.stats["last_checkpoint"] == {"0": 0, "1": 1}


@pytest.mark.asyncio
async def test_checkpoint_write_failure_preserves_batch_and_receipt_but_restart_can_replay(
    pipeline_boundary, mocker
):
    consumer, _, adapter, sdk, channel = pipeline_boundary
    batch, contexts = source_batch("0")
    channel.append_rows_with_wait.return_value = acknowledged()
    contexts[0].update_checkpoint.side_effect = [RuntimeError("checkpoint unavailable")] * 3 + [
        None
    ]
    mocker.patch("consumers.eventhub.asyncio.sleep", new=AsyncMock())
    consumer.running = True
    consumer._detached_batches.append(batch)

    assert await consumer._process_detached_batch(batch) is False
    assert contexts[0].update_checkpoint.await_count == 3
    assert consumer.current_batch is batch
    assert consumer.stats["last_checkpoint"] is None
    assert adapter.get_stats()["total_messages_sent"] == 1
    assert channel.append_rows_with_wait.call_count == 1

    assert await consumer._process_batch(consumer.current_batch) is True
    assert contexts[0].update_checkpoint.await_count == 4
    assert consumer.stats["last_checkpoint"] == {"0": 0}
    assert channel.append_rows_with_wait.call_count == 1

    adapter.stop()
    adapter.start()
    contexts[0].update_checkpoint.side_effect = None
    assert await consumer._process_batch(batch) is True
    assert sdk.get_elastic_channel.call_count == 2
    assert channel.append_rows_with_wait.call_count == 2
    assert adapter.get_stats()["delivery_semantics"] == "at_least_once"


@pytest.mark.asyncio
async def test_wait_timeout_then_late_ack_without_source_retry_cannot_advance_checkpoint(
    pipeline_boundary,
):
    consumer, _, adapter, _, channel = pipeline_boundary
    batch, contexts = source_batch("0")
    pending = Future()
    channel.append_rows_with_wait.return_value = pending
    assert await consumer._process_batch(batch) is False
    pending.set_result(None)
    contexts[0].update_checkpoint.assert_not_called()
    assert consumer.stats["last_checkpoint"] is None
    # Closing can observe a buffer ack, but only the consumer's source callback
    # can save a checkpoint. A new process can therefore replay this event.
    adapter.stop()
    contexts[0].update_checkpoint.assert_not_called()
    assert adapter.get_stats()["total_messages_sent"] == 1


@pytest.fixture
def cli_pipeline(pipeline_boundary, mocker):
    """Keep the CLI and all pipeline layers real; replace only cloud I/O/setup."""
    consumer, mapping, adapter, sdk, channel = pipeline_boundary
    config = mapping.pipeline_config
    adapter.connection_config = adapter.connection_config.model_copy(
        update={"ack_timeout_seconds": 0.02, "close_timeout_seconds": 0.02}
    )
    mapping.eventhub_consumer = consumer
    mapping.running = True
    manager = MagicMock()
    manager.get_last_checkpoint = AsyncMock(return_value=None)
    credential = MagicMock(close=AsyncMock())
    client = MagicMock(close=AsyncMock())
    mocker.patch("consumers.eventhub.SnowflakeCheckpointManager", return_value=manager)
    mocker.patch("consumers.eventhub.EventHubConsumerClient", return_value=client)
    mocker.patch(
        "consumers.eventhub.build_eventhub_credential",
        new=AsyncMock(return_value=(credential, 0, "offline")),
    )
    orchestrator = PipelineOrchestrator(config)

    def initialize():
        orchestrator.mappings = [mapping]

    mocker.patch.object(orchestrator, "initialize", side_effect=initialize)
    mocker.patch.object(orchestrator, "setup_signal_handlers")
    mocker.patch("pipeline.orchestrator.PipelineOrchestrator", return_value=orchestrator)
    mocker.patch("utils.snowflake.close_all_cached_connections")
    mocker.patch("main.load_config", return_value=config)
    mocker.patch("main._initialize_logfire")
    mocker.patch("utils.smart_retry.RetryManager")
    return consumer, mapping, adapter, sdk, channel, orchestrator, client, credential, manager


@pytest.mark.parametrize(
    "failure",
    ["pending_append", "sdk_cancellation", "terminal_late_ack", "final_drain", "close", "signal"],
)
def test_cli_reports_pipeline_failure_after_cleaning_every_component(cli_pipeline, failure):
    consumer, mapping, _, sdk, channel, orchestrator, client, credential, manager = cli_pipeline
    batch, contexts = source_batch("0")
    pending = Future()
    channel.append_rows_with_wait.return_value = pending

    async def receive(**kwargs):
        consumer.current_batch = batch
        if failure in {"pending_append", "sdk_cancellation", "terminal_late_ack"}:
            consumer.current_batch = None
            consumer._detached_batches.append(batch)
            assert await consumer._process_detached_batch(batch) is False
            if failure == "terminal_late_ack":
                pending.set_result(None)
            if failure == "sdk_cancellation":
                raise asyncio.CancelledError
        elif failure == "close":
            pending.set_result(None)
            sdk.close.side_effect = RuntimeError("SDK close failed")
        elif failure == "signal":
            # Model the separate cleanup task scheduled by SIGTERM, while the
            # receive task returns and run_pipeline enters its finally block.
            orchestrator.shutdown_task = asyncio.create_task(orchestrator.stop())

    client.receive = receive
    result = CliRunner().invoke(app, ["run"])
    assert result.exit_code == 1, result.output
    assert isinstance(result.exception, SystemExit)
    sdk.close.assert_called_once()
    client.close.assert_awaited_once()
    credential.close.assert_awaited_once()
    manager.close.assert_called_once()
    assert mapping.eventhub_consumer is None
    assert mapping.snowflake_client is None
    assert orchestrator.mappings == []
    assert orchestrator.tasks == []
    if failure in {"terminal_late_ack", "close"}:
        contexts[0].update_checkpoint.assert_awaited_once()
    else:
        contexts[0].update_checkpoint.assert_not_called()
        assert consumer.current_batch is batch
    assert channel.append_rows_with_wait.call_count == 1


def test_cli_graceful_final_drain_waits_for_ack_and_exits_successfully(cli_pipeline):
    consumer, _, adapter, sdk, channel, orchestrator, client, credential, manager = cli_pipeline
    batch, contexts = source_batch("0")
    pending = Future()
    channel.append_rows_with_wait.return_value = pending

    async def receive(**kwargs):
        consumer.current_batch = batch
        asyncio.get_running_loop().call_later(0.001, pending.set_result, None)
        orchestrator.shutdown_task = asyncio.create_task(orchestrator.stop())

    client.receive = receive
    result = CliRunner().invoke(app, ["run"])
    assert result.exit_code == 0, result.output
    contexts[0].update_checkpoint.assert_awaited_once_with(batch.messages[0].event_data)
    assert consumer.current_batch is None
    assert adapter.get_stats()["total_messages_sent"] == 1
    sdk.close.assert_called_once()
    client.close.assert_awaited_once()
    credential.close.assert_awaited_once()
    manager.close.assert_called_once()


@pytest.mark.asyncio
async def test_failed_mapping_stops_healthy_peer_without_waiting_indefinitely(cli_pipeline):
    consumer, mapping, _, sdk, channel, orchestrator, client, _, _ = cli_pipeline
    batch, contexts = source_batch("0")
    channel.append_rows_with_wait.return_value = Future()
    peer_receiving = asyncio.Event()
    peer_stopped = asyncio.Event()

    async def peer_start():
        peer_receiving.set()
        await peer_stopped.wait()

    peer = MagicMock()
    peer.stats = {"mapping_key": "healthy-peer"}
    peer.start_async = peer_start
    peer.stop = AsyncMock(side_effect=peer_stopped.set)

    def initialize():
        orchestrator.mappings = [mapping, peer]

    orchestrator.initialize.side_effect = initialize

    async def receive(**kwargs):
        await peer_receiving.wait()
        consumer.current_batch = None
        consumer._detached_batches.append(batch)
        assert await consumer._process_detached_batch(batch) is False

    client.receive = receive
    with pytest.raises(RuntimeError, match="Batch processing failed"):
        await asyncio.wait_for(run_pipeline(mapping.pipeline_config), timeout=1)
    peer.stop.assert_awaited_once()
    assert peer_stopped.is_set()
    contexts[0].update_checkpoint.assert_not_called()
    sdk.close.assert_called_once()
    assert orchestrator.mappings == []


@pytest.mark.asyncio
async def test_concurrent_consumer_shutdown_drains_once_and_closes_after_ack(pipeline_boundary):
    consumer, _, adapter, _, channel = pipeline_boundary
    adapter.connection_config = adapter.connection_config.model_copy(
        update={"ack_timeout_seconds": 1}
    )
    batch, contexts = source_batch("0")
    consumer.current_batch = batch
    consumer.running = True
    credential = MagicMock(close=AsyncMock())
    consumer.credential = credential
    pending = Future()
    submitted = Event()

    def append(rows, append_token):
        submitted.set()
        return pending

    channel.append_rows_with_wait.side_effect = append
    first_stop = asyncio.create_task(consumer.stop())
    assert await asyncio.to_thread(submitted.wait, 1)
    second_stop = asyncio.create_task(consumer.stop())
    await asyncio.sleep(0)
    assert not second_stop.done()
    credential.close.assert_not_called()
    pending.set_result(None)
    await asyncio.gather(first_stop, second_stop)
    contexts[0].update_checkpoint.assert_awaited_once()
    channel.append_rows_with_wait.assert_called_once()
    credential.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_consumer_cleanup_continues_after_checkpoint_manager_close_fails(
    pipeline_boundary, mocker
):
    consumer, _, _, _, _ = pipeline_boundary
    manager = MagicMock()
    manager.close.side_effect = RuntimeError("checkpoint close failed")
    consumer.checkpoint_manager = manager
    capture_stop = mocker.patch.object(consumer, "_stop_capture_writer", new=AsyncMock())
    with pytest.raises(RuntimeError, match="checkpoint close failed"):
        await consumer.stop()
    capture_stop.assert_awaited_once()
    assert consumer.checkpoint_manager is None


@pytest.mark.asyncio
async def test_failed_background_close_keeps_client_for_shutdown_retry(pipeline_boundary):
    consumer, _, _, _, _ = pipeline_boundary
    client = MagicMock(close=AsyncMock(side_effect=[RuntimeError("close failed"), None]))
    credential = MagicMock(close=AsyncMock())
    consumer.client = client
    consumer.credential = credential
    await consumer._stop_after_batch_failure()
    assert consumer._receive_error_close_task is not None
    await consumer._receive_error_close_task
    assert consumer.client is client
    with pytest.raises(RuntimeError, match="Batch processing failed"):
        await consumer.stop()
    assert client.close.await_count == 2
    credential.close.assert_awaited_once()
    assert consumer.client is None
