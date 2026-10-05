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

from consumers.eventhub import EventHubAsyncConsumer, EventHubMessage, MessageBatch
from pipeline.orchestrator import PipelineMapping
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
