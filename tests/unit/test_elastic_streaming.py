"""Elastic acknowledgement, ambiguous-outcome, lifecycle and replay contracts."""

import json
from concurrent.futures import Future
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from snowflake.ingest.streaming import StreamingIngestClient, StreamingIngestElasticChannel

from streaming.factory import create_snowflake_client
from streaming.snowflake_elastic import SnowflakeElasticStreamingClient
from streaming.snowflake_high_performance import SnowflakeHighPerformanceStreamingClient


@pytest.fixture
def elastic_client(sample_snowflake_config, sample_snowflake_connection_config, mocker):
    connection = sample_snowflake_connection_config.model_copy(
        update={"channel_mode": "elastic", "ack_timeout_seconds": 0.01}
    )
    sdk = MagicMock(spec=StreamingIngestClient)
    channel = MagicMock(spec=StreamingIngestElasticChannel)
    sdk.get_elastic_channel.return_value = channel
    mocker.patch("streaming.snowflake_elastic.StreamingIngestClient", return_value=sdk)
    client = SnowflakeElasticStreamingClient(sample_snowflake_config, connection, "test-elastic")
    client.start()
    return client, sdk, channel


@pytest.fixture
def raw_rows():
    return [
        {
            "eventhub_namespace": "example.servicebus.windows.net",
            "eventhub_name": "events",
            "consumer_group": "$Default",
            "partition_id": "0",
            "offset": str(index * 10),
            "sequence_number": index,
            "body": json.dumps({"event_id": str(index), "payload": {"value": index}}),
            "event_body": json.dumps({"event_id": str(index), "payload": {"value": index}}),
            "properties": '{"producer": "deterministic"}',
            "system_properties": "{}",
            "enqueued_time": "2026-10-05T19:30:00+00:00",
            "timestamp_ns": 123456,
            "ingestion_timestamp": datetime.now(UTC).isoformat(),
        }
        for index in range(3)
    ]


def completed_future(error=None):
    future = Future()
    if error is not None:
        future.set_exception(error)
    else:
        future.set_result(None)
    return future


def test_factory_preserves_named_default_and_selects_elastic(
    sample_snowflake_config, sample_snowflake_connection_config
):
    named = create_snowflake_client(sample_snowflake_config, sample_snowflake_connection_config)
    elastic = create_snowflake_client(
        sample_snowflake_config,
        sample_snowflake_connection_config.model_copy(update={"channel_mode": "elastic"}),
    )
    assert isinstance(named, SnowflakeHighPerformanceStreamingClient)
    assert isinstance(elastic, SnowflakeElasticStreamingClient)


def test_singleton_channel_and_complete_rows(elastic_client, raw_rows):
    client, sdk, channel = elastic_client
    channel.append_rows_with_wait.return_value = completed_future()
    client.start()
    assert client.ingest_batch("logical-p0", raw_rows, "0") is True
    sdk.get_elastic_channel.assert_called_once_with()
    sdk.open_channel.assert_not_called()
    assert channel.append_rows_with_wait.call_args.args[0] is raw_rows
    token = channel.append_rows_with_wait.call_args.kwargs["append_token"]
    assert isinstance(token, str) and len(token) == 32
    assert client.get_stats()["total_messages_sent"] == 3
    assert client.get_stats()["acknowledgement_scope"] == "durable_buffer"
    assert client.health_check()["buffer_ack_only"] is True
    assert client.health_check()["table_visibility_verified"] is False


@pytest.mark.parametrize("error", [RuntimeError("rejected append"), TimeoutError("SDK failed")])
def test_async_failure_is_failure_and_source_may_replay(elastic_client, raw_rows, error):
    client, _, channel = elastic_client
    channel.append_rows_with_wait.side_effect = [completed_future(error), completed_future()]
    assert client.ingest_batch("logical-p0", raw_rows) is False
    assert client.get_stats()["total_messages_sent"] == 0
    assert client.get_stats()["acknowledgement_failures"] == 1
    assert client.get_stats()["pending_append"] is False
    assert client.ingest_batch("logical-p0", raw_rows) is True
    assert channel.append_rows_with_wait.call_count == 2
    assert client.get_stats()["delivery_semantics"] == "at_least_once"


def test_wait_timeout_reobserves_original_future_and_counts_late_ack_once(elastic_client, raw_rows):
    client, _, channel = elastic_client
    pending = Future()
    channel.append_rows_with_wait.return_value = pending
    assert client.ingest_batch("logical-p0", raw_rows) is False
    assert client.ingest_batch("logical-p0", raw_rows) is False
    assert channel.append_rows_with_wait.call_count == 1
    assert not pending.cancelled()
    assert client.get_stats()["total_messages_sent"] == 0
    assert client.get_stats()["acknowledgement_timeouts"] == 2
    replay_rows = [
        dict(row, timestamp_ns=999, ingestion_timestamp="a later clock") for row in raw_rows
    ]
    pending.set_result(None)
    assert client.ingest_batch("logical-p0", replay_rows) is True
    assert client.ingest_batch("logical-p0", replay_rows) is True
    assert channel.append_rows_with_wait.call_count == 1
    assert client.get_stats()["total_messages_sent"] == 3
    assert client.get_stats()["total_batches_sent"] == 1


@pytest.mark.parametrize(
    "changed_field", ["eventhub_namespace", "eventhub_name", "body", "sequence_number"]
)
def test_another_source_or_payload_cannot_claim_pending_ack(
    elastic_client, raw_rows, changed_field
):
    client, _, channel = elastic_client
    pending = Future()
    channel.append_rows_with_wait.return_value = pending
    assert client.ingest_batch("logical-p0", raw_rows) is False
    changed_rows = [dict(row, **{changed_field: "a different value"}) for row in raw_rows]
    pending.set_result(None)
    assert client.ingest_batch("logical-p0", changed_rows) is False
    assert client.get_stats()["pending_append"] is True
    assert client.ingest_batch("logical-p0", raw_rows) is True
    assert channel.append_rows_with_wait.call_count == 1


def test_invalid_incoming_rows_do_not_discard_pending_append(elastic_client, raw_rows):
    client, _, channel = elastic_client
    pending = Future()
    channel.append_rows_with_wait.return_value = pending
    assert client.ingest_batch("logical-p0", raw_rows) is False
    assert client.ingest_batch("different", [{"body": object()}]) is False
    assert client.get_stats()["pending_append"] is True
    pending.set_result(None)
    assert client.ingest_batch("logical-p0", raw_rows) is True
    assert channel.append_rows_with_wait.call_count == 1


def test_completed_receipt_does_not_hide_distinct_payload(elastic_client, raw_rows):
    client, _, channel = elastic_client
    channel.append_rows_with_wait.return_value = completed_future()
    assert client.ingest_batch("logical-p0", raw_rows) is True
    changed_rows = [dict(row, body="changed producer event") for row in raw_rows]
    assert client.ingest_batch("logical-p0", changed_rows) is True
    assert channel.append_rows_with_wait.call_count == 2


def test_external_retry_does_not_wrap_ambiguous_append(
    sample_snowflake_config, sample_snowflake_connection_config, caplog
):
    retry_manager = MagicMock()
    client = SnowflakeElasticStreamingClient(
        sample_snowflake_config, sample_snowflake_connection_config, retry_manager=retry_manager
    )
    retry_manager.get_retry_decorator.assert_not_called()
    assert "does not apply the external retry decorator" in caplog.text
    assert client.ingest_batch("logical", [{"body": "event"}]) is False


def test_stop_observes_pending_without_reappend_and_bounds_sdk_close(elastic_client, raw_rows):
    client, sdk, channel = elastic_client
    pending = Future()
    channel.append_rows_with_wait.return_value = pending
    assert client.ingest_batch("logical-p0", raw_rows) is False
    pending.set_result(None)
    client.stop()
    client.stop()
    assert channel.append_rows_with_wait.call_count == 1
    sdk.close.assert_called_once()
    close_args = sdk.close.call_args.kwargs
    assert close_args["wait_for_flush"] is True
    assert 0 < close_args["timeout_seconds"] <= client.connection_config.close_timeout_seconds
    assert not client.is_started
    assert client.get_stats()["total_messages_sent"] == 3


def test_stop_timeout_does_not_cancel_unknown_append(elastic_client, raw_rows, mocker):
    client, sdk, channel = elastic_client
    pending = Future()
    channel.append_rows_with_wait.return_value = pending
    assert client.ingest_batch("logical-p0", raw_rows) is False
    mocker.patch.object(pending, "result", side_effect=TimeoutError())
    mocker.patch("streaming.snowflake_elastic.time.monotonic", side_effect=[0.0, 0.0, 61.0])
    with pytest.raises(RuntimeError, match="shutdown was incomplete"):
        client.stop()
    sdk.close.assert_called_once_with(wait_for_flush=False, timeout_seconds=0)
    assert not pending.cancelled()
    assert channel.append_rows_with_wait.call_count == 1
    assert client.get_stats()["total_messages_sent"] == 0


def test_start_failure_closes_partially_created_client(
    sample_snowflake_config, sample_snowflake_connection_config, mocker
):
    sdk = MagicMock(spec=StreamingIngestClient)
    sdk.get_elastic_channel.side_effect = RuntimeError("unavailable")
    mocker.patch("streaming.snowflake_elastic.StreamingIngestClient", return_value=sdk)
    client = SnowflakeElasticStreamingClient(
        sample_snowflake_config, sample_snowflake_connection_config
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        client.start()
    sdk.close.assert_called_once()
    assert not client.is_started


def test_restart_cannot_claim_in_memory_exactly_once(elastic_client, raw_rows):
    client, sdk, channel = elastic_client
    channel.append_rows_with_wait.return_value = completed_future()
    assert client.ingest_batch("logical-p0", raw_rows) is True
    client.stop()
    client.start()
    assert client.ingest_batch("logical-p0", raw_rows) is True
    assert sdk.get_elastic_channel.call_count == 2
    assert channel.append_rows_with_wait.call_count == 2
    assert client.get_stats()["total_messages_sent"] == 6
