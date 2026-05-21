"""
Performance benchmarks for evsnow core functions.

These benchmarks target pure, CPU-bound functions that are on the hot path
of the event processing pipeline: message serialization, bytes conversion,
batch management, config validation, and Pydantic model construction.
"""

import json
import time
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from consumers.messages import (
    BytesEncoder,
    MessageBatch,
    EventHubMessage,
    _convert_bytes_to_str,
)
from utils.config import EvSnowConfig
from utils.smart_retry import RetryDecision


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_event_data(body: dict, partition_id: str = "0", seq: int = 100):
    """Create a mock EventData without importing azure SDK internals."""
    mock = MagicMock()
    mock.body_as_str.return_value = json.dumps(body)
    mock.enqueued_time = datetime.now(UTC)
    mock.properties = {"key": "value"}
    mock.system_properties = {"offset": "12345", "sequence_number": seq}
    mock.offset = "12345"
    mock.sequence_number = seq
    mock.content_type = "application/json"
    return mock


# ---------------------------------------------------------------------------
# Benchmarks: _convert_bytes_to_str (hot path in message ingestion)
# ---------------------------------------------------------------------------


def test_convert_bytes_to_str_flat(benchmark):
    """Benchmark bytes-to-str conversion on a flat dictionary."""
    data = {
        b"key1": b"value1",
        b"key2": b"value2",
        b"key3": b"value3",
        "mixed": b"bytes_value",
    }
    result = benchmark(_convert_bytes_to_str, data)
    assert isinstance(result["key1"], str)


def test_convert_bytes_to_str_nested(benchmark):
    """Benchmark bytes-to-str on a deeply nested structure."""
    data = {
        b"level1": {
            b"level2": {
                b"level3": [b"a", b"b", b"c", {b"deep": b"value"}],
            },
            "list": [b"x", b"y", b"z"],
        },
        "plain": "no conversion needed",
        b"tuple": (b"t1", b"t2"),
    }
    result = benchmark(_convert_bytes_to_str, data)
    assert isinstance(result["level1"]["level2"]["level3"][0], str)


def test_convert_bytes_to_str_large_list(benchmark):
    """Benchmark bytes-to-str on a large list of byte strings."""
    data = [f"item-{i}".encode() for i in range(500)]
    result = benchmark(_convert_bytes_to_str, data)
    assert len(result) == 500
    assert isinstance(result[0], str)


# ---------------------------------------------------------------------------
# Benchmarks: BytesEncoder (JSON serialization with bytes handling)
# ---------------------------------------------------------------------------


def test_bytes_encoder_small_payload(benchmark):
    """Benchmark JSON encoding a small payload with bytes values."""
    data = {
        "string_key": "value",
        "bytes_key": b"bytes_value",
        "nested": {"inner": b"inner_bytes"},
    }

    def encode():
        return json.dumps(data, cls=BytesEncoder)

    result = benchmark(encode)
    assert "bytes_value" in result


def test_bytes_encoder_large_payload(benchmark):
    """Benchmark JSON encoding a larger payload mimicking real event data."""
    data = {
        "events": [
            {
                "id": f"evt-{i}",
                "body": f"payload-{i}",
                "properties": {f"prop-{j}": f"val-{j}" for j in range(5)},
            }
            for i in range(100)
        ],
        "metadata": {"source": "benchmark", "count": 100},
    }

    def encode():
        return json.dumps(data, cls=BytesEncoder)

    result = benchmark(encode)
    parsed = json.loads(result)
    assert len(parsed["events"]) == 100


# ---------------------------------------------------------------------------
# Benchmarks: MessageBatch operations (core batching logic)
# ---------------------------------------------------------------------------


def test_message_batch_add_messages(benchmark):
    """Benchmark adding messages to a MessageBatch."""
    events = [
        _make_mock_event_data({"id": i, "data": f"payload-{i}"}, str(i % 4), i) for i in range(200)
    ]
    messages = [
        EventHubMessage(event_data=e, partition_id=str(i % 4), sequence_number=i)
        for i, e in enumerate(events)
    ]

    def add_all():
        batch = MessageBatch(max_size=1000, max_wait_seconds=300)
        for msg in messages:
            batch.add_message(msg)
        return batch

    result = benchmark(add_all)
    assert len(result.messages) == 200


def test_message_batch_to_dict_list(benchmark):
    """Benchmark converting a full batch to list of dicts (serialization path)."""
    events = [_make_mock_event_data({"id": i, "value": i * 10}, str(i % 4), i) for i in range(50)]
    messages = [
        EventHubMessage(event_data=e, partition_id=str(i % 4), sequence_number=i)
        for i, e in enumerate(events)
    ]
    batch = MessageBatch(max_size=1000)
    for msg in messages:
        batch.add_message(msg)

    result = benchmark(batch.to_dict_list)
    assert len(result) == 50
    assert "event_body" in result[0]


def test_message_batch_checkpoint_data(benchmark):
    """Benchmark extracting checkpoint data from a batch."""
    events = [_make_mock_event_data({"id": i}, str(i % 8), i) for i in range(100)]
    messages = [
        EventHubMessage(event_data=e, partition_id=str(i % 8), sequence_number=i)
        for i, e in enumerate(events)
    ]
    batch = MessageBatch(max_size=1000)
    for msg in messages:
        batch.add_message(msg)

    result = benchmark(batch.get_checkpoint_data)
    assert len(result) == 8


# ---------------------------------------------------------------------------
# Benchmarks: EventHubMessage.to_dict (per-message serialization)
# ---------------------------------------------------------------------------


def test_eventhub_message_to_dict(benchmark):
    """Benchmark single message to_dict conversion."""
    event = _make_mock_event_data(
        {"source": "perf-test", "timestamp": "2024-01-01T00:00:00Z", "value": 42}
    )
    msg = EventHubMessage(
        event_data=event,
        partition_id="0",
        sequence_number=1,
        eventhub_namespace="test.servicebus.windows.net",
        eventhub_name="perf-hub",
        consumer_group="$Default",
    )
    result = benchmark(msg.to_dict)
    assert "event_body" in result
    assert "partition_id" in result


# ---------------------------------------------------------------------------
# Benchmarks: Config validation helpers (called during startup + channel ops)
# ---------------------------------------------------------------------------


def test_sanitize_channel_segment(benchmark):
    """Benchmark channel name segment sanitization."""
    inputs = [
        "my-event-hub",
        "hub with spaces & special!chars@#$",
        "normal_name",
        "---leading-trailing---",
        "UPPER.case.dots",
    ]

    def sanitize_all():
        return [EvSnowConfig._sanitize_channel_segment(s) for s in inputs]

    results = benchmark(sanitize_all)
    assert all(isinstance(r, str) for r in results)
    assert all(r for r in results)


def test_normalize_postgres_identifier(benchmark):
    """Benchmark Postgres identifier normalization."""
    inputs = [
        "CONTROL",
        "PUBLIC",
        '"QuotedName"',
        "  SPACES  ",
        "lowercase",
        '"Already_Lower"',
    ]

    def normalize_all():
        return [EvSnowConfig._normalize_postgres_identifier(s) for s in inputs]

    results = benchmark(normalize_all)
    assert results[0] == "control"
    assert results[2] == "QuotedName"


# ---------------------------------------------------------------------------
# Benchmarks: Pydantic model construction (RetryDecision)
# ---------------------------------------------------------------------------


def test_retry_decision_model_creation(benchmark):
    """Benchmark Pydantic RetryDecision model instantiation."""

    def create_decisions():
        decisions = []
        for i in range(50):
            decisions.append(
                RetryDecision(
                    should_retry=i % 2 == 0,
                    reasoning=f"Test reasoning for attempt {i}",
                    suggested_wait_seconds=min(i + 1, 60),
                    confidence=round(i / 50, 2),
                )
            )
        return decisions

    results = benchmark(create_decisions)
    assert len(results) == 50
    assert results[0].should_retry is True


def test_retry_decision_model_validation(benchmark):
    """Benchmark RetryDecision model validation with edge-case inputs."""

    def validate():
        return RetryDecision(
            should_retry=True,
            reasoning="a" * 500,
            suggested_wait_seconds=60,
            confidence=1.0,
        )

    result = benchmark(validate)
    assert result.confidence == 1.0
