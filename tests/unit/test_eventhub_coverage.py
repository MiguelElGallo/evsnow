"""Targeted unit tests to increase coverage in `consumers.eventhub`.

These tests exercise small logging/error-handling helpers directly and do not
require real Azure or Snowflake connectivity.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import UTC, datetime

import pytest

from consumers.eventhub import EventHubAsyncConsumer


def _make_consumer(sample_eventhub_config) -> EventHubAsyncConsumer:
    return EventHubAsyncConsumer(
        eventhub_config=sample_eventhub_config,
        target_db="TEST_DB",
        target_schema="TEST_SCHEMA",
        target_table="TEST_TABLE",
        message_processor=lambda _msgs: True,
    )


@pytest.mark.unit
def test_log_no_checkpoint_behavior_variants(sample_eventhub_config, caplog):
    consumer = _make_consumer(sample_eventhub_config)

    caplog.set_level(logging.INFO)

    consumer._log_no_checkpoint_behavior("-1")
    assert "No checkpoints found" in caplog.text
    assert "BEGINNING" in caplog.text

    caplog.clear()
    consumer._log_no_checkpoint_behavior("@latest")
    assert "LATEST" in caplog.text


@pytest.mark.unit
def test_log_rbac_permission_validation_notice(sample_eventhub_config, caplog):
    consumer = _make_consumer(sample_eventhub_config)

    caplog.set_level(logging.WARNING)
    consumer._log_rbac_permission_validation_notice()

    assert "RBAC Permission Validation" in caplog.text
    assert "Azure does NOT provide an API" in caplog.text


@pytest.mark.unit
def test_handle_receive_error_credential_error_fails_closed(
    sample_eventhub_config, monkeypatch
):
    consumer = _make_consumer(sample_eventhub_config)

    calls = {"count": 0}

    def fake_logfire_error(*_args, **_kwargs):
        calls["count"] += 1

    # Avoid real logfire side-effects
    monkeypatch.setattr("consumers.eventhub.logfire.error", fake_logfire_error, raising=False)

    exc = RuntimeError("Failed to retrieve a token from Azure CLI credential")
    handled = consumer._handle_receive_error(exc)

    assert handled is False
    assert calls["count"] == 1


@pytest.mark.unit
def test_handle_receive_error_rbac_raises(sample_eventhub_config):
    consumer = _make_consumer(sample_eventhub_config)

    exc = RuntimeError("Unauthorized (401) - not authorized")

    with pytest.raises(RuntimeError, match="Azure RBAC"):
        consumer._handle_receive_error(exc)


@pytest.mark.unit
def test_capture_writer_normalizes_bytes_keys(sample_eventhub_config, tmp_path):
    consumer = _make_consumer(sample_eventhub_config)

    timestamp_ns = time.time_ns()
    payload = {
        "timestamp_ns": timestamp_ns,
        "properties": {b"k": b"v", "nested": {b"inner": b"x"}},
        "system_properties": {b"sys": 1},
    }

    consumer._write_capture_file(tmp_path, timestamp_ns, payload)

    out_path = tmp_path / f"f_{timestamp_ns}.json"
    assert out_path.exists()

    loaded = json.loads(out_path.read_text(encoding="utf-8"))
    assert loaded["properties"]["k"] == "v"
    assert loaded["properties"]["nested"]["inner"] == "x"
    assert loaded["system_properties"]["sys"] == 1


@pytest.mark.unit
def test_event_body_to_utf8_handles_iterable_and_fallback():
    class DummyEvent:
        def __init__(self, body):
            self.body = body

    event = DummyEvent([b"hello", " ", b"world"])
    assert EventHubAsyncConsumer._event_body_to_utf8(event) == "hello world"

    class BadEvent:
        @property
        def body(self):
            raise ValueError("boom")

    assert EventHubAsyncConsumer._event_body_to_utf8(BadEvent()) == ""


@pytest.mark.unit
def test_to_jsonable_converts_sets_and_unknown(sample_eventhub_config):
    converted = EventHubAsyncConsumer._to_jsonable({b"a"})
    assert converted == ["a"]

    class Custom:
        def __str__(self):
            return "custom"

    assert EventHubAsyncConsumer._to_jsonable(Custom()) == "custom"


@pytest.mark.unit
def test_enqueue_capture_queue_full_increments_drop(sample_eventhub_config):
    consumer = _make_consumer(sample_eventhub_config)
    consumer.capture_messages = True
    consumer._capture_queue = asyncio.Queue(maxsize=1)
    consumer._capture_queue.put_nowait((0, {}))

    class DummyEvent:
        def __init__(self):
            self.offset = "1"
            self.sequence_number = 2
            self.enqueued_time = datetime.now(UTC)
            self.properties = {b"k": b"v"}
            self.system_properties = {"sys": "ok"}
            self.content_type = "application/json"
            self.body = b"payload"

    consumer._enqueue_capture("0", DummyEvent())

    assert consumer._capture_dropped_messages == 1


@pytest.mark.asyncio
async def test_capture_writer_loop_returns_when_queue_none(sample_eventhub_config):
    consumer = _make_consumer(sample_eventhub_config)
    await consumer._capture_writer_loop()
