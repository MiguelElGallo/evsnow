"""Targeted unit tests to increase coverage in `consumers.eventhub`.

These tests exercise small logging/error-handling helpers directly and do not
require real Azure or Snowflake connectivity.
"""

from __future__ import annotations

import logging

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

    caplog.clear()
    consumer._log_no_checkpoint_behavior("0")
    assert "EARLIEST" in caplog.text


@pytest.mark.unit
def test_log_rbac_permission_validation_notice(sample_eventhub_config, caplog):
    consumer = _make_consumer(sample_eventhub_config)

    caplog.set_level(logging.WARNING)
    consumer._log_rbac_permission_validation_notice()

    assert "RBAC Permission Validation" in caplog.text
    assert "Azure does NOT provide an API" in caplog.text


@pytest.mark.unit
def test_handle_receive_error_credential_error_is_swallowed(
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

    assert handled is True
    assert calls["count"] == 1


@pytest.mark.unit
def test_handle_receive_error_rbac_raises(sample_eventhub_config):
    consumer = _make_consumer(sample_eventhub_config)

    exc = RuntimeError("Unauthorized (401) - not authorized")

    with pytest.raises(RuntimeError, match="Azure RBAC"):
        consumer._handle_receive_error(exc)
