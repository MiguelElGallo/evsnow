"""Exercise live-proof replay against a retained, partitioned source without cloud I/O."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from consumers.messages import EventHubMessage
from utils.config import EvSnowConfig, SnowflakeConnectionConfig


@pytest.mark.asyncio
@pytest.mark.parametrize("event_count", range(4, 45, 4))
async def test_live_proof_replays_only_available_retained_events(
    event_count: int,
    sample_eventhub_config: Any,
    sample_snowflake_config: Any,
    sample_snowflake_connection_config: Any,
    sample_mapping: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Small accepted runs finish; larger runs still select the last 20 per partition."""
    script = Path(__file__).resolve().parents[2] / "tools" / "elastic_live_test.py"
    spec = importlib.util.spec_from_file_location("elastic_live_test", script)
    assert spec is not None and spec.loader is not None
    harness = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(harness)
    monkeypatch.chdir(tmp_path)
    for name in list(os.environ):
        if name.startswith(("SNOWFLAKE_", "EVENTHUB", "CONTROL", "TARGET")):
            monkeypatch.delenv(name, raising=False)
    connection_config = SnowflakeConnectionConfig(
        **(sample_snowflake_connection_config.model_dump() | {"channel_mode": "elastic"})
    )
    config = EvSnowConfig(
        eventhub_namespace=sample_eventhub_config.namespace,
        event_hubs={"EVENTHUBNAME_1": sample_eventhub_config.model_dump()},
        snowflake_configs={"SNOWFLAKE_1": sample_snowflake_config.model_dump()},
        snowflake_connection=connection_config,
        mappings=[sample_mapping.model_dump()],
        _source_env={},
    )
    run_id = f"small-replay-{event_count}"
    source: dict[str, list[Any]] = {"0": [], "1": []}
    target: list[EventHubMessage] = []
    replayed: list[EventHubMessage] = []
    checkpoints: dict[str, int] = {}
    received_starts: dict[str, int] = {}
    credential = SimpleNamespace(close=AsyncMock())

    class RetainedConsumer:
        """Model Azure's exclusive integer sequence selectors, including -1."""

        closed = False

        async def get_eventhub_properties(self) -> dict[str, Any]:
            return {"partition_ids": list(source)}

        async def get_partition_properties(self, partition: str) -> dict[str, int]:
            return {"last_enqueued_sequence_number": len(source[partition]) - 1}

        async def receive(self, *, on_event: Any, starting_position: dict[str, int]) -> None:
            received_starts.update(starting_position)
            for partition, events in source.items():
                context = SimpleNamespace(partition_id=partition)
                for event in events:
                    if event.sequence_number > starting_position[partition]:
                        await on_event(context, event)
            await asyncio.Event().wait()

        async def close(self) -> None:
            self.closed = True

    consumer = RetainedConsumer()

    class ReplayMapping:
        """Capture the messages selected by the production replay callback."""

        stopped = False

        def start(self) -> None:
            pass

        def _process_messages(self, messages: list[EventHubMessage]) -> bool:
            replayed.extend(messages)
            target.extend(messages)
            return True

        async def stop(self) -> None:
            self.stopped = True

    mapping = ReplayMapping()

    async def stage(
        stage_config: EvSnowConfig,
        stage_run_id: str,
        first: int,
        end: int,
        rate: int,
        timeout: float,
    ) -> dict[str, Any]:
        assert stage_config is config and stage_run_id == run_id
        first_messages: dict[str, dict[str, int]] = {}
        for event_id in range(first, end):
            partition = str(event_id % 2)
            sequence = len(source[partition])
            event = MagicMock()
            event.body_as_str.return_value = json.dumps(harness.payload(run_id, event_id))
            event.sequence_number = sequence
            event.enqueued_time = datetime(2026, 10, 5, tzinfo=UTC)
            event.properties = {}
            event.system_properties = {}
            event.offset = str(sequence)
            event.content_type = "application/json"
            source[partition].append(event)
            target.append(
                EventHubMessage(
                    event,
                    partition,
                    sequence,
                    sample_eventhub_config.namespace,
                    sample_eventhub_config.name,
                    sample_eventhub_config.consumer_group,
                )
            )
            first_messages.setdefault(
                partition, {"sequence_number": sequence, "event_id": event_id}
            )
            checkpoints[partition] = sequence
        return {"first_messages": first_messages}

    def verify(
        connection: Any,
        table: str,
        namespace: str,
        hub: str,
        requested_run_id: str,
        count: int,
    ) -> dict[str, int]:
        bodies = [json.loads(message.body) for message in target]
        ids = {body["event_id"] for body in bodies}
        source_ids = {(message.partition_id, message.sequence_number) for message in target}
        assert requested_run_id == run_id
        assert all(body == harness.payload(run_id, body["event_id"]) for body in bodies)
        assert all(
            message.partition_id == str(body["event_id"] % 2)
            and message.sequence_number == body["event_id"] // 2
            and message.eventhub_namespace == namespace
            and message.eventhub_name == hub
            for message, body in zip(target, bodies, strict=True)
        )
        return {
            "RAW_ROWS": len(target),
            "DISTINCT_IDS": len(ids),
            "DISTINCT_SOURCE_IDS": len(source_ids),
            "MISSING_IDS": count - len(ids),
            "DUPLICATE_ROWS": len(target) - len(ids),
            "INVALID_IDS": 0,
            "INVALID_PAYLOADS": 0,
            "INVALID_SOURCE_METADATA": 0,
            "PROCESSING_ERRORS": 0,
            "ALL_TABLE_ROWS": len(target),
        }

    def sql_rows(connection: Any, query: str) -> list[dict[str, Any]]:
        if query.startswith("SELECT COUNT(*)"):
            return [{"N": 0}]
        return [
            {"PARTITION_ID": partition, "SEQUENCE_NUMBER": sequence}
            for partition, sequence in checkpoints.items()
        ]

    snowflake_connection = MagicMock()
    monkeypatch.setattr(harness, "load_config", lambda **kwargs: config)
    monkeypatch.setattr(harness, "pipeline_stage", stage)
    monkeypatch.setattr(harness, "verification", verify)
    monkeypatch.setattr(harness, "sql_rows", sql_rows)
    monkeypatch.setattr(harness, "AzureCliCredential", lambda: credential)
    monkeypatch.setattr(harness, "EventHubConsumerClient", lambda **kwargs: consumer)
    monkeypatch.setattr(harness, "PipelineMapping", lambda *args: mapping)
    monkeypatch.setattr(
        harness.snowflake.connector, "connect", lambda **kwargs: snowflake_connection
    )
    monkeypatch.setattr(harness, "close_all_cached_connections", lambda: None)
    args = argparse.Namespace(
        config_file=tmp_path / "config.toml",
        env_file=tmp_path / ".env",
        run_id=run_id,
        events=event_count,
        rate=800,
        timeout=1,
        evidence_dir=tmp_path / "evidence",
    )

    # Bound regressions that request more retained events than the source contains.
    await asyncio.wait_for(harness.run(args), timeout=1)

    tail_length = min(20, event_count // 2)
    first_sequence = event_count // 2 - tail_length
    assert received_starts == {"0": first_sequence - 1, "1": first_sequence - 1}
    assert {(message.partition_id, message.sequence_number) for message in replayed} == {
        (partition, sequence)
        for partition in ("0", "1")
        for sequence in range(first_sequence, event_count // 2)
    }
    assert len(replayed) == 2 * tail_length
    assert checkpoints == {"0": event_count // 2 - 1, "1": event_count // 2 - 1}
    evidence = json.loads((args.evidence_dir / "live-test.json").read_text())
    assert evidence["status"] == "passed"
    assert evidence["replay"]["replayed_source_events"] == len(replayed)
    assert evidence["after_replay"]["DUPLICATE_ROWS"] == len(replayed)
    assert consumer.closed and mapping.stopped
    credential.close.assert_awaited_once()
    snowflake_connection.close.assert_called_once()
