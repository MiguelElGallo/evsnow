"""Prove a configured Elastic pipeline with deterministic events and source replay.

Requires dedicated, empty Event Hubs/Snowflake test resources. Does not provision
resources. A shell/CI cleanup trap must delete the dedicated Azure resource group.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import platform
import time
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

import logfire
import snowflake.connector
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubConsumerClient, EventHubProducerClient
from azure.identity.aio import AzureCliCredential

from consumers.messages import EventHubMessage
from pipeline.orchestrator import PipelineMapping
from utils.config import EvSnowConfig, load_config
from utils.snowflake import close_all_cached_connections


def payload(run_id: str, event_id: int) -> dict[str, Any]:
    """Create a payload whose contents can be checked entirely in SQL."""
    return {
        "run_id": run_id,
        "event_id": event_id,
        "value": event_id * 17 + 3,
        "message": f"elastic-{event_id:06d}",
    }


def sql_rows(connection: Any, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """Execute a query and retain named, JSON-serializable results."""
    with connection.cursor(snowflake.connector.DictCursor) as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()


async def publish(
    namespace: str, hub: str, run_id: str, first: int, end: int, rate: int
) -> dict[str, Any]:
    """Send source events at a bounded rate in Basic-compatible AMQP batches."""
    credential = AzureCliCredential()
    producer = EventHubProducerClient(
        fully_qualified_namespace=namespace, eventhub_name=hub, credential=credential
    )
    sent = 0
    started = time.monotonic()
    try:
        async with producer:
            for chunk_start in range(first, end, 400):
                chunk_end = min(end, chunk_start + 400)
                for partition in (0, 1):
                    batch = await producer.create_batch(
                        partition_id=str(partition), max_size_in_bytes=240 * 1024
                    )
                    for event_id in range(chunk_start, chunk_end):
                        if event_id % 2 != partition:
                            continue
                        event = EventData(json.dumps(payload(run_id, event_id)))
                        event.content_type = "application/json"
                        try:
                            batch.add(event)
                        except ValueError:
                            await producer.send_batch(batch)
                            batch = await producer.create_batch(
                                partition_id=str(partition), max_size_in_bytes=240 * 1024
                            )
                            batch.add(event)
                        sent += 1
                    if len(batch):
                        await producer.send_batch(batch)
                await asyncio.sleep(max(0, sent / rate - (time.monotonic() - started)))
                if sent % 20000 == 0 or chunk_end == end:
                    print(f"Published {sent:,} source events in this stage", flush=True)
    finally:
        await credential.close()
    return {"sent": sent, "seconds": round(time.monotonic() - started, 3), "rate_limit": rate}


def verification(
    connection: Any, table: str, namespace: str, hub: str, run_id: str, count: int
) -> dict[str, Any]:
    """Check ID coverage, deterministic contents and source identity independently of ack."""
    rows = sql_rows(
        connection,
        f"""WITH data AS (
          SELECT TRY_PARSE_JSON(event_body) body,eventhub_namespace,eventhub_name,
                 partition_id,sequence_number FROM {table}
        ), run AS (
          SELECT *,TRY_TO_NUMBER(body:event_id::VARCHAR) id FROM data
          WHERE body:run_id::VARCHAR=%s
        ) SELECT COUNT(*) RAW_ROWS,COUNT(DISTINCT id) DISTINCT_IDS,
          COALESCE(COUNT_IF(id IS NULL OR id<0 OR id>=%s),0) INVALID_IDS,
          COALESCE(COUNT_IF(TRY_TO_NUMBER(body:value::VARCHAR) IS NULL
            OR TRY_TO_NUMBER(body:value::VARCHAR)<>id*17+3
            OR body:message::VARCHAR IS NULL
            OR body:message::VARCHAR<>'elastic-'||LPAD(id::VARCHAR,6,'0')),0) INVALID_PAYLOADS,
          COALESCE(COUNT_IF(eventhub_namespace IS NULL OR eventhub_namespace<>%s
            OR eventhub_name IS NULL OR eventhub_name<>%s
            OR partition_id IS NULL OR partition_id<>MOD(id,2)::VARCHAR
            OR sequence_number IS NULL OR sequence_number<>FLOOR(id/2)),0) INVALID_SOURCE_METADATA,
          COUNT(DISTINCT eventhub_namespace||'/'||eventhub_name||'/'||partition_id||'/'||sequence_number)
            DISTINCT_SOURCE_IDS FROM run""",
        (run_id, count, namespace, hub),
    )[0]
    rows["MISSING_IDS"] = count - rows["DISTINCT_IDS"]
    rows["DUPLICATE_ROWS"] = rows["RAW_ROWS"] - rows["DISTINCT_IDS"]
    rows["PROCESSING_ERRORS"] = sql_rows(
        connection, f"SELECT COUNT(*) N FROM ERROR_TABLE({table})"
    )[0]["N"]
    rows["ALL_TABLE_ROWS"] = sql_rows(connection, f"SELECT COUNT(*) N FROM {table}")[0]["N"]
    return rows


async def wait_for_visibility(
    connection: Any,
    table: str,
    namespace: str,
    hub: str,
    run_id: str,
    count: int,
    raw_count: int,
    timeout: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        result = await asyncio.to_thread(
            verification, connection, table, namespace, hub, run_id, count
        )
        if result["RAW_ROWS"] >= raw_count and result["DISTINCT_IDS"] >= count:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Target visibility deadline exceeded: {result}")
        await asyncio.sleep(5)


async def pipeline_stage(
    config: EvSnowConfig, run_id: str, first: int, end: int, rate: int, timeout: float
) -> dict[str, Any]:
    """Run the production mapping and capture restart/checkpoint evidence."""
    first_messages: dict[str, dict[str, int]] = {}

    class ObservedMapping(PipelineMapping):
        def _process_messages(self, messages: list[EventHubMessage]) -> bool:
            for message in messages:
                first_messages.setdefault(
                    message.partition_id,
                    {
                        "sequence_number": message.sequence_number,
                        "event_id": json.loads(message.body)["event_id"],
                    },
                )
            return super()._process_messages(messages)

    mapping = ObservedMapping(config.mappings[0], config)
    await asyncio.to_thread(mapping.start)
    streaming_client = mapping.snowflake_client
    eventhub_consumer = mapping.eventhub_consumer
    assert mapping.eventhub_config is not None
    task = asyncio.create_task(mapping.start_async())
    started = time.monotonic()
    try:
        producer = await asyncio.wait_for(
            publish(
                mapping.eventhub_config.namespace,
                mapping.eventhub_config.name,
                run_id,
                first,
                end,
                rate,
            ),
            timeout=timeout,
        )
        deadline = time.monotonic() + timeout
        while mapping.stats["messages_processed"] < end - first:
            if task.done():
                task.result()
                raise RuntimeError("Pipeline exited before processing all source events")
            if mapping.stats["errors"]:
                raise RuntimeError(f"Pipeline ingestion failed: {mapping.stats['errors']}")
            if time.monotonic() > deadline:
                raise TimeoutError(f"Pipeline drain deadline exceeded: {mapping.get_stats()}")
            await asyncio.sleep(1)
    finally:
        await mapping.stop()
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return {
        "producer": producer,
        "seconds": round(time.monotonic() - started, 3),
        "first_messages": first_messages,
        "mapping_stats": mapping.get_stats(),
        "streaming_stats": streaming_client.get_stats() if streaming_client else {},
        "consumer_stats": eventhub_consumer.get_stats() if eventhub_consumer else {},
    }


async def replay_source(config: EvSnowConfig, per_partition: int = 20) -> dict[str, Any]:
    """Read the retained source tail and append it again without changing primary checkpoints."""
    hub = config.get_event_hub_config(config.mappings[0].event_hub_key)
    assert hub is not None
    credential = AzureCliCredential()
    consumer = EventHubConsumerClient(
        fully_qualified_namespace=hub.namespace,
        eventhub_name=hub.name,
        consumer_group=hub.consumer_group,
        credential=credential,
    )
    messages: dict[tuple[str, int], EventHubMessage] = {}
    ready = asyncio.Event()
    properties = await consumer.get_eventhub_properties()
    starts: dict[str, int] = {}
    for partition in properties["partition_ids"]:
        status = await consumer.get_partition_properties(partition)
        starts[partition] = status["last_enqueued_sequence_number"] - per_partition

    async def on_event(context: Any, event: EventData | None) -> None:
        if event is not None and event.sequence_number is not None:
            key = (context.partition_id, event.sequence_number)
            messages[key] = EventHubMessage(
                event,
                context.partition_id,
                event.sequence_number,
                hub.namespace,
                hub.name,
                hub.consumer_group,
            )
            if len(messages) >= per_partition * len(starts):
                ready.set()

    task = asyncio.create_task(consumer.receive(on_event=on_event, starting_position=starts))
    try:
        await asyncio.wait_for(ready.wait(), timeout=120)
    finally:
        await consumer.close()
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await credential.close()
    mapping = PipelineMapping(config.mappings[0], config)
    await asyncio.to_thread(mapping.start)
    try:
        success = await asyncio.to_thread(mapping._process_messages, list(messages.values()))
        if not success:
            raise RuntimeError("Source replay append failed")
        return {"replayed_source_events": len(messages), "source_starts_exclusive": starts}
    finally:
        await mapping.stop()


async def run(args: argparse.Namespace) -> None:
    config = load_config(env_file=str(args.env_file), config_file=str(args.config_file))
    if len(config.mappings) != 1 or config.snowflake_connection is None:
        raise ValueError("The live proof requires exactly one mapping and Snowflake connection")
    connection_config = config.snowflake_connection
    if connection_config.channel_mode != "elastic":
        raise ValueError("The proof requires channel_mode=elastic; Named mode is not a substitute")
    if args.events < 4 or args.events % 4:
        raise ValueError("Event count must be positive and divisible by four")
    if not 1 <= args.rate <= 800:
        raise ValueError("Rate must be 1..800 events/sec for this Basic-tier proof")
    target = config.get_snowflake_config(config.mappings[0].snowflake_key)
    hub = config.get_event_hub_config(config.mappings[0].event_hub_key)
    assert target is not None and hub is not None
    table = f"{target.database}.{target.schema_name}.{target.table_name}"
    control = f"{config.target_db}.{config.target_schema}.{config.target_table}"
    evidence: dict[str, Any] = {
        "started_at": datetime.now(UTC).isoformat(),
        "run_id": args.run_id,
        "events": args.events,
        "channel_mode": "elastic",
        "target": table,
        "namespace": hub.namespace,
        "event_hub": hub.name,
        "versions": {
            package: version(package)
            for package in (
                "evsnow",
                "snowpipe-streaming",
                "snowflake-connector-python",
                "azure-eventhub",
                "azure-identity",
                "aiohttp",
                "pydantic",
                "pydantic-settings",
                "cryptography",
                "logfire",
                "pydantic-ai",
                "typer",
            )
        },
        "python": platform.python_version(),
        "stages": [],
    }
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    output = args.evidence_dir / "live-test.json"
    connection = snowflake.connector.connect(
        account=connection_config.account,
        user=connection_config.user,
        private_key_file=connection_config.private_key_file,
        private_key_file_pwd=connection_config.private_key_password,
        role=connection_config.role,
        warehouse=connection_config.warehouse,
        database=target.database,
        schema=target.schema_name,
        login_timeout=60,
        network_timeout=60,
        session_parameters={"STATEMENT_TIMEOUT_IN_SECONDS": 60},
    )
    try:
        if sql_rows(connection, f"SELECT COUNT(*) N FROM {table}")[0]["N"]:
            raise ValueError("The dedicated target must be empty before this proof")
        if sql_rows(connection, f"SELECT COUNT(*) N FROM {control}")[0]["N"]:
            raise ValueError("The dedicated checkpoint table must be empty before this proof")
        for first, end in ((0, args.events // 2), (args.events // 2, args.events)):
            stage = await pipeline_stage(config, args.run_id, first, end, args.rate, args.timeout)
            stage["checkpoints"] = sql_rows(connection, f"SELECT * FROM {control}")
            expected_first = first // 2
            if set(stage["first_messages"]) != {"0", "1"} or any(
                observed["sequence_number"] != expected_first
                or observed["event_id"] != first + int(partition)
                for partition, observed in stage["first_messages"].items()
            ):
                raise AssertionError(
                    f"Unexpected source resume positions: {stage['first_messages']}"
                )
            stage["verification"] = await wait_for_visibility(
                connection, table, hub.namespace, hub.name, args.run_id, end, end, args.timeout
            )
            evidence["stages"].append(stage)
            output.write_text(json.dumps(evidence, indent=2, default=str) + "\n")
            print(f"Stage {end:,}: {stage['verification']}", flush=True)
        evidence["before_replay"] = evidence["stages"][-1]["verification"]
        if evidence["before_replay"]["DUPLICATE_ROWS"] != 0:
            raise AssertionError("Clean persisted restart unexpectedly duplicated rows")
        replay = await replay_source(config, per_partition=min(20, args.events // 2))
        evidence["replay"] = replay
        final = await wait_for_visibility(
            connection,
            table,
            hub.namespace,
            hub.name,
            args.run_id,
            args.events,
            args.events + replay["replayed_source_events"],
            args.timeout,
        )
        evidence["after_replay"] = final
        for field in (
            "MISSING_IDS",
            "INVALID_IDS",
            "INVALID_PAYLOADS",
            "INVALID_SOURCE_METADATA",
            "PROCESSING_ERRORS",
        ):
            if final[field] != 0:
                raise AssertionError(f"Acceptance failed: {final}")
        if final["DISTINCT_IDS"] != args.events or final["DISTINCT_SOURCE_IDS"] != args.events:
            raise AssertionError(f"Source/event identity coverage failed: {final}")
        if final["DUPLICATE_ROWS"] != replay["replayed_source_events"]:
            raise AssertionError(f"Replay behavior differed from expected: {final}")
        if final["ALL_TABLE_ROWS"] != final["RAW_ROWS"]:
            raise AssertionError("Target contains rows from an unexpected run")
        evidence["status"] = "passed"
        print(f"PASS: {final}", flush=True)
    except BaseException as error:
        evidence["status"] = "failed"
        evidence["error"] = str(error)
        raise
    finally:
        evidence["finished_at"] = datetime.now(UTC).isoformat()
        output.write_text(json.dumps(evidence, indent=2, default=str) + "\n")
        connection.close()
        close_all_cached_connections()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-file", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--events", type=int, default=200000)
    parser.add_argument("--rate", type=int, default=800)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    logfire.configure(send_to_logfire=False, console=False)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
