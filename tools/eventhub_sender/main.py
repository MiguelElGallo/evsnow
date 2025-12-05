"""
CLI utility to send sequentially numbered JSON messages to an Event Hub.

Uses either an Event Hub connection string or DefaultAzureCredential
against a fully qualified namespace. Each message carries a monotonic
`sequence_id` so downstream ingestion checks can verify no IDs are missing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv

app = typer.Typer(help="Send sequential JSON messages to an Event Hub for ingestion checks.")

logger = logging.getLogger("evsnow.tools.eventhub_sender")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def _load_env(env_file: Path | None) -> None:
    """Load .env values so CLI defaults come from the shared config."""
    if env_file is None:
        env_file = Path(".env")
    load_dotenv(env_file)


async def _build_producer(
    connection_string: str | None,
    namespace: str | None,
    eventhub: str,
) -> EventHubProducerClient:
    """Create an Event Hub producer using connection string or DefaultAzureCredential."""
    if connection_string:
        logger.info("Using connection string authentication")
        return EventHubProducerClient.from_connection_string(
            conn_str=connection_string,
            eventhub_name=eventhub,
        )

    if not namespace:
        raise typer.BadParameter("Namespace is required when no connection string is provided")

    logger.info("Using DefaultAzureCredential for authentication")
    credential = DefaultAzureCredential()
    return EventHubProducerClient(
        fully_qualified_namespace=namespace,
        eventhub_name=eventhub,
        credential=credential,
    )


def _build_payload(sequence_id: int, base_payload: dict[str, Any]) -> str:
    """Assemble the JSON payload with a fail-safe sequence id."""
    envelope = {
        "sequence_id": sequence_id,
        "sent_at": datetime.now(UTC).isoformat(),
        "source": "evsnow-cli-sender",
        "trace_id": uuid.uuid4().hex,
        "payload": base_payload,
    }
    return json.dumps(envelope)


async def send_messages(
    connection_string: str | None,
    namespace: str | None,
    eventhub: str,
    count: int,
    start_id: int,
    batch_size: int,
    interval_seconds: float,
    partition_key: str | None,
    payload_json: str | None,
) -> None:
    base_payload: dict[str, Any] = {}
    if payload_json:
        try:
            base_payload = json.loads(payload_json)
            if not isinstance(base_payload, dict):
                raise ValueError("Payload JSON must be an object")
        except Exception as err:
            raise typer.BadParameter(f"Invalid JSON for --payload: {err}") from err

    producer = await _build_producer(connection_string, namespace, eventhub)
    async with producer:
        batch = await producer.create_batch(partition_key=partition_key)
        batch_count = 0
        sent = 0
        sequence_id = start_id
        start_time = time.time()

        while sent < count:
            message_body = _build_payload(sequence_id, base_payload)
            event = EventData(message_body)

            try:
                batch.add(event)
                batch_count += 1
            except ValueError:
                logger.info("Sending batch of %d messages", batch_count)
                await producer.send_batch(batch)
                batch = await producer.create_batch(partition_key=partition_key)
                batch.add(event)
                batch_count = 1

            if batch_count >= batch_size:
                logger.info("Sending batch of %d messages (size threshold)", batch_count)
                await producer.send_batch(batch)
                batch = await producer.create_batch(partition_key=partition_key)
                batch_count = 0

            sent += 1
            sequence_id += 1

            if interval_seconds > 0:
                await asyncio.sleep(interval_seconds)

        if batch_count > 0:
            logger.info("Sending final batch of %d messages", batch_count)
            await producer.send_batch(batch)

        duration = time.time() - start_time
        logger.info("Completed sending %d messages in %.2fs", sent, duration)


@app.command()
def send(
    eventhub: Annotated[
        str | None,
        typer.Option(help="Event Hub name to send to (default: EVENTHUBNAME_1 in .env)"),
    ] = None,
    namespace: Annotated[
        str | None,
        typer.Option(
            help=(
                "Fully qualified namespace (default: EVENTHUB_NAMESPACE in .env). "
                "Required if no connection string."
            )
        ),
    ] = None,
    connection_string: Annotated[
        str | None,
        typer.Option(
            envvar="AZURE_EVENTHUB_CONNECTION_STRING",
            help=(
                "Event Hub connection string (default: AZURE_EVENTHUB_CONNECTION_STRING in .env). "
                "If absent, DefaultAzureCredential is used."
            ),
        ),
    ] = None,
    count: Annotated[int, typer.Option(help="Number of messages to send")] = 10,
    start_id: Annotated[int, typer.Option(help="Starting sequence_id value")] = 1,
    batch_size: Annotated[int, typer.Option(help="Max messages per batch before sending")] = 100,
    interval_seconds: Annotated[
        float,
        typer.Option(help="Optional delay between messages in seconds"),
    ] = 0.0,
    partition_key: Annotated[str | None, typer.Option(help="Partition key to pin messages")] = None,
    payload: Annotated[
        str | None,
        typer.Option(help="Optional JSON object to merge into each message payload"),
    ] = None,
    env_file: Annotated[
        Path | None,
        typer.Option(help="Path to .env file (default: .env)"),
    ] = None,
) -> None:
    """Send sequentially numbered JSON messages to the specified Event Hub."""
    try:
        _load_env(env_file)

        resolved_connection_string = connection_string or os.getenv(
            "AZURE_EVENTHUB_CONNECTION_STRING"
        )
        resolved_namespace = namespace or os.getenv("EVENTHUB_NAMESPACE")
        resolved_eventhub = eventhub or os.getenv("EVENTHUBNAME_1")

        if not resolved_eventhub:
            raise typer.BadParameter(
                "Event Hub name is required (via --eventhub or EVENTHUBNAME_1 in .env)"
            )

        assert resolved_eventhub is not None

        asyncio.run(
            send_messages(
                connection_string=resolved_connection_string,
                namespace=resolved_namespace,
                eventhub=resolved_eventhub,
                count=count,
                start_id=start_id,
                batch_size=batch_size,
                interval_seconds=interval_seconds,
                partition_key=partition_key,
                payload_json=payload,
            )
        )
    except Exception as exc:  # pragma: no cover - CLI surface
        logger.error("Failed to send messages: %s", exc)
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()
