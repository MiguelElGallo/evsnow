"""
CLI utility to send sequentially numbered JSON messages to an Event Hub.

Uses either an Event Hub connection string or an Azure identity credential
against a fully qualified namespace. Each message carries a monotonic
`sequence_id` so downstream ingestion checks can verify no IDs are missing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
from dotenv import load_dotenv

from utils.azure_identity import EventHubCredentialMode, build_eventhub_credential

app = typer.Typer(help="Send sequential JSON messages to an Event Hub for ingestion checks.")

logger = logging.getLogger("evsnow.tools.eventhub_sender")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

_RETRY_WAIT_PATTERN = re.compile(r"wait\s+(\d+(?:\.\d+)?)\s+seconds", re.IGNORECASE)


def _load_env(env_file: Path | None) -> None:
    """Load .env values so CLI defaults come from the shared config."""
    if env_file is None:
        env_file = Path(".env")
    load_dotenv(env_file)


async def _build_producer(
    connection_string: str | None,
    namespace: str | None,
    eventhub: str,
    credential_mode: EventHubCredentialMode,
) -> tuple[EventHubProducerClient, Any | None]:
    """Create an Event Hub producer using connection string or Azure identity."""
    if connection_string:
        logger.info("Using connection string authentication")
        return (
            EventHubProducerClient.from_connection_string(
                conn_str=connection_string,
                eventhub_name=eventhub,
            ),
            None,
        )

    if not namespace:
        raise typer.BadParameter("Namespace is required when no connection string is provided")

    credential, _expires_on, credential_label = await build_eventhub_credential(
        namespace=namespace,
        logger=logger,
        credential_mode=credential_mode,
    )
    logger.info("Using %s for authentication", credential_label)
    return (
        EventHubProducerClient(
            fully_qualified_namespace=namespace,
            eventhub_name=eventhub,
            credential=credential,
        ),
        credential,
    )


def _build_payload(sequence_id: int, base_payload: dict[str, Any]) -> str:
    """Assemble the JSON payload with a fail-safe sequence id."""
    envelope = {
        "sequence_id": sequence_id,
        "sent_at": datetime.now(UTC).isoformat(),
        "source": "evsnow-cli-sender",
        "trace_id": uuid.uuid4().hex,
        "message": f"Test message {sequence_id}",
        "payload": base_payload,
    }
    return json.dumps(envelope)


def _is_retryable_error(exc: Exception) -> bool:
    """Best-effort detection of retryable/throttling errors."""
    if isinstance(exc, EventHubError):
        retryable = getattr(exc, "retryable", None)
        if retryable is True:
            return True

    text = str(exc).lower()
    return any(
        token in text
        for token in (
            "server-busy",
            "throttl",
            "50002",
            "timeout",
            "temporar",
        )
    )


def _get_retry_delay_seconds(
    exc: Exception,
    base_delay: float,
    max_delay: float,
    attempt: int,
) -> float:
    """Compute delay for retries, honoring server suggested wait when present."""
    text = str(exc)
    match = _RETRY_WAIT_PATTERN.search(text)
    if match:
        suggested = float(match.group(1))
        return max(0.0, min(max_delay, suggested))

    exp_delay = base_delay * (2**attempt)
    exp_delay = min(max_delay, exp_delay)
    jitter = exp_delay * random.uniform(0.0, 0.2)
    return exp_delay + jitter


async def _send_batch_with_retry(
    producer: EventHubProducerClient,
    batch: Any,
    max_retries: int,
    base_delay: float,
    max_delay: float,
) -> None:
    """Send a batch with exponential backoff for throttling errors."""
    attempt = 0
    while True:
        try:
            await producer.send_batch(batch)
            return
        except Exception as exc:
            if attempt >= max_retries or not _is_retryable_error(exc):
                raise
            delay = _get_retry_delay_seconds(exc, base_delay, max_delay, attempt)
            logger.warning(
                "Send throttled, retrying in %.2fs (attempt %d/%d)",
                delay,
                attempt + 1,
                max_retries,
            )
            await asyncio.sleep(delay)
            attempt += 1


async def send_messages(
    connection_string: str | None,
    namespace: str | None,
    eventhub: str,
    count: int,
    start_id: int,
    batch_size: int,
    interval_seconds: float,
    max_messages_per_second: float,
    batch_pause_seconds: float,
    max_retries: int,
    retry_base_delay_seconds: float,
    retry_max_delay_seconds: float,
    credential_mode: EventHubCredentialMode,
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

    producer, credential = await _build_producer(
        connection_string, namespace, eventhub, credential_mode
    )
    try:
        async with producer:
            await _send_messages_with_producer(
                producer=producer,
                count=count,
                start_id=start_id,
                batch_size=batch_size,
                interval_seconds=interval_seconds,
                max_messages_per_second=max_messages_per_second,
                batch_pause_seconds=batch_pause_seconds,
                max_retries=max_retries,
                retry_base_delay_seconds=retry_base_delay_seconds,
                retry_max_delay_seconds=retry_max_delay_seconds,
                partition_key=partition_key,
                base_payload=base_payload,
            )
    finally:
        if credential is not None:
            # Async Azure credentials own transport sessions; close them even when
            # producer creation/send fails so local smoke tests do not leak aiohttp clients.
            # https://learn.microsoft.com/python/api/azure-identity/azure.identity.aio.defaultazurecredential
            await credential.close()


async def _send_messages_with_producer(
    *,
    producer: EventHubProducerClient,
    count: int,
    start_id: int,
    batch_size: int,
    interval_seconds: float,
    max_messages_per_second: float,
    batch_pause_seconds: float,
    max_retries: int,
    retry_base_delay_seconds: float,
    retry_max_delay_seconds: float,
    partition_key: str | None,
    base_payload: dict[str, Any],
) -> None:
    """Send messages using an already-created producer."""
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
            await _send_batch_with_retry(
                producer,
                batch,
                max_retries=max_retries,
                base_delay=retry_base_delay_seconds,
                max_delay=retry_max_delay_seconds,
            )
            if batch_pause_seconds > 0:
                await asyncio.sleep(batch_pause_seconds)
            batch = await producer.create_batch(partition_key=partition_key)
            batch.add(event)
            batch_count = 1

        if batch_count >= batch_size:
            logger.info("Sending batch of %d messages (size threshold)", batch_count)
            await _send_batch_with_retry(
                producer,
                batch,
                max_retries=max_retries,
                base_delay=retry_base_delay_seconds,
                max_delay=retry_max_delay_seconds,
            )
            if batch_pause_seconds > 0:
                await asyncio.sleep(batch_pause_seconds)
            batch = await producer.create_batch(partition_key=partition_key)
            batch_count = 0

        sent += 1
        sequence_id += 1

        if interval_seconds > 0:
            await asyncio.sleep(interval_seconds)

        if max_messages_per_second > 0:
            target_time = start_time + (sent / max_messages_per_second)
            now = time.time()
            if target_time > now:
                await asyncio.sleep(target_time - now)

    if batch_count > 0:
        logger.info("Sending final batch of %d messages", batch_count)
        await _send_batch_with_retry(
            producer,
            batch,
            max_retries=max_retries,
            base_delay=retry_base_delay_seconds,
            max_delay=retry_max_delay_seconds,
        )
        if batch_pause_seconds > 0:
            await asyncio.sleep(batch_pause_seconds)

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
                "If absent, the selected Azure credential mode is used."
            ),
        ),
    ] = None,
    count: Annotated[int, typer.Option(help="Number of messages to send")] = 10,
    start_id: Annotated[int, typer.Option(help="Starting sequence_id value")] = 1,
    batch_size: Annotated[int, typer.Option(help="Max messages per batch before sending")] = 100000,
    interval_seconds: Annotated[
        float,
        typer.Option(help="Optional delay between messages in seconds"),
    ] = 0.0,
    max_messages_per_second: Annotated[
        float,
        typer.Option(help="Rate limit total send throughput (0 disables)"),
    ] = 0.0,
    batch_pause_seconds: Annotated[
        float,
        typer.Option(help="Delay after each batch send to reduce throttling"),
    ] = 0.0,
    max_retries: Annotated[
        int,
        typer.Option(help="Max retry attempts when throttled"),
    ] = 8,
    retry_base_delay_seconds: Annotated[
        float,
        typer.Option(help="Base delay for retry backoff (seconds)"),
    ] = 1.0,
    retry_max_delay_seconds: Annotated[
        float,
        typer.Option(help="Max delay for retry backoff (seconds)"),
    ] = 30.0,
    credential_mode: Annotated[
        EventHubCredentialMode,
        typer.Option(
            help=(
                "Azure identity mode when no connection string is used: 'default' or 'azure_cli'."
            )
        ),
    ] = "default",
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
                max_messages_per_second=max_messages_per_second,
                batch_pause_seconds=batch_pause_seconds,
                max_retries=max_retries,
                retry_base_delay_seconds=retry_base_delay_seconds,
                retry_max_delay_seconds=retry_max_delay_seconds,
                credential_mode=credential_mode,
                partition_key=partition_key,
                payload_json=payload,
            )
        )
    except Exception as exc:  # pragma: no cover - CLI surface
        logger.error("Failed to send messages: %s", exc)
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()
