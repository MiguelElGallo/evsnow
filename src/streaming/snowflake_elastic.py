"""Elastic Channels with durable-buffer acknowledgement and source replay safety.

Elastic append tokens only correlate acknowledgements. They are never source
offsets or deduplication keys. Source checkpoints remain the consumer's job.
"""

import hashlib
import json
import logging
import time
import uuid
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Any

from snowflake.ingest.streaming import StreamingIngestClient, StreamingIngestElasticChannel

from streaming.base import SnowflakeStreamingClientBase
from utils.config import SnowflakeConfig, SnowflakeConnectionConfig

logger = logging.getLogger(__name__)


@dataclass
class _PendingAppend:
    """Keep an ambiguous append alive until its original Future resolves."""

    batch_key: str
    source_slot: str
    row_count: int
    future: Future[Any]


class SnowflakeElasticStreamingClient(SnowflakeStreamingClientBase):
    """Append bounded source batches with at-least-once delivery.

    A wait timeout does not cancel the SDK append. Retrying the same batch in
    this instance observes its existing Future. A completed acknowledgement is
    cached for the latest batch of each logical source so a partially successful
    multi-partition source batch can be drained without resubmitting its already
    acknowledged parts. These receipts are only in memory; a process restart or
    SDK error can cause source replay and duplicates.
    """

    def __init__(
        self,
        snowflake_config: SnowflakeConfig,
        connection_config: SnowflakeConnectionConfig,
        client_name_suffix: str | None = None,
        retry_manager: Any | None = None,
    ) -> None:
        super().__init__(snowflake_config, connection_config, client_name_suffix, retry_manager)
        self.client_name_suffix = client_name_suffix or uuid.uuid4().hex[:8]
        self.streaming_client: StreamingIngestClient | None = None
        self.elastic_channel: StreamingIngestElasticChannel | None = None
        self._lock = RLock()
        self._pending: _PendingAppend | None = None
        self._acknowledged_batches: dict[str, str] = {}
        self.stats: dict[str, Any] = {
            "channel_mode": "elastic",
            "delivery_semantics": "at_least_once",
            "acknowledgement_scope": "durable_buffer",
            "client_created_at": None,
            "total_messages_sent": 0,
            "total_batches_sent": 0,
            "append_requests": 0,
            "acknowledgement_failures": 0,
            "acknowledgement_timeouts": 0,
            "channels_created": 0,
            "last_ingestion": None,
            "last_error": None,
            "close_failures": 0,
        }
        if retry_manager is not None:
            logger.warning(
                "Elastic mode does not apply the external retry decorator: the SDK retries "
                "transport failures, and failed source batches remain uncheckpointed for replay."
            )

    @property
    def is_started(self) -> bool:
        """Whether the SDK client and its singleton Elastic Channel are ready."""
        return self.streaming_client is not None and self.elastic_channel is not None

    def _build_connection_profile(self) -> dict[str, Any]:
        """Build the SDK's JWT profile without altering the raw ingestion rows."""
        private_key = Path(self.connection_config.private_key_file).expanduser().resolve()
        if not private_key.is_file():
            raise FileNotFoundError(f"Private key file not found: {private_key}")
        profile: dict[str, Any] = {
            "authorization_type": "JWT",
            "user": self.connection_config.user,
            "account": self.connection_config.account,
            "url": f"https://{self.connection_config.account}.snowflakecomputing.com:443",
            "private_key_file": str(private_key),
        }
        if self.connection_config.private_key_password:
            profile["private_key_passphrase"] = self.connection_config.private_key_password
        if self.connection_config.role:
            profile["role"] = self.connection_config.role
        return profile

    def start(self) -> None:
        """Create one SDK client and obtain its one implicit Elastic Channel."""
        with self._lock:
            if self.is_started:
                return
            if not self.connection_config.pipe_name:
                raise ValueError("pipe_name is required for Elastic streaming")
            self._acknowledged_batches.clear()
            try:
                self.streaming_client = StreamingIngestClient(
                    client_name=f"evsnow_elastic_{self.client_name_suffix}",
                    db_name=self.snowflake_config.database,
                    schema_name=self.snowflake_config.schema_name,
                    pipe_name=self.connection_config.pipe_name,
                    properties=self._build_connection_profile(),
                )
                self.elastic_channel = self.streaming_client.get_elastic_channel()
                self.stats["client_created_at"] = datetime.now(UTC)
                self.stats["channels_created"] += 1
            except Exception:
                self.stop()
                raise

    @staticmethod
    def _batch_identity(
        channel_name: str,
        partition_id: str,
        rows: list[dict[str, Any]],
    ) -> tuple[str, str]:
        """Hash source identity and payload, excluding regenerated ingestion clocks.

        EventHubMessage.to_dict regenerates these two clocks during a final
        drain. The clocks remain in the SDK rows, but cannot identify source
        replay. Every other field, including namespace, hub, partition, sequence,
        event body and producer identity, contributes to this fingerprint.
        """
        stable_rows = [
            {
                key: value
                for key, value in row.items()
                if key not in {"timestamp_ns", "ingestion_timestamp"}
            }
            for row in rows
        ]
        sources = sorted(
            {
                tuple(
                    str(row.get(key, ""))
                    for key in (
                        "eventhub_namespace",
                        "eventhub_name",
                        "consumer_group",
                        "partition_id",
                    )
                )
                for row in rows
            }
        )
        source_json = json.dumps([channel_name, partition_id, sources], separators=(",", ":"))
        rows_json = json.dumps(stable_rows, sort_keys=True, separators=(",", ":"))
        source_slot = hashlib.sha256(source_json.encode()).hexdigest()
        batch_key = hashlib.sha256((source_json + rows_json).encode()).hexdigest()
        return source_slot, batch_key

    def _acknowledge_pending(self) -> None:
        """Record one observed durable acknowledgement, without checkpointing."""
        pending = self._pending
        if pending is None:
            return
        self._acknowledged_batches[pending.source_slot] = pending.batch_key
        self.stats["total_messages_sent"] += pending.row_count
        self.stats["total_batches_sent"] += 1
        self.stats["last_ingestion"] = datetime.now(UTC)
        self.stats["last_error"] = None
        self._pending = None

    def _fail_pending(self, error: Exception) -> bool:
        """Release a failed append; its uncheckpointed source may later replay."""
        self.stats["acknowledgement_failures"] += 1
        self.stats["last_error"] = str(error)
        self._pending = None
        logger.error("Elastic append failed; source checkpoint must remain unchanged: %s", error)
        return False

    def _await_pending(self, timeout_seconds: float) -> bool:
        """Observe the original Future; a deadline never cancels or resubmits it."""
        pending = self._pending
        if pending is None:
            return True
        try:
            pending.future.result(timeout=timeout_seconds)
        except TimeoutError as error:
            # An SDK failure can itself be TimeoutError. Also handle completion
            # racing the caller's wait deadline by observing the finished Future.
            if pending.future.done():
                try:
                    pending.future.result(timeout=0)
                except Exception as completed_error:
                    return self._fail_pending(completed_error)
            else:
                self.stats["acknowledgement_timeouts"] += 1
                self.stats["last_error"] = str(error) or "Elastic acknowledgement wait timed out"
                logger.warning("Elastic acknowledgement wait timed out; original append is pending")
                return False
        except Exception as error:
            return self._fail_pending(error)
        self._acknowledge_pending()
        return True

    def ingest_batch(
        self,
        channel_name: str,
        data_batch: list[dict[str, Any]],
        partition_id: str = "0",
    ) -> bool:
        """Return success only after the batch's original durable acknowledgement."""
        if not data_batch:
            return True
        with self._lock:
            if not self.is_started or self.elastic_channel is None:
                self.stats["last_error"] = "Elastic streaming client is not started"
                return False
            try:
                source_slot, batch_key = self._batch_identity(
                    channel_name, partition_id, data_batch
                )
                if self._acknowledged_batches.get(source_slot) == batch_key:
                    return True
                if self._pending is not None:
                    if self._pending.batch_key != batch_key:
                        self.stats["last_error"] = (
                            "Another source batch has an unresolved Elastic append"
                        )
                        return False
                else:
                    future = self.elastic_channel.append_rows_with_wait(
                        data_batch, append_token=uuid.uuid4().hex
                    )
                    self._pending = _PendingAppend(batch_key, source_slot, len(data_batch), future)
                    self.stats["append_requests"] += 1
                return self._await_pending(self.connection_config.ack_timeout_seconds)
            except Exception as error:
                # Failure to identify/submit a new batch cannot erase an earlier
                # unresolved append. Only its own Future can settle that append.
                self.stats["acknowledgement_failures"] += 1
                self.stats["last_error"] = str(error)
                logger.error("Elastic batch submission failed: %s", error)
                return False

    def stop(self) -> None:
        """Observe pending work and close the SDK client within the close wait budget.

        Closing never resubmits rows or advances a source checkpoint. Elastic
        Channel lifecycle belongs to the client; there is no channel close API.
        """
        with self._lock:
            client = self.streaming_client
            if client is None:
                return
            deadline = time.monotonic() + self.connection_config.close_timeout_seconds
            shutdown_error: Exception | None = None
            try:
                if self._pending is not None and not self._await_pending(
                    max(0, deadline - time.monotonic())
                ):
                    shutdown_error = RuntimeError(
                        "Elastic shutdown did not acknowledge the pending source batch"
                    )
                remaining = max(0, int(deadline - time.monotonic()))
                client.close(wait_for_flush=remaining > 0, timeout_seconds=remaining)
            except Exception as error:
                shutdown_error = error
            finally:
                self.streaming_client = None
                self.elastic_channel = None
                self._pending = None
                self._acknowledged_batches.clear()
            if shutdown_error is not None:
                self.stats["close_failures"] += 1
                self.stats["last_error"] = str(shutdown_error)
                raise RuntimeError(
                    "Elastic client shutdown was incomplete; unacknowledged source remains "
                    "uncheckpointed and may replay"
                ) from shutdown_error

    def get_stats(self) -> dict[str, Any]:
        """Report acknowledged rows; no row count implies final table visibility."""
        with self._lock:
            stats = self.stats.copy()
            stats["pending_append"] = self._pending is not None
            created_at = stats["client_created_at"]
            if created_at is not None:
                runtime = (datetime.now(UTC) - created_at).total_seconds()
                stats["runtime_seconds"] = runtime
                stats["messages_per_second"] = (
                    stats["total_messages_sent"] / runtime if runtime > 0 else 0.0
                )
            return stats

    def health_check(self) -> dict[str, Any]:
        """Report local readiness, explicitly limited to buffer acknowledgements."""
        with self._lock:
            return {
                "client_status": "started" if self.is_started else "stopped",
                "connection_active": self.is_started,
                "channels_count": int(self.is_started),
                "channel_mode": "elastic",
                "delivery_semantics": "at_least_once",
                "buffer_ack_only": True,
                "table_visibility_verified": False,
                "pending_append": self._pending is not None,
                "errors": [self.stats["last_error"]] if self.stats["last_error"] else [],
            }
