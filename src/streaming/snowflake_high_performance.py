"""
Snowflake High-Performance SDK streaming client implementation.

This module provides a streaming client using the HIGH-PERFORMANCE Snowpipe Streaming architecture:
- Uses snowpipe-streaming SDK v1.1.0+ (requires PIPE object)
- Designed for high throughput (~10 GB/s per table)
- Server-side schema validation
- Supports in-flight transformations via PIPE

Documentation: https://docs.snowflake.com/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview
"""

import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import logfire

# Snowpipe Streaming exposes ChannelStatus at the package root in SDK 1.2.0,
# while older examples import it from a channel_status submodule.
try:
    from snowflake.ingest.streaming import ChannelStatus, StreamingIngestClient
except ImportError:  # pragma: no cover - compatibility with older SDK layouts
    from snowflake.ingest.streaming import StreamingIngestClient
    from snowflake.ingest.streaming.channel_status import ChannelStatus

from streaming.base import SnowflakeStreamingClientBase
from utils.config import SnowflakeConfig, SnowflakeConnectionConfig

logger = logging.getLogger(__name__)


class SnowflakeHighPerformanceStreamingClient(SnowflakeStreamingClientBase):
    """
    Snowflake streaming client using HIGH-PERFORMANCE SDK architecture.

    This implementation uses the new high-performance SDK with PIPE objects
    for maximum throughput. Currently has Azure compatibility issues.

    Key characteristics:
    - PIPE object required
    - Server-side configuration
    - ~10 GB/s throughput per table

    """

    def __init__(
        self,
        snowflake_config: SnowflakeConfig,
        connection_config: SnowflakeConnectionConfig,
        client_name_suffix: str | None = None,
        retry_manager: Any | None = None,
    ):
        super().__init__(snowflake_config, connection_config, client_name_suffix, retry_manager)

        # Ensure client_name_suffix is properly typed and set
        self.client_name_suffix: str
        if not self.client_name_suffix:
            self.client_name_suffix = str(uuid.uuid4())[:8]

        # Client components
        self.streaming_client: StreamingIngestClient | None = None
        self.channels: dict[str, Any] = {}  # channel_name -> channel object
        self._last_channel_status_check_at_by_channel: dict[str, datetime] = {}
        self._last_client_refresh_at: datetime | None = None

        # Statistics
        self.stats: dict[str, Any] = {
            "client_created_at": None,
            "total_messages_sent": 0,
            "total_batches_sent": 0,
            "channels_created": 0,
            "last_ingestion": None,
            "channel_status_checks": 0,
            "last_channel_status_check": None,
            "last_channel_status_code": None,
            "retry_stats": {
                "total_retries": 0,
                "successful_retries": 0,
                "failed_retries": 0,
            },
        }

        # Apply retry decorator to implementation if retry manager is provided
        if self.retry_manager:
            decorator = self.retry_manager.get_retry_decorator()
            self._ingest_with_retry = decorator(self._ingest_batch_impl)
        else:
            self._ingest_with_retry = self._ingest_batch_impl

    @property
    def is_started(self) -> bool:
        """Check if the client is started and ready for ingestion."""
        return self.streaming_client is not None

    def _build_connection_profile(self) -> dict[str, Any]:
        """
        Build connection profile for StreamingIngestClient.

        For the high-performance architecture, the profile requires:
        - user: Snowflake username
        - account: Account identifier (e.g., "ZZZZUUUU-YU88540")
        - url: Full Snowflake URL with port
        - private_key_file: Path to PEM private key file
        - private_key_passphrase: Optional passphrase for encrypted private keys
        - role: Role to use (optional)

        IMPORTANT: The profile does NOT include:
        - warehouse, database, schema (passed to StreamingIngestClient constructor instead)
        - host (use full 'url' instead)

        Reference:
        https://docs.snowflake.com/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-configurations

        Returns a dictionary with connection parameters including private key authentication.
        """
        logger.info("Building Snowflake connection profile for high-performance SDK")

        try:
            private_key_path = Path(self.connection_config.private_key_file).expanduser().resolve()
            if not private_key_path.is_file():
                raise FileNotFoundError(f"Private key file not found: {private_key_path}")

            profile = {
                "authorization_type": "JWT",
                "user": self.connection_config.user,
                "account": self.connection_config.account,
                "url": f"https://{self.connection_config.account}.snowflakecomputing.com:443",
                "private_key_file": str(private_key_path),
            }

            if self.connection_config.private_key_password:
                profile["private_key_passphrase"] = self.connection_config.private_key_password

            # Add role if specified
            if self.connection_config.role:
                profile["role"] = self.connection_config.role

            logger.info(
                f"Profile built for high-performance SDK: account={self.connection_config.account}, "
                f"user={self.connection_config.user}, url={profile['url']}, "
                f"role={self.connection_config.role or 'default'}"
            )

            return profile

        except Exception as e:
            logger.error(f"Failed to build connection profile: {e}", exc_info=True)
            raise ValueError(f"Cannot build Snowflake connection profile: {e}") from e

    def start(self) -> None:
        """Initialize the Snowflake streaming client."""
        with logfire.span(
            "snowflake.client.start",
            database=self.snowflake_config.database,
            schema=self.snowflake_config.schema_name,
            table=self.snowflake_config.table_name,
            client_suffix=self.client_name_suffix,
        ):
            logger.info(
                f"Starting Snowflake streaming client for {self.snowflake_config.database}.{self.snowflake_config.schema_name}.{self.snowflake_config.table_name}"
            )

            try:
                # Build connection profile
                profile = self._build_connection_profile()

                # Create StreamingIngestClient using the high-performance SDK.
                client_name = f"evsnow_{self.client_name_suffix}"
                logger.info(f"Creating High-Performance StreamingIngestClient: {client_name}")

                pipe_name = self.connection_config.pipe_name
                if not pipe_name:
                    raise ValueError(
                        "pipe_name is required for high-performance streaming. "
                        "Set SNOWFLAKE_PIPE_NAME environment variable."
                    )

                self.streaming_client = StreamingIngestClient(
                    client_name=client_name,
                    db_name=self.snowflake_config.database,
                    schema_name=self.snowflake_config.schema_name,
                    pipe_name=pipe_name,
                    properties=profile,
                )

                logger.info(f"✅ High-Performance StreamingIngestClient created: {client_name}")
                logfire.info(
                    "Snowflake StreamingIngestClient initialized",
                    client_name=client_name,
                    database=self.snowflake_config.database,
                    schema=self.snowflake_config.schema_name,
                    pipe=pipe_name,
                    table=self.snowflake_config.table_name,
                )

                # Ensure target table exists
                self._ensure_target_table()

                self.stats["client_created_at"] = datetime.now(UTC)
                self._last_client_refresh_at = self.stats["client_created_at"]
                logger.info("✅ Snowflake streaming client started successfully")

            except Exception as e:
                logger.error(f"Failed to start Snowflake streaming client: {e}", exc_info=True)
                logfire.error("Failed to start Snowflake client", error=str(e))
                self.stop()
                raise

    def _close_with_flush(self, closeable: Any) -> None:
        """Close SDK objects while preserving flush semantics across SDK versions."""
        close = getattr(closeable, "close", None)
        if not callable(close):
            return

        try:
            close(wait_for_flush=True)
        except TypeError:
            # Some Snowpipe Streaming SDK versions expose close(wait_for_flush=True),
            # while older Python bindings only expose close(). The append path already
            # waits for commits before checkpoints advance:
            # https://docs.snowflake.com/en/user-guide/snowpipe-streaming-sdk-python/reference/latest/api/snowflake/ingest/streaming/index
            close()

    def stop(self) -> None:
        """Stop the Snowflake streaming client and clean up resources."""
        logger.info("Stopping Snowflake streaming client...")

        # Close all channels with wait_for_flush=True
        # The Snowflake SDK will flush each channel before closing it
        logger.info(f"🔌 Closing {len(self.channels)} channels with flush...")
        for channel_name, channel in self.channels.items():
            try:
                if channel is not None:
                    logger.info(f"Closing channel with flush: {channel_name}")
                    self._close_with_flush(channel)
                    logger.info(f"✅ Channel closed and flushed: {channel_name}")
            except Exception as e:
                logger.error(f"Error closing channel {channel_name}: {e}", exc_info=True)

        self.channels.clear()
        self._last_channel_status_check_at_by_channel.clear()
        logger.info("✅ All channels closed with flush")

        if self.streaming_client is not None:
            try:
                logger.info("Closing StreamingIngestClient (channels already flushed)...")
                self._close_with_flush(self.streaming_client)
                logger.info("✅ StreamingIngestClient closed")
                self.streaming_client = None
            except Exception as e:
                logger.error(f"Error closing StreamingIngestClient: {e}", exc_info=True)

        logger.info("Snowflake streaming client stopped")

    def __del__(self) -> None:
        """Destructor to ensure proper cleanup if stop() wasn't called."""
        # If the streaming_client still exists when this object is being destroyed,
        # it means stop() was never called. Call it now to ensure proper flush.
        if self.streaming_client is not None:
            logger.warning(
                "⚠️ Snowflake client being destroyed without explicit stop() call! "
                "Calling stop() now to flush data..."
            )
            try:
                self.stop()
            except Exception as e:
                logger.error(f"Error in __del__ while stopping Snowflake client: {e}")

    def _ensure_target_table(self) -> None:
        """Ensure the target table exists with the correct schema."""
        with logfire.span(
            "snowflake.ensure_table",
            database=self.snowflake_config.database,
            schema=self.snowflake_config.schema_name,
            table=self.snowflake_config.table_name,
        ):
            logger.info("Table schema verification should be done via Snowflake DDL")
            logger.info(
                "Ensure the following table exists in Snowflake:\n"
                f"  Database: {self.snowflake_config.database}\n"
                f"  Schema: {self.snowflake_config.schema_name}\n"
                f"  Table: {self.snowflake_config.table_name}\n"
                "  Columns: event_body, partition_id, sequence_number, "
                "enqueued_time, properties, system_properties, ingestion_timestamp"
            )

            # TODO: Implement table existence check using Snowflake connector
            # For now, we assume the table exists
            logfire.info("Target table assumed to exist", table=self.snowflake_config.table_name)

    def _get_or_create_channel(self, channel_name: str) -> Any:
        """
        Get or create a single shared channel for all partitions.

        Args:
            channel_name: Deterministic channel name shared across partitions

        Returns:
            Channel object for the shared channel

        Raises:
            RuntimeError: If client not initialized or channel creation fails
        """
        if self.streaming_client is None:
            raise RuntimeError("StreamingIngestClient not initialized. Call start() first.")

        # Check if channel already exists
        if channel_name in self.channels:
            logger.debug("Using existing channel: %s", channel_name)
            return self.channels[channel_name]

        # Create new channel
        try:
            logger.debug("Opening new channel: %s", channel_name)

            # Open channel (high-performance SDK returns tuple: (channel, status))
            # Reference: https://gist.github.com/sfc-gh-chathomas/a7b06bb46907bead737954d53b3a8495
            channel, status = self.streaming_client.open_channel(channel_name)

            self.channels[channel_name] = channel
            self.stats["channels_created"] += 1

            logger.info(
                "✅ Channel opened: %s (status: %s, total channels: %s)",
                channel_name,
                status,
                len(self.channels),
            )
            logfire.info(
                "Snowflake channel opened",
                channel_name=channel_name,
                status=str(status),
                total_channels=len(self.channels),
            )

            return channel

        except Exception as e:
            logger.error(
                "Failed to create channel %s: %s",
                channel_name,
                e,
                exc_info=True,
            )
            logfire.error(
                "Failed to create Snowflake channel",
                channel_name=channel_name,
                error=str(e),
            )
            raise

    def _maybe_check_channel_status(
        self,
        channel_name: str,
        start_offset_token: str | None = None,
        end_offset_token: str | None = None,
        force: bool = False,
    ) -> None:
        if self.streaming_client is None:
            return

        interval = getattr(self.snowflake_config, "channel_status_interval_seconds", 60)
        if interval <= 0 and not force:
            return

        now = datetime.now(UTC)
        last_channel_status_check_at = self._last_channel_status_check_at_by_channel.get(
            channel_name
        )
        if not force and last_channel_status_check_at is not None:
            elapsed = (now - last_channel_status_check_at).total_seconds()
            if elapsed < interval:
                return

        try:
            statuses = self.streaming_client.get_channel_statuses([channel_name])
        except Exception as exc:
            logger.error(
                "Failed to fetch channel status for %s: %s",
                channel_name,
                exc,
                exc_info=True,
            )
            logfire.error(
                "Failed to fetch Snowflake channel status",
                channel_name=channel_name,
                error=str(exc),
            )
            if force:
                raise RuntimeError(f"Failed to fetch channel status for {channel_name}") from exc
            return

        status = statuses.get(channel_name)
        if status is None:
            logger.warning("No channel status returned for %s", channel_name)
            if force:
                raise RuntimeError(f"No channel status returned for {channel_name}")
            return

        self.stats["channel_status_checks"] += 1
        self.stats["last_channel_status_check"] = now
        self.stats["last_channel_status_code"] = status.status_code
        self._last_channel_status_check_at_by_channel[channel_name] = now

        logger.info(
            "Channel status: name=%s status=%s rows_inserted=%s rows_parsed=%s rows_error=%s",
            status.channel_name,
            status.status_code,
            status.rows_inserted_count,
            status.rows_parsed_count,
            status.rows_error_count,
        )
        logfire.info(
            "Snowflake channel status",
            channel_name=status.channel_name,
            status_code=status.status_code,
            rows_inserted=status.rows_inserted_count,
            rows_parsed=status.rows_parsed_count,
            rows_error=status.rows_error_count,
            last_error_message=status.last_error_message,
            last_error_timestamp=str(status.last_error_timestamp)
            if status.last_error_timestamp
            else None,
        )

        if self._channel_status_has_row_errors(
            status,
            start_offset_token=start_offset_token,
            end_offset_token=end_offset_token,
        ):
            logger.warning(
                "Channel %s reported errors: status=%s rows_error=%s last_error=%s",
                status.channel_name,
                status.status_code,
                status.rows_error_count,
                status.last_error_message,
            )
            logfire.warning(
                "Snowflake channel reported errors",
                channel_name=status.channel_name,
                status_code=status.status_code,
                rows_error=status.rows_error_count,
                last_error_message=status.last_error_message,
            )
            raise RuntimeError(
                f"Snowflake channel {status.channel_name} reported row errors: "
                f"{status.last_error_message or status.rows_error_count}"
            )

        if self._is_channel_status_error(status):
            logger.error(
                "Channel %s status indicates an error (%s); reopening channel.",
                status.channel_name,
                status.status_code,
            )
            logfire.error(
                "Snowflake channel status indicates error; reopening channel",
                channel_name=status.channel_name,
                status_code=status.status_code,
                last_error_message=status.last_error_message,
            )
            self._reopen_channel(status.channel_name)
            if force:
                raise RuntimeError(
                    f"Snowflake channel {status.channel_name} status indicates an error: "
                    f"{status.status_code}"
                )

    @staticmethod
    def _is_channel_status_error(status: ChannelStatus) -> bool:
        status_code = (status.status_code or "").upper()
        if status_code in {"OK", "ACTIVE"}:
            return False
        return (
            status_code.startswith("ERR_")
            or "MUST_BE_REOPENED" in status_code
            or "INVALID" in status_code
        )

    @staticmethod
    def _parse_offset_token(offset_token: str | None) -> tuple[str, int] | None:
        if not isinstance(offset_token, str):
            return None

        partition_id, separator, sequence_number = offset_token.rpartition("_")
        if not separator:
            return None

        try:
            return partition_id, int(sequence_number)
        except ValueError:
            return None

    @classmethod
    def _offset_token_in_range(
        cls,
        candidate_token: str | None,
        start_offset_token: str | None,
        end_offset_token: str | None,
    ) -> bool:
        candidate = cls._parse_offset_token(candidate_token)
        start = cls._parse_offset_token(start_offset_token)
        end = cls._parse_offset_token(end_offset_token)
        if candidate is None or start is None or end is None:
            return False

        candidate_partition, candidate_sequence = candidate
        start_partition, start_sequence = start
        end_partition, end_sequence = end
        return (
            candidate_partition == start_partition == end_partition
            and start_sequence <= candidate_sequence <= end_sequence
        )

    @classmethod
    def _channel_status_has_row_errors(
        cls,
        status: ChannelStatus,
        start_offset_token: str | None,
        end_offset_token: str | None,
    ) -> bool:
        if not ((status.rows_error_count or 0) > 0 or status.last_error_message):
            return False

        error_upper_bound = getattr(status, "last_error_offset_token_upper_bound", None)
        if error_upper_bound:
            return cls._offset_token_in_range(
                error_upper_bound,
                start_offset_token,
                end_offset_token,
            )

        # Without an error offset token, fail closed so Event Hub checkpointing
        # cannot advance past a Snowflake channel that reports row errors.
        return True

    def _reopen_channel(self, channel_name: str) -> None:
        if self.streaming_client is None:
            return

        channel = self.channels.get(channel_name)
        if channel is not None:
            try:
                self._close_with_flush(channel)
            except Exception as exc:
                logger.error(
                    "Failed to close channel %s during reopen: %s",
                    channel_name,
                    exc,
                    exc_info=True,
                )
                logfire.error(
                    "Failed to close Snowflake channel during reopen",
                    channel_name=channel_name,
                    error=str(exc),
                )
            finally:
                self.channels.pop(channel_name, None)
                self._last_channel_status_check_at_by_channel.pop(channel_name, None)

        channel, status = self.streaming_client.open_channel(channel_name)
        self.channels[channel_name] = channel
        logger.info(
            "✅ Channel reopened: %s (status: %s)",
            channel_name,
            status,
        )
        logfire.info(
            "Snowflake channel reopened",
            channel_name=channel_name,
            status=str(status),
        )

    def _recover_streaming_state(self, channel_name: str, error: Exception) -> bool:
        """
        Attempt to recover the streaming client/channel after a fatal streaming error.

        Returns True if recovery actions were executed, False otherwise.
        """
        if self._should_recreate_client(error):
            logger.warning(
                "Streaming client requires recreation after error; restarting client.",
                exc_info=True,
            )
            logfire.warning(
                "Streaming client requires recreation; restarting client",
                channel_name=channel_name,
                error=str(error),
            )
            try:
                self.stop()
                self.start()
                return True
            except Exception as restart_error:
                logger.error(
                    "Failed to restart StreamingIngestClient: %s",
                    restart_error,
                    exc_info=True,
                )
                logfire.error(
                    "Failed to restart StreamingIngestClient",
                    error=str(restart_error),
                )
                return False

        if self._should_reopen_channel(error):
            logger.warning(
                "Streaming channel requires reopen after error; reopening channel.",
                exc_info=True,
            )
            logfire.warning(
                "Streaming channel requires reopen; reopening channel",
                channel_name=channel_name,
                error=str(error),
            )
            try:
                self._reopen_channel(channel_name)
                return True
            except Exception as reopen_error:
                logger.error(
                    "Failed to reopen channel %s: %s",
                    channel_name,
                    reopen_error,
                    exc_info=True,
                )
                logfire.error(
                    "Failed to reopen Snowflake channel",
                    channel_name=channel_name,
                    error=str(reopen_error),
                )
                return False

        return False

    def _maybe_refresh_client(self) -> None:
        interval_seconds = self.snowflake_config.client_refresh_interval_seconds
        if interval_seconds <= 0:
            return

        if self.streaming_client is None:
            return

        now = datetime.now(UTC)
        last_refresh = self._last_client_refresh_at or self.stats.get("client_created_at")
        if last_refresh is None:
            self._last_client_refresh_at = now
            return

        elapsed = (now - last_refresh).total_seconds()
        if elapsed < interval_seconds:
            return

        logger.warning(
            "Proactively refreshing Snowflake streaming client after %s seconds.",
            int(elapsed),
        )
        logfire.warning(
            "Proactively refreshing Snowflake streaming client",
            elapsed_seconds=int(elapsed),
            interval_seconds=interval_seconds,
        )

        self.stop()
        self.start()

    @staticmethod
    def _should_recreate_client(error: Exception) -> bool:
        message = str(error).lower()
        return (
            "token has expired" in message
            or "fail to create authorization token" in message
            or "re-create the client" in message
            or ("client" in message and "invalid state" in message)
        )

    @staticmethod
    def _should_reopen_channel(error: Exception) -> bool:
        message = str(error).lower()
        return (
            "invalidchannelerror" in message
            or "must be reopened" in message
            or ("channel" in message and "invalid state" in message)
        )

    @staticmethod
    def _offset_token_for_row(
        row: dict[str, Any],
        partition_id: str,
    ) -> str:
        # Snowflake treats this value as an application offset token, not a row ID.
        # Keep it partition-scoped so replay checks are comparable within one channel.
        sequence_number = row.get("sequence_number")
        if sequence_number is None:
            raise ValueError("Cannot build Snowflake offset token without sequence_number")
        try:
            sequence_number = int(sequence_number)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Cannot build Snowflake offset token from sequence_number={sequence_number!r}"
            ) from exc
        return f"{partition_id}_{sequence_number}"

    @staticmethod
    def _offset_token_reached(committed_token: str | None, target_token: str) -> bool:
        committed = SnowflakeHighPerformanceStreamingClient._parse_offset_token(committed_token)
        target = SnowflakeHighPerformanceStreamingClient._parse_offset_token(target_token)
        if committed is None or target is None:
            return False
        committed_partition, committed_sequence = committed
        target_partition, target_sequence = target
        if committed_partition != target_partition:
            return False
        return committed_sequence >= target_sequence

    def _append_rows_to_channel(
        self,
        channel: Any,
        rows: list[dict[str, Any]],
        offset_tokens: list[str],
    ) -> None:
        # Prefer the SDK batch API so Snowflake receives one offset-token range for
        # the append: https://docs.snowflake.com/en/user-guide/snowpipe-streaming-sdk-python/reference/latest/api/snowflake/ingest/streaming/index
        append_rows = getattr(channel, "append_rows", None)
        if callable(append_rows):
            append_rows(
                rows,
                start_offset_token=offset_tokens[0],
                end_offset_token=offset_tokens[-1],
            )
            return

        for row, offset_token in zip(rows, offset_tokens, strict=True):
            channel.append_row(row, offset_token)

    def _wait_for_channel_commit(
        self,
        channel: Any,
        channel_name: str,
        end_offset_token: str,
    ) -> Any | None:
        # Event Hub checkpoints must not advance until Snowflake has committed the
        # token range; wait_for_commit is the explicit SDK confirmation point.
        flush_result = None
        timeout_seconds = self.snowflake_config.connection_timeout_seconds

        initiate_flush = getattr(channel, "initiate_flush", None)
        if callable(initiate_flush):
            flush_result = initiate_flush()
            logger.debug(
                "Initiated flush for channel %s after append (result=%s)",
                channel_name,
                flush_result,
            )
        else:
            flush = getattr(channel, "flush", None)
            if callable(flush):
                flush_result = flush()
                logger.debug(
                    "Flushed channel %s after append (result=%s)",
                    channel_name,
                    flush_result,
                )

        wait_for_commit = getattr(channel, "wait_for_commit", None)
        if not callable(wait_for_commit):
            raise RuntimeError(
                "Snowflake channel does not support wait_for_commit; "
                "cannot safely advance Event Hub checkpoints"
            )

        wait_for_commit(
            lambda committed_token: self._offset_token_reached(
                committed_token,
                end_offset_token,
            ),
            timeout_seconds=timeout_seconds,
        )
        logger.debug(
            "Committed channel %s through offset token %s",
            channel_name,
            end_offset_token,
        )
        return flush_result

    def _ingest_batch_impl(
        self,
        channel_name: str,
        data_batch: list[dict[str, Any]],
        partition_id: str = "0",
        _recovery_attempted: bool = False,
    ) -> bool:
        """
        Internal implementation of batch ingestion.
        This method will be wrapped with retry decorator if configured.

        Args:
            channel_name: Name of the logical channel (for logging/tracking)
            data_batch: List of dictionaries containing data to ingest
            partition_id: Partition ID for channel selection

        Returns:
            True if ingestion was successful

        Raises:
            RuntimeError: If client is not started or ingestion fails
        """
        with logfire.span(
            "snowflake.ingest_batch",
            channel_name=channel_name,
            partition_id=partition_id,
            batch_size=len(data_batch),
            database=self.snowflake_config.database,
            table=self.snowflake_config.table_name,
        ) as span:
            if not data_batch:
                logger.warning("Empty data batch provided for ingestion")
                span.set_attribute("empty_batch", True)
                return True

            logger.debug(
                f"Ingesting {len(data_batch)} records to channel {channel_name} (partition {partition_id})"
            )

            self._maybe_refresh_client()
            channel = self._get_or_create_channel(channel_name)

            if channel is None:
                raise RuntimeError(
                    f"Failed to get channel {channel_name}. Client may not be started."
                )

            # Insert rows into the channel with a single SDK batch append.
            try:
                offset_tokens = [
                    self._offset_token_for_row(row, partition_id) for row in data_batch
                ]
                committed_before_append = channel.get_latest_committed_offset_token()
                rows_to_append = [
                    (row, offset_token)
                    for row, offset_token in zip(data_batch, offset_tokens, strict=True)
                    if not self._offset_token_reached(committed_before_append, offset_token)
                ]

                # Snowflake channels store the latest committed offset token. On
                # replay, skip rows at or below that token to avoid duplicate writes.
                # See: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-channels
                if not rows_to_append:
                    logger.debug(
                        "All %s rows for channel %s are already committed through offset token %s",
                        len(data_batch),
                        channel_name,
                        committed_before_append,
                    )
                    self._maybe_check_channel_status(
                        channel_name,
                        start_offset_token=offset_tokens[0],
                        end_offset_token=offset_tokens[-1],
                        force=True,
                    )
                    return True

                append_rows, append_offset_tokens = zip(*rows_to_append, strict=True)
                self._append_rows_to_channel(
                    channel,
                    list(append_rows),
                    list(append_offset_tokens),
                )
                rows_inserted = len(rows_to_append)

                logger.debug(
                    f"Appended {rows_inserted} rows to channel {channel_name} (partition {partition_id})"
                )

                self._wait_for_channel_commit(
                    channel,
                    channel_name,
                    append_offset_tokens[-1],
                )

                offset_token = channel.get_latest_committed_offset_token()
                logger.debug(f"Latest committed offset for channel {channel_name}: {offset_token}")

                self._maybe_check_channel_status(
                    channel_name,
                    start_offset_token=append_offset_tokens[0],
                    end_offset_token=append_offset_tokens[-1],
                    force=True,
                )

            except Exception as e:
                if not _recovery_attempted and self._recover_streaming_state(channel_name, e):
                    logger.warning(
                        "Recovered Snowflake streaming state after error; retrying batch once."
                    )
                    logfire.warning(
                        "Recovered Snowflake streaming state; retrying batch",
                        channel_name=channel_name,
                        partition_id=partition_id,
                        batch_size=len(data_batch),
                        error=str(e),
                    )
                    return self._ingest_batch_impl(
                        channel_name=channel_name,
                        data_batch=data_batch,
                        partition_id=partition_id,
                        _recovery_attempted=True,
                    )

                logger.error(
                    f"Failed to insert rows into channel {channel_name}: {e}",
                    exc_info=True,
                )
                logfire.error(
                    "Failed to insert rows into Snowflake channel",
                    channel_name=channel_name,
                    partition_id=partition_id,
                    batch_size=len(data_batch),
                    error=str(e),
                )
                raise

            # Update statistics
            self.stats["total_messages_sent"] += rows_inserted
            self.stats["total_batches_sent"] += 1
            self.stats["last_ingestion"] = datetime.now(UTC)

            # Track ingestion metrics in span
            span.set_attribute("messages_sent", rows_inserted)
            span.set_attribute("source_messages", len(data_batch))
            span.set_attribute("total_messages", self.stats["total_messages_sent"])
            span.set_attribute("total_batches", self.stats["total_batches_sent"])

            logger.debug(
                f"Successfully ingested {rows_inserted} records to {channel_name} (partition {partition_id})"
            )
            logfire.info(
                "Batch ingested successfully",
                channel=channel_name,
                partition=partition_id,
                records=rows_inserted,
                total_messages=self.stats["total_messages_sent"],
            )
            return True

    def ingest_batch(
        self,
        channel_name: str,
        data_batch: list[dict[str, Any]],
        partition_id: str = "0",
    ) -> bool:
        """
        Public method to ingest a batch of data into Snowflake.

        This method calls the internal implementation which may be wrapped
        with retry logic if a retry manager is configured.

        Args:
            channel_name: Name of the logical channel (for logging/tracking)
            data_batch: List of dictionaries containing data to ingest
            partition_id: Partition ID for channel selection

        Returns:
            True if ingestion was successful, False otherwise
        """
        with logfire.span(
            "snowflake.ingest_batch_with_retry",
            channel_name=channel_name,
            partition_id=partition_id,
            batch_size=len(data_batch),
            has_retry_manager=self.retry_manager is not None,
        ) as span:
            try:
                result: bool = self._ingest_with_retry(channel_name, data_batch, partition_id)
                span.set_attribute("success", result)
                return result
            except Exception as e:
                # After all retries exhausted (or no retry configured)
                logger.error(
                    f"❌ Batch ingestion FAILED after all retry attempts: {e}",
                    exc_info=True,
                )
                logfire.error(
                    "Batch ingestion failed after retries",
                    channel=channel_name,
                    partition=partition_id,
                    batch_size=len(data_batch),
                    error=str(e),
                )
                span.set_attribute("success", False)
                span.set_attribute("error", str(e))
                self.stats["retry_stats"]["failed_retries"] += 1
                return False

    def create_channel_name(
        self,
        eventhub_name: str,
        environment: str = "dev",
        region: str = "default",
    ) -> str:
        """
        Create a deterministic channel name for troubleshooting.

        Format: {eventhub}-{env}-{region}-{client_suffix}
        """
        return f"{eventhub_name}-{environment}-{region}-{self.client_name_suffix}"

    def get_stats(self) -> dict[str, Any]:
        """Get client statistics."""
        stats = self.stats.copy()

        # Calculate runtime
        if stats["client_created_at"] is not None:
            runtime = datetime.now(UTC) - stats["client_created_at"]
            runtime_seconds = runtime.total_seconds()
            stats["runtime_seconds"] = runtime_seconds

            if stats["total_messages_sent"] > 0 and runtime_seconds > 0:
                stats["messages_per_second"] = stats["total_messages_sent"] / runtime_seconds
            elif stats["total_messages_sent"] > 0:
                stats["messages_per_second"] = 0.0

        return stats

    def health_check(self) -> dict[str, Any]:
        """Perform a health check of the streaming client."""
        health: dict[str, Any] = {
            "client_status": "placeholder",
            "connection_active": False,
            "channels_count": len(self.channels),
            "errors": [],
        }

        try:
            # TODO: Implement health check with Snowflake Streaming SDK
            # Test connection or channel health
            health["errors"].append("Snowflake SDK integration not implemented")

        except Exception as e:
            health["errors"].append(f"Health check error: {e}")

        return health


def create_snowflake_streaming_client(
    snowflake_config: SnowflakeConfig,
    connection_config: SnowflakeConnectionConfig,
    client_name_suffix: str | None = None,
    retry_manager: Any | None = None,
) -> SnowflakeHighPerformanceStreamingClient:
    """Factory function to create a Snowflake High-Performance streaming client."""
    return SnowflakeHighPerformanceStreamingClient(
        snowflake_config=snowflake_config,
        connection_config=connection_config,
        client_name_suffix=client_name_suffix,
        retry_manager=retry_manager,
    )


# Example usage
if __name__ == "__main__":
    import logging

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    print("Snowflake streaming client module loaded successfully")
    print("Use create_snowflake_streaming_client() to create client instances")
    print(
        "NOTE: This is a placeholder implementation that needs Snowflake Streaming SDK integration"
    )
