"""
Snowflake High-Performance SDK streaming client implementation.

This module provides a streaming client using the HIGH-PERFORMANCE Snowpipe Streaming architecture:
- Uses snowpipe-streaming SDK v1.1.0+ (requires PIPE object)
- Designed for high throughput (~10 GB/s per table)
- Server-side schema validation
- Supports in-flight transformations via PIPE

⚠️  KNOWN ISSUE: Currently fails on Azure-hosted Snowflake with AWS_KEY_ID error.
    SDK bug prevents Azure deployment. Use Classic SDK for Azure until fixed.

Best for:
- AWS-hosted Snowflake (works reliably)
- GCP-hosted Snowflake (should work, needs testing)
- High-throughput requirements (>2 GB/s)
- Once Azure SDK bug is fixed (expected in v1.2.0+)

Documentation: https://docs.snowflake.com/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview
Issue: See SNOWFLAKE_SDK_ISSUE.md for Azure compatibility details
"""

import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import logfire

# Snowflake High-Performance Streaming SDK (v1.1.0+)
# ⚠️  Known Azure bug: expects AWS_KEY_ID field in API response
from snowflake.ingest.streaming import StreamingIngestClient

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
    - ⚠️  Azure bug: AWS_KEY_ID deserialization error
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
        self.channels: dict[str, Any] = {}  # partition_id -> channel object

        # Statistics
        self.stats: dict[str, Any] = {
            "client_created_at": None,
            "total_messages_sent": 0,
            "total_batches_sent": 0,
            "channels_created": 0,
            "last_ingestion": None,
            "retry_stats": {
                "total_retries": 0,
                "successful_retries": 0,
                "failed_retries": 0,
            },
        }

        # Apply retry decorator to implementation if retry manager is provided
        if self.retry_manager:
            decorator = self.retry_manager.get_retry_decorator()
            self._ingest_with_retry = decorator(self._ingest_batch_impl)  # type: ignore[method-assign]
        else:
            self._ingest_with_retry = self._ingest_batch_impl  # type: ignore[assignment]

    @property
    def is_started(self) -> bool:
        """Check if the client is started and ready for ingestion."""
        return self.streaming_client is not None

    def _build_connection_profile(self) -> dict[str, Any]:
        """
        Build connection profile for StreamingIngestClient.

        For the NEW high-performance architecture (v1.0.2+), the profile requires:
        - user: Snowflake username
        - account: Account identifier (e.g., "KPWHYJX-YU88540")
        - url: Full Snowflake URL with port
        - private_key_file: Path to PEM private key file (we write temp file in start())
        - role: Role to use (optional)

        IMPORTANT: Unlike classic SDK, the profile does NOT include:
        - warehouse, database, schema (passed to StreamingIngestClient constructor instead)
        - host (use full 'url' instead)
        - private_key content (use 'private_key_file' path instead)

        Reference:
        https://docs.snowflake.com/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started

        Returns a dictionary with connection parameters including private key authentication.
        """
        logger.info("Building Snowflake connection profile for high-performance SDK")

        try:
            # Load and decrypt private key to write to temp file later
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            private_key_path = Path(self.connection_config.private_key_file).expanduser().resolve()

            with private_key_path.open("rb") as key_file:
                key_data = key_file.read()

            # Decrypt the private key if password is provided
            password = None
            if self.connection_config.private_key_password:
                password = self.connection_config.private_key_password.encode()

            private_key_obj = serialization.load_pem_private_key(
                key_data, password=password, backend=default_backend()
            )

            # Convert to unencrypted PEM format (high-performance SDK requires unencrypted key file)
            private_key_pem = private_key_obj.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),  # No encryption for high-performance SDK
            ).decode("utf-8")

            # Build simplified profile for HIGH-PERFORMANCE SDK (v1.0.2+)
            # Reference: https://docs.snowflake.com/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started
            profile = {
                "user": self.connection_config.user,
                "account": self.connection_config.account,
                "url": f"https://{self.connection_config.account}.snowflakecomputing.com:443",
                "private_key": private_key_pem,  # Will write to temp file in start() and replace with path
            }

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

                # Create StreamingIngestClient using HIGH-PERFORMANCE SDK (v1.0.2+)
                # Reference: https://gist.github.com/sfc-gh-chathomas/a7b06bb46907bead737954d53b3a8495
                client_name = f"evsnow_{self.client_name_suffix}"
                logger.info(f"Creating High-Performance StreamingIngestClient: {client_name}")

                # Write private key to temporary file (high-performance SDK requires file path, not content)
                import json
                import tempfile

                # Extract private_key from profile and write to temp file
                private_key_pem = profile.pop("private_key")  # Remove from profile dict

                # Create temporary private key file
                key_fd, key_path = tempfile.mkstemp(suffix=".pem", prefix="snowflake_key_")
                try:
                    with open(key_fd, "w") as f:
                        f.write(private_key_pem)

                    logger.debug(f"Private key written to temporary file: {key_path}")

                    # Add key file path to profile
                    profile["private_key_file"] = key_path

                    # Write profile JSON to temporary file
                    profile_fd, profile_path = tempfile.mkstemp(
                        suffix=".json", prefix="snowflake_profile_"
                    )
                    try:
                        with open(profile_fd, "w") as f:
                            json.dump(profile, f)

                        logger.debug(f"Profile written to temporary file: {profile_path}")

                        # Initialize High-Performance SDK client
                        # Params: client_name, db_name, schema_name, pipe_name, profile_json (file path)
                        self.streaming_client = StreamingIngestClient(
                            client_name=client_name,
                            db_name=self.snowflake_config.database,
                            schema_name=self.snowflake_config.schema_name,
                            pipe_name="EVENTS_TABLE_PIPE",  # Must match existing pipe name exactly (uppercase)
                            profile_json=profile_path,  # Pass file path, not JSON string
                        )

                        logger.info(
                            f"✅ High-Performance StreamingIngestClient created: {client_name}"
                        )
                        logfire.info(
                            "Snowflake StreamingIngestClient initialized (high-performance SDK v1.0.2+)",
                            client_name=client_name,
                            database=self.snowflake_config.database,
                            schema=self.snowflake_config.schema_name,
                            pipe="EVENTS_TABLE_PIPE",
                            table=self.snowflake_config.table_name,
                        )
                    finally:
                        # Clean up temporary profile file (SDK has read it by now)
                        import os

                        try:
                            os.unlink(profile_path)
                            logger.debug(f"Temporary profile file deleted: {profile_path}")
                        except Exception as e:
                            logger.warning(
                                f"Failed to delete temporary profile file {profile_path}: {e}"
                            )
                finally:
                    # Clean up temporary key file
                    import os

                    try:
                        os.unlink(key_path)
                        logger.debug(f"Temporary key file deleted: {key_path}")
                    except Exception as e:
                        logger.warning(f"Failed to delete temporary key file {key_path}: {e}")

                # Ensure target table exists
                self._ensure_target_table()

                self.stats["client_created_at"] = datetime.now(UTC)
                logger.info(
                    "✅ Snowflake streaming client started successfully (high-performance SDK v1.0.2+)"
                )

            except Exception as e:
                logger.error(f"Failed to start Snowflake streaming client: {e}", exc_info=True)
                logfire.error("Failed to start Snowflake client", error=str(e))
                self.stop()
                raise

    def stop(self) -> None:
        """Stop the Snowflake streaming client and clean up resources."""
        logger.info("Stopping Snowflake streaming client...")

        # Close all channels
        for channel_name, channel in self.channels.items():
            try:
                if channel is not None:
                    logger.info(f"Closing channel: {channel_name}")
                    channel.close()
                    logger.info(f"✅ Channel closed: {channel_name}")
            except Exception as e:
                logger.error(f"Error closing channel {channel_name}: {e}", exc_info=True)

        self.channels.clear()

        # Close the StreamingIngestClient
        if self.streaming_client is not None:
            try:
                logger.info("Closing StreamingIngestClient...")
                self.streaming_client.close()
                logger.info("✅ StreamingIngestClient closed")
                self.streaming_client = None
            except Exception as e:
                logger.error(f"Error closing StreamingIngestClient: {e}", exc_info=True)

        logger.info("Snowflake streaming client stopped")

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

    def _get_or_create_channel(self, partition_id: str) -> Any:
        """
        Get or create a channel for the specified partition.

        Snowflake Streaming API recommends one channel per partition for optimal parallelism.

        Args:
            partition_id: EventHub partition ID

        Returns:
            Channel object for the partition

        Raises:
            RuntimeError: If client not initialized or channel creation fails
        """
        if self.streaming_client is None:
            raise RuntimeError("StreamingIngestClient not initialized. Call start() first.")

        channel_name = (
            f"{self.snowflake_config.table_name}_partition_{partition_id}_{self.client_name_suffix}"
        )

        # Check if channel already exists
        if channel_name in self.channels:
            logger.debug(f"Using existing channel: {channel_name}")
            return self.channels[channel_name]

        # Create new channel
        try:
            logger.debug(f"Opening new channel: {channel_name} for partition {partition_id}")

            # Open channel (high-performance SDK returns tuple: (channel, status))
            # Reference: https://gist.github.com/sfc-gh-chathomas/a7b06bb46907bead737954d53b3a8495
            channel, status = self.streaming_client.open_channel(channel_name)

            self.channels[channel_name] = channel
            self.stats["channels_created"] += 1

            logger.info(
                f"✅ Channel opened: {channel_name} (status: {status}, total channels: {len(self.channels)})"
            )
            logfire.info(
                "Snowflake channel opened",
                channel_name=channel_name,
                partition_id=partition_id,
                status=str(status),
                total_channels=len(self.channels),
            )

            return channel

        except Exception as e:
            logger.error(
                f"Failed to create channel {channel_name} for partition {partition_id}: {e}",
                exc_info=True,
            )
            logfire.error(
                "Failed to create Snowflake channel",
                channel_name=channel_name,
                partition_id=partition_id,
                error=str(e),
            )
            raise

    def _ingest_batch_impl(
        self,
        channel_name: str,
        data_batch: list[dict[str, Any]],
        partition_id: str = "0",
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

            # Get or create channel for this partition
            channel = self._get_or_create_channel(partition_id)

            if channel is None:
                raise RuntimeError(
                    f"Failed to get channel for partition {partition_id}. Client may not be started."
                )

            # Insert rows into the channel (based on gist reference pattern)
            try:
                rows_inserted = 0
                for row in data_batch:
                    # Generate unique row_id: partition_id_sequence_number
                    # Use sequence_number from row if available, otherwise use index
                    sequence_number = row.get("sequence_number", rows_inserted)
                    row_id = f"{partition_id}_{sequence_number}"

                    # Insert row with unique ID
                    channel.append_row(row, row_id)
                    rows_inserted += 1

                logger.debug(
                    f"✅ Appended {rows_inserted} rows to channel {channel_name} (partition {partition_id})"
                )

                # Get latest committed offset token for verification
                offset_token = channel.get_latest_committed_offset_token()
                logger.debug(f"Latest committed offset for channel {channel_name}: {offset_token}")

            except Exception as e:
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
            self.stats["total_messages_sent"] += len(data_batch)
            self.stats["total_batches_sent"] += 1
            self.stats["last_ingestion"] = datetime.now(UTC)

            # Track ingestion metrics in span
            span.set_attribute("messages_sent", len(data_batch))
            span.set_attribute("total_messages", self.stats["total_messages_sent"])
            span.set_attribute("total_batches", self.stats["total_batches_sent"])

            logger.debug(
                f"Successfully ingested {len(data_batch)} records to {channel_name} (partition {partition_id})"
            )
            logfire.info(
                "Batch ingested successfully",
                channel=channel_name,
                partition=partition_id,
                records=len(data_batch),
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
