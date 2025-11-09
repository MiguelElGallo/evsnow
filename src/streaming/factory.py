"""
Factory for creating Snowflake streaming clients.

This module provides a factory function for the High-Performance Snowpipe Streaming SDK.
Based on: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started

Usage:
    from streaming.factory import create_snowflake_client

    client = create_snowflake_client(
        snowflake_config=config.snowflake_configs["SNOWFLAKE_1"],
        connection_config=config.snowflake_connection,
    )

    client.start()
    result = client.ingest_batch(channel_name, data_batch, partition_id)
    client.stop()
"""

import logging
from typing import Any

from streaming.base import SnowflakeStreamingClientBase
from streaming.snowflake_high_performance import SnowflakeHighPerformanceStreamingClient
from utils.config import SnowflakeConfig, SnowflakeConnectionConfig

logger = logging.getLogger(__name__)


def create_snowflake_client(
    snowflake_config: SnowflakeConfig,
    connection_config: SnowflakeConnectionConfig,
    client_name_suffix: str | None = None,
    retry_manager: Any | None = None,
) -> SnowflakeStreamingClientBase:
    """
    Create a Snowflake High-Performance streaming client.

    Uses the High-Performance Snowpipe Streaming SDK (v1.0.2+) with PIPE objects
    for maximum throughput (~10 GB/s per table).

    Reference: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started

    Args:
        snowflake_config: Target database/schema/table configuration
        connection_config: Connection settings including pipe_name
        client_name_suffix: Optional client identifier suffix
        retry_manager: Optional retry manager for error handling

    Returns:
        SnowflakeHighPerformanceStreamingClient: Configured high-performance client

    Raises:
        ValueError: If required config is missing (e.g., pipe_name)

    Example:
        # Set environment variables:
        export SNOWFLAKE_STREAMING_MODE=high-performance
        export SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE

        client = create_snowflake_client(snowflake_config, connection_config)
    """
    logger.info(
        f"Creating Snowflake HIGH-PERFORMANCE SDK streaming client "
        f"for {snowflake_config.database}.{snowflake_config.schema_name}."
        f"{snowflake_config.table_name}"
    )

    if not connection_config.pipe_name:
        raise ValueError(
            "pipe_name is required for high-performance streaming. "
            "Set SNOWFLAKE_PIPE_NAME environment variable. "
            "Example: EVENTS_TABLE_PIPE"
        )

    logger.info(
        f"✅ High-Performance SDK: ~10 GB/s throughput, "
        f"requires PIPE object: {connection_config.pipe_name}"
    )

    return SnowflakeHighPerformanceStreamingClient(
        snowflake_config=snowflake_config,
        connection_config=connection_config,
        client_name_suffix=client_name_suffix,
        retry_manager=retry_manager,
    )
