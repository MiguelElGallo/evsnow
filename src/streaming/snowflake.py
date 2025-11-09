"""
Snowflake streaming client facade for backward compatibility.

This module provides a unified interface to the Snowflake streaming clients,
re-exporting the factory function and base class for use by the orchestrator.

The actual implementations are in:
- streaming.snowflake_high_performance: High-Performance SDK (PIPE-based, ~10 GB/s)
- streaming.snowflake_classic: Classic SDK (staged files, ~1-2 GB/s)

The factory function in streaming.factory selects the appropriate implementation
based on the SNOWFLAKE_STREAMING_MODE configuration.

Usage:
    from streaming.snowflake import (
        SnowflakeStreamingClient,
        create_snowflake_streaming_client,
    )

    client = create_snowflake_streaming_client(
        snowflake_config=config.snowflake_configs["SNOWFLAKE_1"],
        connection_config=config.snowflake_connection,
    )
"""

from streaming.base import SnowflakeStreamingClientBase as SnowflakeStreamingClient
from streaming.factory import create_snowflake_client as create_snowflake_streaming_client

__all__ = ["SnowflakeStreamingClient", "create_snowflake_streaming_client"]
