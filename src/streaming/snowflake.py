"""
Snowflake streaming client facade (high-performance only).

Re-exports the high-performance streaming factory and base class for use by the orchestrator.
"""

from streaming.base import SnowflakeStreamingClientBase as SnowflakeStreamingClient
from streaming.factory import create_snowflake_client as create_snowflake_streaming_client

__all__ = ["SnowflakeStreamingClient", "create_snowflake_streaming_client"]
