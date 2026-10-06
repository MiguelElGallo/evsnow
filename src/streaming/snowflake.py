"""
Snowflake streaming client facade (high-performance only).

Re-exports the Named/Elastic streaming factory and base class for the orchestrator.
"""

from streaming.base import SnowflakeStreamingClientBase as SnowflakeStreamingClient
from streaming.factory import create_snowflake_client as create_snowflake_streaming_client

__all__ = ["SnowflakeStreamingClient", "create_snowflake_streaming_client"]
