"""
Abstract base class for Snowflake streaming clients.

This module defines the interface that both Classic and High-Performance
SDK implementations must follow, enabling runtime switching between modes.
"""

from abc import ABC, abstractmethod
from typing import Any

from utils.config import SnowflakeConfig, SnowflakeConnectionConfig


class SnowflakeStreamingClientBase(ABC):
    """
    Abstract base class for Snowflake streaming clients.

    Defines the interface for ingesting data into Snowflake using either:
    - Classic SDK (snowflake-ingest classic architecture)
    - High-Performance SDK (snowpipe-streaming high-performance architecture)

    Implementations must provide methods for:
    - Client lifecycle (start, stop)
    - Data ingestion (ingest_batch)
    - Status reporting (get_stats)
    """

    def __init__(
        self,
        snowflake_config: SnowflakeConfig,
        connection_config: SnowflakeConnectionConfig,
        client_name_suffix: str | None = None,
        retry_manager: Any | None = None,
    ):
        """
        Initialize the streaming client base.

        Args:
            snowflake_config: Snowflake target configuration (database, schema, table)
            connection_config: Snowflake connection settings (account, credentials, SDK mode)
            client_name_suffix: Optional suffix for client identification
            retry_manager: Optional retry manager for error handling
        """
        self.snowflake_config = snowflake_config
        self.connection_config = connection_config
        self.client_name_suffix = client_name_suffix
        self.retry_manager = retry_manager

    @abstractmethod
    def start(self) -> None:
        """
        Initialize the Snowflake streaming client.

        Must establish connection, authenticate, and prepare for data ingestion.

        Raises:
            Exception: If client initialization fails
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """
        Stop the Snowflake streaming client and clean up resources.

        Must close all channels, flush pending data, and release connections.
        """
        pass

    @abstractmethod
    def ingest_batch(
        self,
        channel_name: str,
        data_batch: list[dict[str, Any]],
        partition_id: str = "0",
    ) -> bool:
        """
        Ingest a batch of messages into Snowflake.

        Args:
            channel_name: Name of the logical channel (for logging/tracking)
            data_batch: List of message dictionaries to ingest
            partition_id: Source partition identifier (for channel management)

        Returns:
            True if ingestion was successful, False otherwise

        Raises:
            Exception: If ingestion fails and retry is exhausted
        """
        pass

    @abstractmethod
    def get_stats(self) -> dict[str, Any]:
        """
        Get statistics about the streaming client.

        Returns:
            Dictionary with client statistics:
            {
                "client_created_at": datetime,
                "total_messages_sent": int,
                "total_batches_sent": int,
                "last_ingestion": datetime,
                "channels": dict,
                "retry_stats": dict,
            }
        """
        pass

    @property
    @abstractmethod
    def is_started(self) -> bool:
        """Check if the client is started and ready for ingestion."""
        pass
