"""
Checkpoint management for Event Hub using database-backed storage.

Provides a CheckpointStore compatible with Azure EventHub SDK backed by
Snowflake or Postgres control tables.
"""

import logging
import time
from collections.abc import Iterable
from typing import Any, Protocol

import logfire
from azure.eventhub.aio import CheckpointStore

from utils.config import PostgresConnectionConfig, SnowflakeConnectionConfig

logger = logging.getLogger(__name__)

__all__ = [
    "DatabaseCheckpointStore",
    "PostgresCheckpointManager",
    "PostgresCheckpointStore",
    "SnowflakeCheckpointManager",
    "SnowflakeCheckpointStore",
]


class CheckpointManagerProtocol(Protocol):
    """Protocol for checkpoint managers used by the Azure SDK store."""

    async def get_last_checkpoint(self) -> dict[str, int] | None: ...

    async def save_checkpoint(
        self,
        partition_checkpoints: dict[str, int],
        partition_metadata: dict[str, dict[str, Any]] | None = None,
    ) -> bool: ...

    def close(self) -> None: ...


class SnowflakeCheckpointManager:
    """Manages checkpoints using Snowflake tables."""

    def __init__(
        self,
        eventhub_namespace: str,
        eventhub_name: str,
        target_db: str,
        target_schema: str,
        target_table: str,
        snowflake_config: SnowflakeConnectionConfig | None = None,
        session=None,
        control_db: str | None = None,
        control_schema: str | None = None,
        control_table: str | None = None,
    ):
        self.eventhub_namespace = eventhub_namespace
        self.eventhub_name = eventhub_name
        self.target_db = target_db
        self.target_schema = target_schema
        self.target_table = target_table
        self.control_db = control_db
        self.control_schema = control_schema
        self.control_table = control_table
        self.snowflake_config = snowflake_config
        self.session = session
        self._external_session = session is not None

    async def get_last_checkpoint(self) -> dict[str, int] | None:
        """Get last checkpoint per partition from Snowflake."""
        try:
            from utils.snowflake import get_partition_checkpoints

            result: dict[str, int] | None = get_partition_checkpoints(
                eventhub_namespace=self.eventhub_namespace,
                eventhub=self.eventhub_name,
                target_db=self.target_db,
                target_schema=self.target_schema,
                target_table=self.target_table,
                config=self.snowflake_config,
                control_db=self.control_db,
                control_schema=self.control_schema,
                control_table=self.control_table,
            )

            if result:
                logger.info("Loaded per-partition checkpoints: %s", result)
            else:
                logger.info("No checkpoints found, starting from beginning")

            return result

        except Exception as e:
            logger.error("Failed to get last checkpoint: %s", e, exc_info=True)
            return None

    async def save_checkpoint(
        self,
        partition_checkpoints: dict[str, int],
        partition_metadata: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        """Persist partition checkpoints to Snowflake."""
        with logfire.span(
            "eventhub.checkpoint_save",
            partitions_count=len(partition_checkpoints),
            partition_ids=list(partition_checkpoints.keys()),
            eventhub_name=self.eventhub_name,
        ) as span:
            try:
                from utils.snowflake import insert_partition_checkpoint

                checkpoints_saved = 0
                for partition_id, waterlevel in partition_checkpoints.items():
                    metadata = None
                    if partition_metadata and partition_id in partition_metadata:
                        metadata = partition_metadata[partition_id]

                    logger.info(
                        "📝 Inserting checkpoint: partition=%s, waterlevel=%s, target=%s.%s.%s, control=%s.%s.%s",
                        partition_id,
                        waterlevel,
                        self.target_db,
                        self.target_schema,
                        self.target_table,
                        self.control_db,
                        self.control_schema,
                        self.control_table,
                    )

                    insert_partition_checkpoint(
                        eventhub_namespace=self.eventhub_namespace,
                        eventhub=self.eventhub_name,
                        target_db=self.target_db,
                        target_schema=self.target_schema,
                        target_table=self.target_table,
                        partition_id=partition_id,
                        waterlevel=waterlevel,
                        metadata=metadata,
                        config=self.snowflake_config,
                        control_db=self.control_db,
                        control_schema=self.control_schema,
                        control_table=self.control_table,
                    )
                    checkpoints_saved += 1

                span.set_attribute("checkpoints_saved", checkpoints_saved)
                span.set_attribute("success", True)

                logger.info(
                    "Checkpoint saved for %s partitions: %s",
                    len(partition_checkpoints),
                    partition_checkpoints,
                )

                logfire.info(
                    "Checkpoints saved to Snowflake",
                    eventhub_name=self.eventhub_name,
                    partitions_count=checkpoints_saved,
                    partition_ids=list(partition_checkpoints.keys()),
                )

                return True
            except Exception as e:
                logger.error("Failed to save checkpoint: %s", e, exc_info=True)
                span.set_attribute("error", str(e))
                span.set_attribute("success", False)
                logfire.error(
                    "Checkpoint save failed",
                    error=str(e),
                    eventhub_name=self.eventhub_name,
                    partitions_count=len(partition_checkpoints),
                )
                return False

    def close(self):
        """Close owned Snowflake session resources."""
        if self.session and not self._external_session:
            self.session.close()


class PostgresCheckpointManager:
    """Manages checkpoints using Postgres tables."""

    def __init__(
        self,
        eventhub_namespace: str,
        eventhub_name: str,
        target_db: str,
        target_schema: str,
        target_table: str,
        postgres_config: PostgresConnectionConfig,
        control_db: str | None = None,
        control_schema: str | None = None,
        control_table: str | None = None,
    ):
        self.eventhub_namespace = eventhub_namespace
        self.eventhub_name = eventhub_name
        self.target_db = target_db
        self.target_schema = target_schema
        self.target_table = target_table
        self.control_db = control_db
        self.control_schema = control_schema
        self.control_table = control_table
        self.postgres_config = postgres_config

    async def get_last_checkpoint(self) -> dict[str, int] | None:
        """Get last checkpoint per partition from Postgres."""
        try:
            from utils.postgres import get_partition_checkpoints

            result: dict[str, int] | None = get_partition_checkpoints(
                eventhub_namespace=self.eventhub_namespace,
                eventhub=self.eventhub_name,
                target_db=self.target_db,
                target_schema=self.target_schema,
                target_table=self.target_table,
                config=self.postgres_config,
                control_db=self.control_db,
                control_schema=self.control_schema,
                control_table=self.control_table,
            )

            if result:
                logger.info("Loaded per-partition checkpoints: %s", result)
            else:
                logger.info("No checkpoints found, starting from beginning")

            return result

        except Exception as e:
            logger.error("Failed to get last checkpoint: %s", e, exc_info=True)
            return None

    async def save_checkpoint(
        self,
        partition_checkpoints: dict[str, int],
        partition_metadata: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        """Persist partition checkpoints to Postgres."""
        with logfire.span(
            "eventhub.checkpoint_save",
            partitions_count=len(partition_checkpoints),
            partition_ids=list(partition_checkpoints.keys()),
            eventhub_name=self.eventhub_name,
        ) as span:
            try:
                from utils.postgres import insert_partition_checkpoint

                checkpoints_saved = 0
                for partition_id, waterlevel in partition_checkpoints.items():
                    metadata = None
                    if partition_metadata and partition_id in partition_metadata:
                        metadata = partition_metadata[partition_id]

                    logger.info(
                        "📝 Inserting checkpoint: partition=%s, waterlevel=%s, target=%s.%s.%s, control=%s.%s.%s",
                        partition_id,
                        waterlevel,
                        self.target_db,
                        self.target_schema,
                        self.target_table,
                        self.control_db,
                        self.control_schema,
                        self.control_table,
                    )

                    insert_partition_checkpoint(
                        eventhub_namespace=self.eventhub_namespace,
                        eventhub=self.eventhub_name,
                        target_db=self.target_db,
                        target_schema=self.target_schema,
                        target_table=self.target_table,
                        partition_id=partition_id,
                        waterlevel=waterlevel,
                        metadata=metadata,
                        config=self.postgres_config,
                        control_db=self.control_db,
                        control_schema=self.control_schema,
                        control_table=self.control_table,
                    )
                    checkpoints_saved += 1

                span.set_attribute("checkpoints_saved", checkpoints_saved)
                span.set_attribute("success", True)

                logger.info(
                    "Checkpoint saved for %s partitions: %s",
                    len(partition_checkpoints),
                    partition_checkpoints,
                )

                logfire.info(
                    "Checkpoints saved to Postgres",
                    eventhub_name=self.eventhub_name,
                    partitions_count=checkpoints_saved,
                    partition_ids=list(partition_checkpoints.keys()),
                )

                return True
            except Exception as e:
                logger.error("Failed to save checkpoint: %s", e, exc_info=True)
                span.set_attribute("error", str(e))
                span.set_attribute("success", False)
                logfire.error(
                    "Checkpoint save failed",
                    error=str(e),
                    eventhub_name=self.eventhub_name,
                    partitions_count=len(partition_checkpoints),
                )
                return False

    def close(self):
        """Close cached Postgres connections."""
        try:
            from utils.postgres import close_cached_connections

            close_cached_connections()
        except Exception as exc:
            logger.debug("Postgres connection cleanup skipped: %s", exc)


class DatabaseCheckpointStore(CheckpointStore):
    """Azure SDK-compatible checkpoint store backed by a database manager."""

    def __init__(self, checkpoint_manager: CheckpointManagerProtocol, backend_label: str):
        self.checkpoint_manager = checkpoint_manager
        self.backend_label = backend_label
        self._ownership_cache: dict[str, dict[str, Any]] = {}
        self._checkpoint_cache: dict[str, dict[str, Any]] = {}

    async def list_ownership(
        self,
        fully_qualified_namespace: str,
        eventhub_name: str,
        consumer_group: str,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return cached ownership records."""
        return list(self._ownership_cache.values())

    async def claim_ownership(
        self, ownership_list: Iterable[dict[str, Any]], **kwargs: Any
    ) -> list[dict[str, Any]]:
        """Claim ownership for partitions and cache the claims."""
        claimed = []
        for ownership in ownership_list:
            partition_id = ownership["partition_id"]
            self._ownership_cache[partition_id] = {
                "fully_qualified_namespace": ownership["fully_qualified_namespace"],
                "eventhub_name": ownership["eventhub_name"],
                "consumer_group": ownership["consumer_group"],
                "partition_id": partition_id,
                "owner_id": ownership["owner_id"],
                "last_modified_time": time.time(),
                "etag": str(time.time()),
            }
            claimed.append(self._ownership_cache[partition_id])

        logger.debug("Claimed ownership for %s partitions", len(claimed))
        return claimed

    async def update_checkpoint(self, checkpoint: dict[str, Any], **kwargs: Any) -> None:
        """Update checkpoint from SDK callback."""
        partition_id = checkpoint["partition_id"]
        offset = checkpoint["offset"]
        sequence_number = checkpoint["sequence_number"]

        logger.info(
            "🔍 SDK update_checkpoint called: partition=%s, offset=%r, sequence=%s",
            partition_id,
            offset,
            sequence_number,
        )

        self._checkpoint_cache[partition_id] = checkpoint

        try:
            offset_int = int(offset)
            logger.info(
                "✅ Converted offset to int: partition=%s, offset_int=%s", partition_id, offset_int
            )
        except (ValueError, TypeError) as e:
            logger.error(
                "❌ Invalid offset format: offset=%r, type=%s, error=%s, falling back to sequence_number",
                offset,
                type(offset).__name__,
                e,
            )
            offset_int = sequence_number

        partition_checkpoints = {partition_id: offset_int}
        partition_metadata = {
            partition_id: {
                "sequence_number": sequence_number,
                "offset_string": offset,
                "fully_qualified_namespace": checkpoint.get("fully_qualified_namespace"),
                "eventhub_name": checkpoint.get("eventhub_name"),
                "consumer_group": checkpoint.get("consumer_group"),
            }
        }

        logger.info(
            "💾 Calling save_checkpoint: partition=%s, waterlevel=%s, metadata.sequence_number=%s",
            partition_id,
            offset_int,
            sequence_number,
        )

        success = await self.checkpoint_manager.save_checkpoint(
            partition_checkpoints, partition_metadata
        )

        if success:
            logger.debug(
                "Checkpoint updated for partition %s: offset=%s, sequence=%s",
                partition_id,
                offset,
                sequence_number,
            )
        else:
            logger.warning("Failed to update checkpoint for partition %s", partition_id)

    async def list_checkpoints(
        self,
        fully_qualified_namespace: str,
        eventhub_name: str,
        consumer_group: str,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """List checkpoints in Azure SDK format."""
        checkpoints_data = await self.checkpoint_manager.get_last_checkpoint()

        if not checkpoints_data:
            return []

        checkpoints = []
        for partition_id, offset_value in checkpoints_data.items():
            checkpoint = {
                "fully_qualified_namespace": fully_qualified_namespace,
                "eventhub_name": eventhub_name,
                "consumer_group": consumer_group,
                "partition_id": partition_id,
                "offset": str(offset_value),
                "sequence_number": offset_value,
            }
            self._checkpoint_cache[partition_id] = checkpoint
            checkpoints.append(checkpoint)
            logger.info(
                "📍 Returning checkpoint to SDK: partition=%s, offset=%s",
                partition_id,
                offset_value,
            )

        logger.info(
            "Loaded %s checkpoints from %s for SDK", len(checkpoints), self.backend_label
        )
        return checkpoints


class SnowflakeCheckpointStore(DatabaseCheckpointStore):
    """Azure SDK-compatible checkpoint store backed by Snowflake."""

    def __init__(self, checkpoint_manager: SnowflakeCheckpointManager):
        super().__init__(checkpoint_manager, backend_label="Snowflake")


class PostgresCheckpointStore(DatabaseCheckpointStore):
    """Azure SDK-compatible checkpoint store backed by Postgres."""

    def __init__(self, checkpoint_manager: PostgresCheckpointManager):
        super().__init__(checkpoint_manager, backend_label="Postgres")
