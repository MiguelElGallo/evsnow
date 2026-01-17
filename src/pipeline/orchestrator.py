"""
Pipeline orchestrator for EvSnow pipeline.

This module coordinates multiple EventHub consumers and Snowflake streaming clients
to provide a complete data pipeline from Azure Event Hubs to Snowflake.

The orchestrator:
1. Manages multiple EventHub->Snowflake mapping pairs concurrently
2. Coordinates EventHub consumers with Snowflake streaming clients
3. Provides comprehensive monitoring and health checking
4. Handles graceful shutdown and resource cleanup
5. Offers runtime statistics and status reporting
"""

import asyncio
import logging
import os
import signal
from datetime import UTC, datetime
from typing import Any

import logfire

from consumers.eventhub import (
    EventHubAsyncConsumer,
    EventHubMessage,
)
from streaming.snowflake import (
    SnowflakeStreamingClient,
    create_snowflake_streaming_client,
)
from utils.config import (
    EventHubSnowflakeMapping,
    EvSnowConfig,
)

logger = logging.getLogger(__name__)


class PipelineMapping:
    """Represents a single EventHub -> Snowflake mapping with its components."""

    def __init__(
        self,
        mapping_config: EventHubSnowflakeMapping,
        pipeline_config: EvSnowConfig,
        retry_manager: Any | None = None,
    ):
        self.mapping_config = mapping_config
        self.pipeline_config = pipeline_config
        self.retry_manager = retry_manager

        # Get component configurations
        self.eventhub_config = pipeline_config.get_event_hub_config(mapping_config.event_hub_key)
        self.snowflake_config = pipeline_config.get_snowflake_config(mapping_config.snowflake_key)

        if not self.eventhub_config or not self.snowflake_config:
            raise ValueError(f"Invalid mapping configuration: {mapping_config}")

        self.channel_name = pipeline_config.generate_channel_name(
            mapping_config.event_hub_key,
            pipeline_config.client_id,
        )

        # Initialize components
        self.eventhub_consumer: EventHubAsyncConsumer | None = None
        self.snowflake_client: SnowflakeStreamingClient | None = None
        self.running = False

        # Statistics
        self.stats: dict[str, Any] = {
            "mapping_key": f"{mapping_config.event_hub_key}->{mapping_config.snowflake_key}",
            "destination_type": "Snowflake",
            "started_at": None,
            "messages_processed": 0,
            "batches_processed": 0,
            "last_activity": None,
            "errors": [],
        }

    def start(self) -> None:
        """Start the mapping components."""
        if self.running:
            logger.warning(f"Mapping {self.stats['mapping_key']} is already running")
            return

        logger.info(f"Starting mapping: {self.stats['mapping_key']} (Snowflake)")

        # Validate configurations
        if not self.eventhub_config or not self.snowflake_config:
            raise ValueError("Missing EventHub or Snowflake configuration")

        try:
            # Start Snowflake streaming client
            if not self.pipeline_config.snowflake_connection:
                raise ValueError("Snowflake connection configuration is required")

            self.snowflake_client = create_snowflake_streaming_client(
                snowflake_config=self.snowflake_config,
                connection_config=self.pipeline_config.snowflake_connection,
                client_name_suffix=self.pipeline_config.client_id,
                retry_manager=self.retry_manager,
            )
            self.snowflake_client.start()

            # Create message processor that uses Snowflake client
            def message_processor(messages: list[EventHubMessage]) -> bool:
                return self._process_messages(messages)

            # Create EventHub consumer (synchronous creation)
            self.eventhub_consumer = EventHubAsyncConsumer(
                eventhub_config=self.eventhub_config,
                target_db=self.snowflake_config.database,
                target_schema=self.snowflake_config.schema_name,
                target_table=self.snowflake_config.table_name or "events",
                message_processor=message_processor,
                snowflake_config=self.pipeline_config.snowflake_connection,
                batch_size=self.snowflake_config.batch_size,
                control_db=self.pipeline_config.target_db,
                control_schema=self.pipeline_config.target_schema,
                control_table=self.pipeline_config.target_table,
                control_table_backend=self.pipeline_config.control_table_backend,
                control_postgres_config=self.pipeline_config.control_postgres,
                capture_messages=bool(getattr(self.pipeline_config, "capture_messages", False)),
                capture_messages_dir=str(
                    getattr(self.pipeline_config, "capture_messages_dir", "messages")
                ),
            )

            self.running = True
            self.stats["started_at"] = datetime.now(UTC)

            logger.info(f"Mapping {self.stats['mapping_key']} started successfully")

        except Exception as e:
            logger.error(f"Failed to start mapping {self.stats['mapping_key']}: {e}")
            if self.snowflake_client:
                self.snowflake_client.stop()
            raise

    async def start_async(self) -> None:
        """Start the async components (EventHub consumer)."""
        if not self.running or not self.eventhub_consumer:
            raise RuntimeError("Mapping must be started before starting async components")

        logger.info(f"Starting async components for mapping: {self.stats['mapping_key']}")

        # Wrap consumer execution in a span to show active mapping in hierarchy
        with logfire.span(
            f"mapping.{self.stats['mapping_key']}",
            event_hub=self.eventhub_config.name if self.eventhub_config else "unknown",
            destination_type="Snowflake",
            destination_table=self.snowflake_config.table_name
            if self.snowflake_config
            else "unknown",
            batch_size=self.snowflake_config.batch_size if self.snowflake_config else 0,
        ):
            await self.eventhub_consumer.start()

    async def stop(self) -> None:
        """Stop the mapping components gracefully."""
        if not self.running:
            return

        logger.info(f"🛑 Stopping mapping: {self.stats['mapping_key']}")
        self.running = False

        # Stop EventHub consumer first (this will process remaining messages and save checkpoints)
        if self.eventhub_consumer:
            logger.info(f"📦 Finalizing EventHub consumer for {self.stats['mapping_key']}...")
            await self.eventhub_consumer.stop()
            # DON'T set to None yet - keep reference until Snowflake is closed

        # Stop Snowflake client and ensure flush completes
        # CRITICAL: Do this BEFORE setting eventhub_consumer to None to avoid GC issues
        if self.snowflake_client:
            logger.info(
                f"🔌 Flushing and closing Snowflake client for {self.stats['mapping_key']}..."
            )
            # Run synchronous stop() in executor to not block the event loop
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.snowflake_client.stop)
            # Give extra time for flush to propagate through Snowflake infrastructure
            logger.info("⏳ Waiting for Snowflake flush to complete...")
            await asyncio.sleep(3)
            logger.info(f"✅ Snowflake client flushed and closed for {self.stats['mapping_key']}")
            self.snowflake_client = None

        # Now safe to clear EventHub consumer reference
        if self.eventhub_consumer:
            self.eventhub_consumer = None

        logger.info(f"✅ Mapping {self.stats['mapping_key']} stopped gracefully")

    def _process_messages(self, messages: list[EventHubMessage]) -> bool:
        """Process a batch of EventHub messages by sending them to Snowflake."""
        with logfire.span(
            "orchestrator.process_messages",
            message_count=len(messages),
            mapping_key=self.stats["mapping_key"],
            destination_type="Snowflake",
        ) as span:
            logger.debug(f"Processing {len(messages)} messages")

            if not self.snowflake_client:
                logger.error("❌ Snowflake client not available for message processing")
                span.set_attribute("error", "missing_snowflake_client")
                span.set_attribute("success", False)
                return False

            if not self.eventhub_config:
                logger.error("❌ EventHub config not available for message processing")
                span.set_attribute("error", "missing_eventhub_config")
                span.set_attribute("success", False)
                return False

            try:
                logger.debug(f"Converting {len(messages)} messages to dict format...")
                # Convert messages to data format
                data_batch = [msg.to_dict() for msg in messages]

                # Log first message as sample
                if data_batch:
                    logger.debug(f"Sample message: {str(data_batch[0])[:200]}...")

                # Ingest data with channel name (for logging/tracking)
                channel_name = self.channel_name
                span.set_attribute("channel_name", channel_name)

                # Get partition_id from first message for Snowflake channel management
                partition_id = messages[0].partition_id if messages else "0"

                logger.debug(
                    f"Sending batch of {len(data_batch)} messages to Snowflake (channel: {channel_name})..."
                )

                success = self.snowflake_client.ingest_batch(
                    channel_name=channel_name,
                    data_batch=data_batch,
                    partition_id=partition_id,
                )

                if success:
                    self.stats["messages_processed"] += len(messages)
                    self.stats["batches_processed"] += 1
                    self.stats["last_activity"] = datetime.now(UTC)
                    logger.info(
                        f"✅ Processed batch: {len(messages)} messages. "
                        f"Total: {self.stats['messages_processed']}"
                    )
                    span.set_attribute("success", True)
                    span.set_attribute("total_messages_processed", self.stats["messages_processed"])
                    return True
                else:
                    logger.error(f"❌ Failed to ingest {len(messages)} messages")
                    self.stats["errors"].append(
                        {"timestamp": datetime.now(UTC), "message_count": len(messages)}
                    )
                    span.set_attribute("success", False)
                    return False

            except Exception as e:
                logger.error(f"❌ Error processing messages: {e}", exc_info=True)
                self.stats["errors"].append(
                    {
                        "timestamp": datetime.now(UTC),
                        "error": str(e),
                        "message_count": len(messages),
                    }
                )
                span.set_attribute("error", str(e))
                span.set_attribute("success", False)
                return False

    def get_stats(self) -> dict[str, Any]:
        """Get mapping statistics."""
        stats = self.stats.copy()

        # Calculate runtime
        if stats["started_at"] is not None:
            runtime = datetime.now(UTC) - stats["started_at"]
            stats["runtime_seconds"] = runtime.total_seconds()

            if stats["messages_processed"] > 0:
                stats["messages_per_second"] = (
                    stats["messages_processed"] / stats["runtime_seconds"]
                )

        return stats

    def health_check(self) -> dict[str, Any]:
        """Perform health check on mapping components."""
        components: dict[str, Any] = {}
        errors: list[str] = []

        # Check Snowflake client
        if self.snowflake_client:
            components["snowflake"] = self.snowflake_client.health_check()
        else:
            errors.append("Snowflake client not initialized")

        # Check EventHub consumer
        if self.eventhub_consumer:
            components["eventhub"] = {"status": "initialized"}
        else:
            errors.append("EventHub consumer not initialized")

        return {
            "mapping_key": self.stats["mapping_key"],
            "running": self.running,
            "components": components,
            "errors": errors,
        }


class PipelineOrchestrator:
    """
    Main pipeline orchestrator that manages multiple EventHub->Snowflake mappings.

    This orchestrator:
    - Initializes and manages multiple mapping pairs
    - Coordinates concurrent processing across all mappings
    - Handles graceful shutdown and error recovery
    - Provides aggregated statistics and health monitoring
    """

    def __init__(self, config: EvSnowConfig, retry_manager: Any | None = None):
        self.config = config
        self.retry_manager = retry_manager
        self.mappings: list[PipelineMapping] = []
        self.running = False
        self.shutdown_requested = False  # Flag to track shutdown requests
        self.shutdown_task: asyncio.Task | None = None
        self.tasks: list[asyncio.Task] = []

        # Statistics
        self.stats: dict[str, Any] = {
            "orchestrator_created_at": datetime.now(UTC),
            "mappings_count": 0,
            "total_messages_processed": 0,
        }

        logger.info("Pipeline orchestrator initialized")

    def initialize(self) -> None:
        """Initialize all pipeline mappings."""
        logger.info(f"Initializing {len(self.config.mappings)} pipeline mappings...")

        for mapping_config in self.config.mappings:
            try:
                mapping = PipelineMapping(
                    mapping_config=mapping_config,
                    pipeline_config=self.config,
                    retry_manager=self.retry_manager,
                )

                # Start the mapping (synchronous initialization)
                mapping.start()

                self.mappings.append(mapping)
                mappings_count = int(self.stats.get("mappings_count", 0))
                self.stats["mappings_count"] = mappings_count + 1

                logger.info(f"✓ Initialized mapping: {mapping.stats['mapping_key']}")

            except Exception as e:
                logger.error(f"✗ Failed to initialize mapping {mapping_config}: {e}")
                raise

        logger.info(f"Successfully initialized {len(self.mappings)} mappings")

    def start(self) -> None:
        """Start the orchestrator (synchronous initialization)."""
        if self.running:
            logger.warning("Orchestrator is already running")
            return

        logger.info("Starting pipeline orchestrator...")
        self.initialize()
        self.running = True
        logger.info("Pipeline orchestrator started")

    async def run_async(self) -> None:
        """Run the pipeline asynchronously."""
        if not self.running:
            raise RuntimeError("Orchestrator must be started before running async")

        logger.info("Starting async pipeline execution...")

        try:
            # Start all mappings asynchronously
            self.tasks = []
            for mapping in self.mappings:
                task = asyncio.create_task(mapping.start_async())
                self.tasks.append(task)
                logger.info(f"Started async task for mapping: {mapping.stats['mapping_key']}")

            logger.info(f"All {len(self.tasks)} mapping tasks started")

            # Wait for all tasks (they should run indefinitely until stopped)
            results = await asyncio.gather(*self.tasks, return_exceptions=True)
            errors = [
                result
                for result in results
                if isinstance(result, Exception)
                and not isinstance(result, asyncio.CancelledError)
            ]
            if errors:
                for error in errors:
                    logger.error("Mapping task failed: %s", error)
                raise errors[0]

        except asyncio.CancelledError:
            logger.info("Pipeline execution cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in pipeline execution: {e}")
            raise

    async def stop(self) -> None:
        """Stop the orchestrator and all mappings gracefully."""
        if not self.running:
            return

        logger.info("Stopping pipeline orchestrator...")
        self.running = False

        # Stop all mappings gracefully (don't cancel tasks - let them shut down properly)
        for mapping in self.mappings:
            try:
                await mapping.stop()
            except Exception as e:
                logger.error(
                    f"Error stopping mapping {mapping.stats['mapping_key']}: {e}", exc_info=True
                )

        # Now wait for all tasks to complete
        if self.tasks:
            pending_tasks = [task for task in self.tasks if not task.done()]
            if pending_tasks:
                logger.info(
                    "⏳ Waiting for %d mapping tasks to finish...",
                    len(pending_tasks),
                )
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*pending_tasks, return_exceptions=True),
                        timeout=5,
                    )
                except TimeoutError:
                    logger.warning(
                        "Timed out waiting for mapping tasks; cancelling pending tasks..."
                    )
                    for task in pending_tasks:
                        if not task.done():
                            task.cancel()
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*pending_tasks, return_exceptions=True),
                            timeout=5,
                        )
                    except TimeoutError:
                        logger.error("Mapping tasks did not cancel; forcing exit to avoid hang")
                        os._exit(1)

        # Stop all mappings
        for mapping in self.mappings:
            await mapping.stop()

        self.mappings.clear()
        self.tasks.clear()

        # Clean up cached Snowflake connections
        try:
            from utils.snowflake import close_all_cached_connections

            close_all_cached_connections()
            logger.info("Cached Snowflake connections cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up Snowflake connections: {e}")

        logger.info("Pipeline orchestrator stopped")

    def get_stats(self) -> dict[str, Any]:
        """Get aggregated pipeline statistics."""
        stats = self.stats.copy()

        # Add runtime
        created_at = stats.get("orchestrator_created_at")
        if created_at and isinstance(created_at, datetime):
            runtime = datetime.now(UTC) - created_at
            stats["runtime_seconds"] = runtime.total_seconds()

        # Aggregate mapping stats
        total_messages = sum(m.stats["messages_processed"] for m in self.mappings)
        total_batches = sum(m.stats["batches_processed"] for m in self.mappings)

        stats["total_messages_processed"] = total_messages
        stats["total_batches_processed"] = total_batches

        # Add per-mapping stats
        stats["mappings"] = [m.get_stats() for m in self.mappings]

        return stats

    def health_check(self) -> dict[str, Any]:
        """Perform health check on the orchestrator and all mappings."""
        health: dict[str, Any] = {
            "orchestrator_status": "running" if self.running else "stopped",
            "mappings_count": len(self.mappings),
            "mappings": [],
            "errors": [],
        }

        for mapping in self.mappings:
            mapping_health = mapping.health_check()
            if isinstance(health["mappings"], list):
                health["mappings"].append(mapping_health)

            mapping_errors = mapping_health.get("errors")
            if mapping_errors and isinstance(health["errors"], list):
                health["errors"].extend(mapping_errors)

        return health

    def setup_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Setup signal handlers for graceful shutdown.

        Args:
            loop: The asyncio event loop to use for handling signals
        """

        def signal_handler(sig: signal.Signals) -> None:
            if self.shutdown_requested:
                logger.warning(
                    f"Received signal {sig.name} ({sig.value}) again - forcing immediate shutdown"
                )
                # Force exit on second signal
                os._exit(1)

            logger.info(
                f"Received signal {sig.name} ({sig.value}), initiating graceful shutdown..."
            )
            self.shutdown_requested = True

            # Stop the orchestrator gracefully - DON'T cancel tasks
            # The stop() method will handle proper shutdown of all components
            if self.shutdown_task is None or self.shutdown_task.done():
                self.shutdown_task = loop.create_task(self.stop())

        # Use asyncio's add_signal_handler for proper async signal handling
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s: signal_handler(s), sig)


async def run_pipeline(config: EvSnowConfig, retry_manager: Any | None = None) -> None:
    """
    Main entry point to run the pipeline.

    Args:
        config: Pipeline configuration
        retry_manager: Optional retry manager for smart retry logic
    """
    orchestrator = PipelineOrchestrator(config=config, retry_manager=retry_manager)
    loop = asyncio.get_running_loop()

    try:
        # Start orchestrator (synchronous initialization)
        orchestrator.start()
        orchestrator.setup_signal_handlers(loop)

        # Run async pipeline
        await orchestrator.run_async()

    except asyncio.CancelledError:
        logger.info("Pipeline cancelled, shutting down...")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        raise
    finally:
        # Always cleanup
        await orchestrator.stop()
        logger.info("Pipeline shutdown complete")
