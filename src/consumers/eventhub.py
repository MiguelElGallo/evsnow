"""
Azure EventHub async consumer with custom Snowflake-based checkpoint management.

This module provides an EventHub consumer that:
1. Receives messages asynchronously from EventHub partitions
2. Batches messages for efficient processing (1000 messages or 5 minutes)
3. Uses Snowflake tables for checkpoint storage instead of blob storage
4. Integrates with the existing Snowflake connection utilities
5. Provides robust error handling and recovery mechanisms

Based on Azure EventHub SDK patterns but adapted for Snowflake checkpointing.
"""

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn

import logfire
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubConsumerClient, PartitionContext

from consumers.checkpoints import SnowflakeCheckpointManager, SnowflakeCheckpointStore
from consumers.messages import BytesEncoder, EventHubMessage, MessageBatch, _convert_bytes_to_str
from utils.azure_identity import build_eventhub_cli_credential
from utils.config import EventHubConfig, SnowflakeConnectionConfig

logger = logging.getLogger(__name__)

# Suppress noisy Azure EventHub SDK warnings about transient connection issues
# These warnings are expected during normal operation (e.g., partition rebalancing)
# We handle these gracefully, so we don't need to see them every time
eventhub_processor_logger = logging.getLogger("azure.eventhub.aio._eventprocessor.event_processor")
eventhub_processor_logger.setLevel(logging.ERROR)  # Only show ERROR and above, suppress WARNING

__all__ = [
    "BytesEncoder",
    "EventHubAsyncConsumer",
    "EventHubMessage",
    "MessageBatch",
    "SnowflakeCheckpointManager",
    "SnowflakeCheckpointStore",
    "_convert_bytes_to_str",
    "create_eventhub_consumer",
]


class EventHubAsyncConsumer:
    """
    Async EventHub consumer with Snowflake-based checkpoint management.

    This consumer:
    - Receives messages from EventHub asynchronously
    - Batches messages for efficient processing
    - Uses Snowflake for checkpoint storage
    - Provides callback-based message processing
    """

    def __init__(
        self,
        eventhub_config: EventHubConfig,
        target_db: str,
        target_schema: str,
        target_table: str,
        message_processor: Callable[[list[EventHubMessage]], bool],
        snowflake_config: SnowflakeConnectionConfig | None = None,
        batch_size: int = 1000,
        batch_timeout_seconds: int = 300,
        control_db: str | None = None,
        control_schema: str | None = None,
        control_table: str | None = None,
        capture_messages: bool = False,
        capture_messages_dir: str = "messages",
    ):
        self.eventhub_config = eventhub_config
        self.target_db = target_db
        self.target_schema = target_schema
        self.target_table = target_table
        self.message_processor = message_processor
        self.snowflake_config = snowflake_config
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds

        # Optional debug capture of raw messages (as received from Event Hub)
        self.capture_messages = capture_messages
        self.capture_messages_dir = capture_messages_dir
        self._capture_queue: asyncio.Queue[tuple[int, dict[str, Any]] | None] | None = None
        self._capture_task: asyncio.Task[None] | None = None
        self._capture_dropped_messages = 0

        # Control table configuration (for checkpoints)
        self.control_db = control_db or target_db
        self.control_schema = control_schema or target_schema
        self.control_table = control_table or "INGESTION_STATUS"

        # Generate unique consumer ID to help identify competing consumers
        self.consumer_id = f"evsnow_{uuid.uuid4().hex[:8]}"
        logger.info(
            f"🆔 Consumer ID: {self.consumer_id} (use this to identify competing consumers)"
        )

        # Runtime state
        self.client: EventHubConsumerClient | None = None
        self.credential: Any = None  # Azure credential (needs to be closed)
        self.checkpoint_manager: SnowflakeCheckpointManager | None = None
        self.current_batch: MessageBatch | None = None
        self.running = False
        self.tasks: set[asyncio.Task] = set()
        self._first_message_logged: set[str] = set()  # Track first message per partition
        self._batch_counter = 0

        # Statistics
        self.stats: dict[str, Any] = {
            "consumer_id": self.consumer_id,
            "messages_received": 0,
            "batches_processed": 0,
            "last_checkpoint": None,
            "start_time": None,
        }

    def _capture_dir_path(self) -> Path:
        return Path.cwd() / self.capture_messages_dir

    @staticmethod
    def _event_body_to_utf8(event: EventData) -> str:
        """Decode Event Hub message body as UTF-8 (replacement for invalid sequences)."""
        try:
            body = event.body
            if isinstance(body, (bytes, bytearray, memoryview)):
                body_bytes = bytes(body)
            elif isinstance(body, str):
                body_bytes = body.encode("utf-8", errors="replace")
            elif isinstance(body, Iterable):
                chunks: list[bytes] = []
                for chunk in body:
                    if isinstance(chunk, (bytes, bytearray, memoryview)):
                        chunks.append(bytes(chunk))
                    else:
                        chunks.append(str(chunk).encode("utf-8", errors="replace"))
                body_bytes = b"".join(chunks)
            else:
                body_bytes = str(body).encode("utf-8", errors="replace")
        except Exception:
            # Fallback: some mocks or SDK versions may expose `body` differently
            try:
                body_bytes = bytes(event.body)  # type: ignore[arg-type]
            except Exception:
                body_bytes = b""
        return body_bytes.decode("utf-8", errors="replace")

    @staticmethod
    def _to_jsonable(value: Any) -> Any:
        """Convert values into a JSON-serializable structure.

        Azure Event Hub `properties` / `system_properties` can include `bytes` keys.
        Python's `json` requires keys to be str/int/float/bool/None, so we normalize.
        """

        if value is None:
            return None

        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")

        if isinstance(value, (str, int, float, bool)):
            return value

        if isinstance(value, dict):
            converted: dict[str, Any] = {}
            for k, v in value.items():
                key = k.decode("utf-8", errors="replace") if isinstance(k, bytes) else str(k)
                converted[key] = EventHubAsyncConsumer._to_jsonable(v)
            return converted

        if isinstance(value, (list, tuple, set)):
            return [EventHubAsyncConsumer._to_jsonable(v) for v in value]

        # Fallback: stringify unknown objects (e.g., UUID, datetime in some contexts)
        return str(value)

    def _enqueue_capture(self, partition_id: str, event: EventData) -> None:
        if not self.capture_messages:
            return

        if self._capture_queue is None:
            return

        timestamp_ns = time.time_ns()
        payload: dict[str, Any] = {
            "timestamp_ns": timestamp_ns,
            "eventhub_namespace": self.eventhub_config.namespace,
            "eventhub_name": self.eventhub_config.name,
            "consumer_group": self.eventhub_config.consumer_group,
            "partition_id": partition_id,
            "offset": event.offset,
            "sequence_number": event.sequence_number,
            "enqueued_time": event.enqueued_time.isoformat() if event.enqueued_time else None,
            "content_type": getattr(event, "content_type", None),
            "body": self._event_body_to_utf8(event),
            "properties": self._to_jsonable(event.properties),
            "system_properties": self._to_jsonable(event.system_properties),
        }

        try:
            self._capture_queue.put_nowait((timestamp_ns, payload))
        except asyncio.QueueFull:
            self._capture_dropped_messages += 1
            # Avoid spamming logs
            if self._capture_dropped_messages in (1, 10, 100, 1000):
                logger.warning(
                    "Capture queue full; dropped %s messages so far",
                    self._capture_dropped_messages,
                )

    async def _capture_writer_loop(self) -> None:
        if self._capture_queue is None:
            return

        capture_dir = self._capture_dir_path()
        capture_dir.mkdir(parents=True, exist_ok=True)

        while True:
            item = await self._capture_queue.get()
            if item is None:
                self._capture_queue.task_done()
                return

            timestamp_ns, payload = item
            try:
                await asyncio.to_thread(
                    self._write_capture_file, capture_dir, timestamp_ns, payload
                )
            except Exception as exc:
                logger.warning("Failed to write captured message: %s", exc)
            finally:
                self._capture_queue.task_done()

    @staticmethod
    def _write_capture_file(
        capture_dir: Path,
        timestamp_ns: int,
        payload: dict[str, Any],
    ) -> None:
        file_path = capture_dir / f"f_{timestamp_ns}.json"
        tmp_path = capture_dir / f"f_{timestamp_ns}.json.tmp"
        safe_payload = EventHubAsyncConsumer._to_jsonable(payload)
        data = json.dumps(safe_payload, ensure_ascii=False, cls=BytesEncoder)
        tmp_path.write_text(data, encoding="utf-8")
        tmp_path.replace(file_path)

    async def _start_capture_writer(self) -> None:
        if not self.capture_messages:
            return

        if self._capture_task is not None:
            return

        # Keep queue bounded to avoid unbounded memory usage
        self._capture_queue = asyncio.Queue(maxsize=10_000)
        self._capture_task = asyncio.create_task(self._capture_writer_loop())
        logger.info("📝 Capture enabled: writing raw messages to %s/", self.capture_messages_dir)

    async def _stop_capture_writer(self) -> None:
        if self._capture_queue is None or self._capture_task is None:
            return

        try:
            await self._capture_queue.put(None)
            await asyncio.wait_for(self._capture_task, timeout=30)
        except TimeoutError:
            logger.warning("Timed out waiting for capture writer to stop")
        except Exception as exc:
            logger.warning("Error stopping capture writer: %s", exc)
        finally:
            self._capture_task = None
            self._capture_queue = None

    def _log_startup_summary(self) -> None:
        logger.info(f"🚀 Starting EventHub consumer for {self.eventhub_config.name}")
        logger.info(f"   Namespace: {self.eventhub_config.namespace}")
        logger.info(f"   Consumer Group: {self.eventhub_config.consumer_group}")
        logger.info(f"   Consumer ID: {self.consumer_id}")
        logger.info(f"   Batch Size: {self.batch_size}")
        logger.info(f"   Batch Timeout: {self.batch_timeout_seconds}s")
        logger.info("")

    def _log_competing_consumer_guidance(self) -> None:
        logger.warning("⚠️  IMPORTANT: Competing Consumer Detection")
        logger.warning("")
        logger.warning(
            f"   This consumer ({self.consumer_id}) is using consumer group '{self.eventhub_config.consumer_group}'"
        )
        logger.warning(
            "   EventHub allows ONLY ONE consumer per partition per consumer group at a time."
        )
        logger.warning("")
        logger.warning("   If you see 'amqp:link:stolen' errors, it means:")
        logger.warning("   • Another consumer instance is competing for the same partitions")
        logger.warning("   • A zombie process from a previous run is still connected")
        logger.warning("   • You're running multiple instances of this application")
        logger.warning("")
        logger.warning("   To fix:")
        logger.warning("   • Kill all running instances: ps aux | grep evsnow")
        logger.warning(
            f"   • Use a different consumer group (current: '{self.eventhub_config.consumer_group}')"
        )
        logger.warning("   • Wait 5-10 minutes for old connections to timeout")
        logger.warning("")

    def _log_no_checkpoint_behavior(self, starting_pos: str) -> None:
        logger.warning("⚠️ No checkpoints found in Snowflake.")

        if starting_pos == "-1":
            logger.info(
                f"   Starting from BEGINNING of stream (starting_position='{starting_pos}') to process ALL existing messages."
            )
        elif starting_pos == "@latest":
            logger.info(
                f"   Starting from LATEST (starting_position='{starting_pos}') - only NEW messages after connection will be received."
            )
        elif starting_pos == "0":
            logger.info(
                f"   Starting from EARLIEST available (starting_position='{starting_pos}') - processes from oldest retained message."
            )

        logger.info(
            "   This ensures consistent behavior when starting fresh. Change with EVENTHUBNAME_{N}_STARTING_POSITION_ON_NO_CHECKPOINT."
        )

    def _log_rbac_permission_validation_notice(self) -> None:
        logger.info("")
        logger.warning("⚠️  IMPORTANT: RBAC Permission Validation")
        logger.warning("")
        logger.warning("   Azure does NOT provide an API to pre-check data plane RBAC permissions.")
        logger.warning("   Permission validation only happens when SDK tries to receive messages.")
        logger.warning("")
        logger.warning("   What happens next:")
        logger.warning("   1. SDK will attempt to connect to EventHub partitions via AMQP")
        logger.warning(
            "   2. If authenticated identity lacks 'Azure Event Hubs Data Receiver' role:"
        )
        logger.warning("      - Connection will FAIL with 'Unauthorized' or 'Forbidden' error")
        logger.warning("      - You'll see detailed error message with fix instructions")
        logger.warning("   3. If connection succeeds:")
        logger.warning("      - The authenticated identity HAS the required role")
        logger.warning("      - Check logs above to see WHICH identity was used")
        logger.warning("")
        logger.warning("   NOTE: System may use Managed Identity (not your CLI user)!")
        logger.warning(
            "   Look for MSI endpoint in logs: http://169.254.169.254/metadata/identity/..."
        )
        logger.warning("")

    def _log_receive_error_header(self, *, error_type: str, receive_error: Exception) -> None:
        logger.error("")
        logger.error("=" * 70)
        logger.error("❌ EVENTHUB RECEIVE ERROR")
        logger.error("=" * 70)
        logger.error(f"Error type: {error_type}")
        logger.error(f"Error message: {receive_error}")
        logger.error("")

    def _log_azure_credential_error_guidance(
        self,
        *,
        error_type: str,
        receive_error: Exception,
    ) -> None:
        logger.error("🔐 AZURE CREDENTIAL ERROR DETECTED!")
        logger.error("")
        logger.error("This error occurs during EventProcessor's partition ownership claiming.")
        logger.error("The SDK repeatedly tries to claim partition ownership and re-authenticates.")
        logger.error("")
        logger.error("Possible causes:")
        logger.error("  1. Azure CLI token expired and refresh failed")
        logger.error(
            "  2. Too many rapid authentication requests overwhelming the credential chain"
        )
        logger.error("  3. Network interruption preventing token refresh")
        logger.error("  4. Azure CLI process busy or locked")
        logger.error("")
        logger.error("Recommended solutions:")
        logger.error("  1. Run: az login --use-device-code (refresh auth)")
        logger.error("  2. Set environment variables for faster auth (skip credential chain):")
        logger.error("     export AZURE_TENANT_ID='your-tenant-id'")
        logger.error("     export AZURE_CLIENT_ID='your-client-id'")
        logger.error("     export AZURE_CLIENT_SECRET='your-secret'")
        logger.error(
            "  3. Check: az account get-access-token --resource https://eventhubs.azure.net"
        )
        logger.error("")
        logfire.error(
            "Azure credential error during EventHub receive",
            error_type=error_type,
            error_message=str(receive_error),
            namespace=self.eventhub_config.namespace,
            eventhub=self.eventhub_config.name,
        )

    def _raise_rbac_permission_error(
        self,
        *,
        error_type: str,
        receive_error: Exception,
    ) -> NoReturn:
        logger.error("")
        logger.error("❌ RBAC PERMISSION ERROR DETECTED!")
        logger.error(f"   Error type: {error_type}")
        logger.error(f"   Error message: {receive_error}")
        logger.error("")
        logger.error("🔐 The authenticated identity lacks required Azure RBAC permissions!")
        logger.error("")
        logger.error("Required Role:")
        logger.error("  • 'Azure Event Hubs Data Receiver' - to read EventHub messages")
        logger.error("")
        logger.error("How to Fix:")
        logger.error("  1. Check which authentication method was used (see logs above)")
        logger.error("  2. If using Managed Identity, assign the role to the managed identity")
        logger.error("  3. If using Azure CLI, assign the role to your Azure CLI user")
        logger.error("  4. Go to Azure Portal → Event Hubs")
        logger.error(f"  5. Find namespace: {self.eventhub_config.namespace}")
        logger.error(f"  6. Click on Event Hub: {self.eventhub_config.name}")
        logger.error("  7. Go to 'Access Control (IAM)' → 'Add role assignment'")
        logger.error("  8. Select role: 'Azure Event Hubs Data Receiver'")
        logger.error("  9. Assign to the correct identity (MSI or user)")
        logger.error("")
        raise RuntimeError(
            "Missing Azure RBAC permission: 'Azure Event Hubs Data Receiver' role required for the authenticated identity"
        ) from receive_error

    def _handle_receive_error(self, receive_error: Exception) -> bool:
        """Handle EventHub receive errors.

        Returns:
            True if the error was handled (and should not be re-raised).
            False if the caller should re-raise the original exception.

        Note: This preserves the current behavior, including swallowing certain
        credential-chain errors after printing guidance.
        """

        error_msg = str(receive_error).lower()
        error_type = type(receive_error).__name__

        self._log_receive_error_header(error_type=error_type, receive_error=receive_error)

        if any(
            keyword in error_msg
            for keyword in [
                "credential",
                "failed to invoke azure cli",
                "failed to retrieve a token",
                "defaultazurecredential failed",
                "authentication unavailable",
            ]
        ):
            self._log_azure_credential_error_guidance(
                error_type=error_type,
                receive_error=receive_error,
            )
            return True

        if (
            any(
                keyword in error_msg
                for keyword in [
                    "unauthorized",
                    "not authorized",
                    "authenticationerror",
                    "permission",
                    "access denied",
                    "forbidden",
                ]
            )
            or "401" in error_msg
            or "403" in error_msg
        ):
            self._raise_rbac_permission_error(
                error_type=error_type,
                receive_error=receive_error,
            )

        return False

    async def start(self) -> None:
        """
        Start the EventHub consumer.

        Note: This method intentionally does NOT use @logfire.instrument() or with logfire.span()
        because it's a long-running async method that runs indefinitely with many await points.
        Both approaches cause async context issues where spans cannot properly detach.

        Instead, we:
        1. Use logfire.info() for discrete events (startup, shutdown)
        2. Use spans in shorter-lived methods (_on_event, _process_batch)

        See: https://logfire.pydantic.dev/docs/reference/advanced/generators/
        """
        if self.running:
            logger.warning("Consumer is already running")
            return

        # Log the start event with key parameters
        logfire.info(
            "Starting EventHub consumer",
            eventhub_namespace=self.eventhub_config.namespace,
            eventhub_name=self.eventhub_config.name,
            consumer_group=self.eventhub_config.consumer_group,
            batch_size=self.batch_size,
            batch_timeout=self.batch_timeout_seconds,
        )

        self._log_startup_summary()
        self._log_competing_consumer_guidance()

        try:
            # Initialize checkpoint manager
            # IMPORTANT: target_db/schema/table should be the DESTINATION DATA TABLE
            # (where events are ingested), NOT the control table location.
            # The control table location is determined by the utility functions internally.
            logger.info("📍 Initializing checkpoint manager...")
            self.checkpoint_manager = SnowflakeCheckpointManager(
                eventhub_namespace=self.eventhub_config.namespace,
                eventhub_name=self.eventhub_config.name,
                target_db=self.target_db,  # Destination data table
                target_schema=self.target_schema,  # Destination data schema
                target_table=self.target_table,  # Destination data table name
                snowflake_config=self.snowflake_config,
                control_db=self.control_db,  # Where checkpoints are stored
                control_schema=self.control_schema,
                control_table=self.control_table,
            )

            # Create Azure SDK-compatible checkpoint store
            logger.info("🔐 Creating checkpoint store...")
            checkpoint_store = SnowflakeCheckpointStore(self.checkpoint_manager)

            # Log existing checkpoints for debugging (SDK will load them automatically)
            logger.info("🔍 Checking for existing checkpoints...")
            partition_checkpoints = await self.checkpoint_manager.get_last_checkpoint()

            has_checkpoints = bool(partition_checkpoints)

            # Determine starting position based on checkpoint existence
            # NOTE: keep the check inline so type-checkers can narrow `partition_checkpoints`.
            if has_checkpoints:
                assert partition_checkpoints is not None
                logger.info(f"✅ Found checkpoints in Snowflake: {partition_checkpoints}")
                logger.info(
                    "   SDK will automatically resume from NEXT sequence after these checkpoints:"
                )
                for partition_id, seq_num in partition_checkpoints.items():
                    logger.info(
                        f"      Partition {partition_id}: last processed seq={seq_num}, will resume from seq={seq_num + 1}"
                    )
                self.stats["last_checkpoint"] = partition_checkpoints
            else:
                starting_pos = self.eventhub_config.starting_position_on_no_checkpoint
                self._log_no_checkpoint_behavior(starting_pos)

            # Create EventHub client WITH checkpoint store
            # The SDK will automatically load checkpoints from the store
            # DO NOT pass starting_position - let the SDK handle it via checkpoint_store
            logger.info("🔌 Creating EventHub client with checkpoint store...")

            logger.info("🔗 Creating EventHub client...")

            # Use connection string if configured, otherwise use credential
            if self.eventhub_config.connection_string:
                logger.info("🔑 Using connection string authentication")
                self.client = EventHubConsumerClient.from_connection_string(
                    conn_str=self.eventhub_config.connection_string,
                    consumer_group=self.eventhub_config.consumer_group,
                    eventhub_name=self.eventhub_config.name,
                    checkpoint_store=checkpoint_store,
                )
                logger.info("✅ EventHub client created with connection string")
            else:
                logger.info("🔐 Initializing Azure CLI credential (Event Hub scope)...")
                try:
                    credential, expiry = await build_eventhub_cli_credential(
                        namespace=self.eventhub_config.namespace,
                        logger=logger,
                    )
                    self.credential = credential
                    logfire.info(
                        "Azure credential test successful",
                        token_expiry=expiry,
                        namespace=self.eventhub_config.namespace,
                    )
                except Exception as cred_error:
                    logger.error(f"❌ Failed to create or test AzureCliCredential: {cred_error}")
                    logger.error("")
                    logger.error("🔧 TROUBLESHOOTING - Azure Authentication:")
                    logger.error("")
                    logger.error("For LOCAL DEVELOPMENT:")
                    logger.error("  Option 1: Use Azure CLI (Recommended)")
                    logger.error("    1. Run: az login")
                    logger.error("    2. Run: az account show")
                    logger.error(
                        "    3. Ensure your account has 'Azure Event Hubs Data Receiver' role"
                    )
                    logger.error("")
                    logger.error("  Option 2: Use Connection String (Quick Testing)")
                    logger.error("    1. Set EVENTHUBNAME_{N}_CONNECTION_STRING in .env")
                    logger.error(
                        "    2. Connection string authentication will be used automatically"
                    )
                    logger.error("")
                    logger.error("For PRODUCTION/CLOUD:")
                    logger.error("  - Use Managed Identity (automatically available in Azure)")
                    logger.error(
                        "  - Ensure the identity has 'Azure Event Hubs Data Receiver' role"
                    )
                    logger.error("")
                    logger.error(
                        "Check Azure SDK logs above to see which credential methods were attempted."
                    )
                    logger.error("")
                    logfire.error(
                        "AzureCliCredential creation failed",
                        error=str(cred_error),
                        namespace=self.eventhub_config.namespace,
                    )
                    raise

                logger.info(
                    "   💡 Run 'python -m src check-credentials' to see which credential will be used"
                )
                logger.info("")

                logger.info("Creating EventHub client with credential-based authentication...")
                self.client = EventHubConsumerClient(
                    fully_qualified_namespace=self.eventhub_config.namespace,
                    eventhub_name=self.eventhub_config.name,
                    credential=self.credential,
                    consumer_group=self.eventhub_config.consumer_group,
                    checkpoint_store=checkpoint_store,
                )
                logger.info("✅ EventHub client created with credential-based auth")

            logger.info("✅ EventHub client configured - SDK will use checkpoint store to resume")

            self._log_rbac_permission_validation_notice()

            # Initialize batch
            self.current_batch = self._new_batch(reason="startup")

            # Start capture writer (if enabled)
            await self._start_capture_writer()

            self.running = True
            self.stats["start_time"] = datetime.now(UTC)

            # Start batch timeout task
            timeout_task = asyncio.create_task(self._batch_timeout_handler())
            self.tasks.add(timeout_task)
            logger.info("⏰ Batch timeout handler started")

            # Start receiving messages
            # SDK will automatically load checkpoints from checkpoint_store
            # and resume from the correct position for each partition
            logger.info("👂 Starting to receive messages from EventHub...")

            if has_checkpoints:
                logger.info(
                    "⏳ SDK loading checkpoints from store and resuming from saved positions..."
                )
            else:
                starting_pos = self.eventhub_config.starting_position_on_no_checkpoint
                logger.info(f"⏳ SDK starting from {starting_pos} (no checkpoints found)...")

            try:
                # Determine starting position based on checkpoint existence
                receive_kwargs: dict[str, Any] = {
                    "on_event": self._on_event,
                }

                # Only set starting_position if NO checkpoints exist
                # When checkpoints exist, SDK will use them automatically
                if not has_checkpoints:
                    starting_pos = self.eventhub_config.starting_position_on_no_checkpoint
                    receive_kwargs["starting_position"] = starting_pos
                    # CRITICAL: Set starting_position_inclusive=False to avoid reprocessing
                    # Without this, SDK may re-process messages from checkpoints
                    receive_kwargs["starting_position_inclusive"] = False
                    logger.info(
                        f"📍 Setting starting_position='{starting_pos}' (exclusive, configured value)"
                    )

                await self.client.receive(**receive_kwargs)
            except Exception as receive_error:
                handled = self._handle_receive_error(receive_error)
                if not handled:
                    raise

        except Exception as e:
            logger.error(f"❌ Failed to start EventHub consumer: {e}", exc_info=True)
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the EventHub consumer gracefully."""
        try:
            logger.info("🛑 Stopping EventHub consumer gracefully...")
            self.running = False

            # Cancel timeout handler task FIRST to prevent it from processing/clearing the batch
            for task in self.tasks:
                if not task.done():
                    task.cancel()

            # Wait for timeout handler to fully stop
            if self.tasks:
                logger.info(f"⏳ Waiting for {len(self.tasks)} tasks to complete...")
                await asyncio.gather(*self.tasks, return_exceptions=True)
                logger.info("✅ All tasks completed")
            self.tasks.clear()
        except Exception as e:
            logger.error(f"❌ Error during initial shutdown steps: {e}", exc_info=True)
            raise

        try:
            # Process any remaining messages in current batch BEFORE closing the client
            batch_exists = self.current_batch is not None
            message_count = 0
            if self.current_batch:
                batch_has_messages = self.current_batch.messages
                message_count = len(batch_has_messages) if batch_has_messages else 0
            logger.info(
                f"🔍 Shutdown check: batch_exists={batch_exists}, message_count={message_count}"
            )

            if self.current_batch and self.current_batch.messages:
                logger.info(f"📦 Processing {message_count} remaining messages before shutdown...")
                try:
                    if not self.current_batch.ready_reason:
                        self.current_batch.ready_reason = "shutdown"
                    await self._process_batch(self.current_batch)
                    logger.info(
                        f"✅ {message_count} remaining messages processed and checkpoints updated"
                    )
                except Exception as e:
                    logger.error(f"❌ Error processing remaining batch: {e}", exc_info=True)
            else:
                logger.info("✅ No remaining messages to process")
        except Exception as e:
            logger.error(f"❌ Error during batch processing: {e}", exc_info=True)
            # Don't raise - continue with cleanup

        try:
            # Close EventHub client to stop receiving new messages
            if self.client:
                logger.info("🔌 Closing EventHub client...")
                try:
                    await self.client.close()
                except Exception as e:
                    logger.warning(f"Error closing EventHub client: {e}")
                self.client = None

            # Close Azure credential to prevent resource leak
            if self.credential:
                logger.info("🔐 Closing Azure credential...")
                try:
                    await self.credential.close()
                except Exception as e:
                    logger.warning(f"Error closing credential: {e}")
                self.credential = None

            # Close checkpoint manager
            if self.checkpoint_manager:
                logger.info("🗄️ Closing checkpoint manager...")
                self.checkpoint_manager.close()
                self.checkpoint_manager = None

            # Flush captured messages (if enabled)
            await self._stop_capture_writer()

            logger.info("✅ EventHub consumer stopped gracefully")
        except Exception as e:
            logger.error(f"❌ Error during final cleanup: {e}", exc_info=True)
            # Don't raise - we're shutting down anyway

    async def _on_event(self, partition_context: PartitionContext, event: EventData | None) -> None:
        """
        Handle incoming EventHub message.

        Note: This method does NOT create Logfire spans for individual events
        because it would be too expensive (thousands of events per second).
        Instead, we log to console and only create spans for batch operations.
        """
        if not self.running:
            logger.debug("Consumer not running, ignoring event")
            return

        if event is None:
            logger.debug(f"Received None event on partition {partition_context.partition_id}")
            return

        try:
            # Log FIRST message received on each partition to verify checkpoint resumption
            if partition_context.partition_id not in self._first_message_logged:
                logger.warning(
                    f"🎯 FIRST MESSAGE on partition {partition_context.partition_id}: "
                    f"offset={event.offset}, sequence={event.sequence_number}, "
                    f"enqueued_time={event.enqueued_time}"
                )
                # Log first message to Logfire for monitoring checkpoint resumption
                logfire.info(
                    "First message on partition",
                    partition_id=partition_context.partition_id,
                    offset=event.offset,
                    sequence_number=event.sequence_number,
                )
                self._first_message_logged.add(partition_context.partition_id)

            logger.debug(
                f"📨 Received event on partition {partition_context.partition_id}, "
                f"offset: {event.offset}, sequence: {event.sequence_number}, "
                f"enqueued_time: {event.enqueued_time}"
            )

            # Ensure sequence_number is not None
            if event.sequence_number is None:
                logger.warning(
                    f"Received event with None sequence_number on partition {partition_context.partition_id}"
                )
                return

            # Optional capture of the raw message (as received)
            self._enqueue_capture(partition_context.partition_id, event)

            # Create message wrapper
            message = EventHubMessage(
                event_data=event,
                partition_id=partition_context.partition_id,
                sequence_number=event.sequence_number,
                eventhub_namespace=self.eventhub_config.namespace,
                eventhub_name=self.eventhub_config.name,
                consumer_group=self.eventhub_config.consumer_group,
            )

            # Store partition_context with message for later checkpoint update
            message.partition_context = partition_context

            self.stats["messages_received"] += 1

            logger.debug(
                f"✅ Message {self.stats['messages_received']} added to batch. "
                f"Current batch size: {len(self.current_batch.messages) if self.current_batch else 0}"
            )

            # Add to current batch
            if self.current_batch and self.current_batch.add_message(message):
                # Batch is ready - process it
                logger.info(
                    f"🔄 Batch ready for processing ({len(self.current_batch.messages)} messages)"
                )
                await self._process_batch(self.current_batch)
                self.current_batch = self._new_batch(reason="after_process")

        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)
            logfire.error(
                "Error processing event",
                error=str(e),
                partition_id=partition_context.partition_id,
            )

    async def _batch_timeout_handler(self) -> None:
        """Handle batch timeout - process partial batches."""
        logger.info("⏰ Batch timeout handler started")
        check_count = 0
        while self.running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                check_count += 1

                if check_count % 6 == 0:  # Log every minute
                    logger.info(
                        f"⏰ Timeout check #{check_count}: "
                        f"Batch has {len(self.current_batch.messages) if self.current_batch else 0} messages, "
                        f"age: {time.time() - self.current_batch.created_at if self.current_batch else 0:.1f}s"
                    )

                if (
                    self.current_batch
                    and self.current_batch.messages
                    and self.current_batch.is_ready()
                ):
                    # Process timed-out batch
                    logger.info(
                        f"⏱️ Batch timeout reached! Processing {len(self.current_batch.messages)} messages"
                    )
                    self.current_batch.mark_timeout_ready()
                    await self._process_batch(self.current_batch)
                    self.current_batch = self._new_batch(reason="timeout_reset")

            except asyncio.CancelledError:
                logger.info("⏰ Batch timeout handler cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in batch timeout handler: {e}", exc_info=True)

    def _new_batch(self, reason: str) -> MessageBatch:
        """Create a new message batch and log its creation."""
        self._batch_counter += 1
        batch = MessageBatch(
            max_size=self.batch_size,
            max_wait_seconds=self.batch_timeout_seconds,
        )
        logfire.info(
            "eventhub.batch.created",
            batch_id=batch.batch_id,
            reason=reason,
            batch_index=self._batch_counter,
            batch_max_size=self.batch_size,
            batch_timeout_seconds=self.batch_timeout_seconds,
        )
        return batch

    async def _process_batch(self, batch: MessageBatch) -> None:
        """Process a batch of messages."""
        batch_age_seconds = time.time() - batch.created_at
        with logfire.span(
            "eventhub.batch",
            batch_id=batch.batch_id,
            ready_reason=batch.ready_reason or "unknown",
            batch_size=len(batch.messages),
            partitions=list(batch.last_sequence_by_partition.keys()),
            eventhub_name=self.eventhub_config.name,
            batch_age_seconds=batch_age_seconds,
        ) as span:
            if not batch.messages:
                logger.debug("No messages in batch to process")
                span.set_attribute("empty_batch", True)
                return

            logger.info(f"🔄 Processing batch of {len(batch.messages)} messages")
            logger.info(f"   Partitions in batch: {list(batch.last_sequence_by_partition.keys())}")
            logger.info(f"   Sequence ranges: {batch.last_sequence_by_partition}")

            try:
                # Call the message processor
                logger.info("📤 Calling message processor...")
                success = self.message_processor(batch.messages)

                span.set_attribute("processor_success", success)

                if success:
                    logger.info("✅ Message processor returned success")

                    # CRITICAL: Update EventHub SDK checkpoints for each partition
                    # This tells EventHub where we've successfully processed up to
                    # Group messages by partition to get the last message per partition
                    last_message_by_partition: dict[str, EventHubMessage] = {}
                    for message in batch.messages:
                        partition_id = message.partition_id
                        if (
                            partition_id not in last_message_by_partition
                            or message.sequence_number
                            > last_message_by_partition[partition_id].sequence_number
                        ):
                            last_message_by_partition[partition_id] = message

                    # Update checkpoint for each partition through the SDK
                    logger.info(
                        f"🔖 Updating EventHub SDK checkpoints for {len(last_message_by_partition)} partitions..."
                    )
                    checkpoints_updated = 0
                    max_checkpoint_attempts = 3
                    for partition_id, last_message in last_message_by_partition.items():
                        if last_message.partition_context:
                            for attempt in range(1, max_checkpoint_attempts + 1):
                                try:
                                    await last_message.partition_context.update_checkpoint(
                                        last_message.event_data
                                    )
                                    checkpoints_updated += 1
                                    logger.info(
                                        f"✅ Updated SDK checkpoint for partition {partition_id}: "
                                        f"offset={last_message.event_data.offset}, sequence={last_message.sequence_number}"
                                    )
                                    break
                                except Exception as e:
                                    logger.error(
                                        "❌ Failed to update SDK checkpoint for partition %s (attempt %s/%s): %s",
                                        partition_id,
                                        attempt,
                                        max_checkpoint_attempts,
                                        e,
                                        exc_info=True,
                                    )
                                    logfire.error(
                                        "Checkpoint update failed",
                                        partition_id=partition_id,
                                        attempt=attempt,
                                        max_attempts=max_checkpoint_attempts,
                                        error=str(e),
                                    )
                                    if attempt < max_checkpoint_attempts:
                                        await asyncio.sleep(2 ** (attempt - 1))
                        else:
                            logger.warning(
                                f"⚠️ No partition_context for partition {partition_id}, cannot update SDK checkpoint"
                            )

                    span.set_attribute("checkpoints_updated", checkpoints_updated)
                    span.set_attribute("partitions_count", len(last_message_by_partition))

                    # NOTE: Checkpoints are already saved via SDK's CheckpointStore.update_checkpoint() above
                    # No need for backup save - it would use sequence numbers instead of offsets

                    self.stats["batches_processed"] += 1
                    span.set_attribute("total_batches_processed", self.stats["batches_processed"])
                    # Store checkpoint data in stats for monitoring (sequence numbers for display)
                    partition_checkpoints = batch.get_checkpoint_data()
                    self.stats["last_checkpoint"] = partition_checkpoints.copy()

                    logger.info(
                        f"✅ Batch processed successfully! Total batches: {self.stats['batches_processed']}, "
                        f"Total messages: {self.stats['messages_received']}"
                    )
                else:
                    logger.error("❌ Message processor returned failure")
                    logfire.error(
                        "Message processor returned failure", batch_size=len(batch.messages)
                    )
                    span.set_attribute("processor_failure", True)

            except Exception as e:
                logger.error(f"❌ Error processing batch: {e}", exc_info=True)
                logfire.error(
                    "Batch processing error", error=str(e), batch_size=len(batch.messages)
                )
                span.set_attribute("error", str(e))

    def get_stats(self) -> dict[str, Any]:
        """Get consumer statistics."""
        stats = self.stats.copy()
        if stats["start_time"] is not None:
            runtime = datetime.now(UTC) - stats["start_time"]
            stats["runtime_seconds"] = runtime.total_seconds()
            if stats["messages_received"] > 0:
                stats["messages_per_second"] = stats["messages_received"] / runtime.total_seconds()
        return stats


async def create_eventhub_consumer(
    eventhub_config: EventHubConfig,
    target_db: str,
    target_schema: str,
    target_table: str,
    message_processor: Callable[[list[EventHubMessage]], bool],
    snowflake_config: SnowflakeConnectionConfig | None = None,
    batch_size: int = 1000,
    batch_timeout_seconds: int = 300,
    control_db: str | None = None,
    control_schema: str | None = None,
    control_table: str | None = None,
) -> EventHubAsyncConsumer:
    """Factory function to create an EventHub consumer."""
    return EventHubAsyncConsumer(
        eventhub_config=eventhub_config,
        target_db=target_db,
        target_schema=target_schema,
        target_table=target_table,
        message_processor=message_processor,
        snowflake_config=snowflake_config,
        batch_size=batch_size,
        batch_timeout_seconds=batch_timeout_seconds,
        control_db=control_db,
        control_schema=control_schema,
        control_table=control_table,
    )


# Example usage
if __name__ == "__main__":
    import logging

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Example message processor
    def example_processor(messages: list[EventHubMessage]) -> bool:
        """Example message processor - just log the messages."""
        logger.info(f"Processing {len(messages)} messages")
        for msg in messages:
            logger.info(f"  Partition {msg.partition_id}: {msg.body[:100]}...")
        return True

    # Example usage would require actual EventHub configuration
    print("EventHub consumer module loaded successfully")
    print("Use create_eventhub_consumer() to create consumer instances")
