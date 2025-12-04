"""
Event Hub message and batching helpers.

Provides lightweight wrappers around EventHub SDK types so ingestion code can
remain focused on pipeline orchestration.
"""

import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from azure.eventhub import EventData
from azure.eventhub.aio import PartitionContext

logger = logging.getLogger(__name__)

__all__ = [
    "BytesEncoder",
    "EventHubMessage",
    "MessageBatch",
    "_convert_bytes_to_str",
]


class BytesEncoder(json.JSONEncoder):
    """Custom JSON encoder that converts bytes to strings."""

    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return super().default(obj)


def _convert_bytes_to_str(obj: Any) -> Any:
    """Recursively convert bytes to strings in nested structures."""
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, dict):
        return {_convert_bytes_to_str(k): _convert_bytes_to_str(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return type(obj)(_convert_bytes_to_str(item) for item in obj)
    return obj


class EventHubMessage:
    """Wrapper for EventHub message with additional metadata."""

    def __init__(self, event_data: EventData, partition_id: str, sequence_number: int):
        self.event_data = event_data
        self.partition_id = partition_id
        self.sequence_number = sequence_number
        self.body = event_data.body_as_str()
        self.enqueued_time = event_data.enqueued_time
        self.properties = event_data.properties
        self.system_properties = event_data.system_properties
        self.partition_context: PartitionContext | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert message to dictionary for Snowflake ingestion."""
        properties_json = None
        if self.properties:
            clean_props = _convert_bytes_to_str(self.properties)
            properties_json = json.dumps(clean_props, cls=BytesEncoder)

        system_props_json = None
        if self.system_properties:
            clean_sys_props = _convert_bytes_to_str(dict(self.system_properties))
            system_props_json = json.dumps(clean_sys_props, cls=BytesEncoder)

        result = {
            "event_body": self.body,
            "partition_id": self.partition_id,
            "sequence_number": self.sequence_number,
            "enqueued_time": self.enqueued_time.isoformat() if self.enqueued_time else None,
            "properties": properties_json,
            "system_properties": system_props_json,
            "ingestion_timestamp": datetime.now(UTC).isoformat(),
        }

        for key, value in result.items():
            if isinstance(value, bytes):
                value_preview = value[:100] if len(value) > 100 else value
                logger.error(
                    "FOUND BYTES in to_dict() result: key=%s, value_type=%s, value=%r",
                    key,
                    type(value),
                    value_preview,
                )

        return result


class MessageBatch:
    """Container for batched EventHub messages."""

    def __init__(self, max_size: int = 1000, max_wait_seconds: int = 300):
        self.messages: list[EventHubMessage] = []
        self.max_size = max_size
        self.max_wait_seconds = max_wait_seconds
        self.created_at = time.time()
        self.last_sequence_by_partition: dict[str, int] = {}

    def add_message(self, message: EventHubMessage) -> bool:
        """Add message to batch and return readiness state."""
        self.messages.append(message)
        self.last_sequence_by_partition[message.partition_id] = message.sequence_number
        return self.is_ready()

    def is_ready(self) -> bool:
        """Return True if batch size or timeout threshold reached."""
        return (
            len(self.messages) >= self.max_size
            or (time.time() - self.created_at) >= self.max_wait_seconds
        )

    def get_checkpoint_data(self) -> dict[str, int]:
        """Get checkpoint data (highest sequence number per partition)."""
        return self.last_sequence_by_partition.copy()

    def to_dict_list(self) -> list[dict[str, Any]]:
        """Convert all messages to list of dictionaries."""
        return [msg.to_dict() for msg in self.messages]
