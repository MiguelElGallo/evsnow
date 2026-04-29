"""Azure Event Hubs configuration models."""

import re

from pydantic import BaseModel, Field, field_validator


class EventHubConfig(BaseModel):
    """Configuration for a single Event Hub."""

    name: str = Field(..., description="Event Hub name")
    namespace: str = Field(..., description="Event Hub namespace")
    connection_string: str | None = None
    consumer_group: str = Field(..., description="Consumer group name (required)")

    max_batch_size: int = 1000
    max_wait_time: int = 60
    prefetch_count: int = 300

    use_connection_string: bool = Field(
        default=False,
        description="Use connection string instead of Azure CLI credential authentication",
    )

    checkpoint_interval_seconds: int = Field(
        default=300, description="Checkpoint interval (seconds)"
    )
    max_message_batch_size: int = Field(
        default=1000,
        description="Maximum messages per processing batch",
    )
    batch_timeout_seconds: int = Field(
        default=300,
        description="Maximum time to wait for batch completion",
    )

    starting_position_on_no_checkpoint: str = Field(
        default="-1",
        description=(
            "Starting position when no checkpoints exist. Options: '-1' (beginning), "
            "'@latest' (only new), '0' (offset zero)"
        ),
    )

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, v: str) -> str:
        """Validate Event Hub namespace format."""
        if not v.endswith(".servicebus.windows.net"):
            raise ValueError("Event Hub namespace must end with .servicebus.windows.net")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate Event Hub name format."""
        if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9\-._])*[a-zA-Z0-9]$", v):
            raise ValueError("Event Hub name contains invalid characters")
        return v

    @field_validator("consumer_group")
    @classmethod
    def validate_consumer_group(cls, v: str) -> str:
        """Validate consumer group format."""
        if not v.strip():
            raise ValueError("Consumer group cannot be empty")
        return v

    @field_validator("starting_position_on_no_checkpoint")
    @classmethod
    def validate_starting_position(cls, v: str) -> str:
        """Validate starting position format."""
        valid_positions = ["-1", "@latest", "0"]
        if v not in valid_positions:
            raise ValueError(
                f"Invalid starting_position: {v}. Must be one of: {', '.join(valid_positions)}"
            )
        return v
