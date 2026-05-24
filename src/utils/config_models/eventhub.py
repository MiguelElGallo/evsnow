"""Azure Event Hubs configuration models."""

import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class EventHubConfig(BaseModel):
    """Configuration for a single Event Hub."""

    name: str = Field(..., description="Event Hub name")
    namespace: str = Field(..., description="Event Hub namespace")
    connection_string: str | None = None
    consumer_group: str = Field(..., description="Consumer group name (required)")

    max_batch_size: int = 1000
    max_wait_time: int = 60
    prefetch_count: int = 300
    retry_total: int = 3
    retry_backoff_factor: float = 0.8
    retry_backoff_max: float = 120
    retry_mode: Literal["exponential", "fixed"] = "exponential"
    load_balancing_interval: float = 30
    partition_ownership_expiration_interval: float | None = None
    load_balancing_strategy: Literal["greedy", "balanced"] = "greedy"
    track_last_enqueued_event_properties: bool = False
    credential_mode: Literal["default", "azure_cli"] = Field(
        default="default",
        description=(
            "Credential strategy for Entra ID auth. Use default for production-capable "
            "DefaultAzureCredential; azure_cli is an explicit local-development opt-in."
        ),
    )
    managed_identity_client_id: str | None = Field(
        default=None,
        description="Optional user-assigned managed identity client ID for DefaultAzureCredential.",
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
            "'@latest' (only new)"
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

    @field_validator("credential_mode", "retry_mode", "load_balancing_strategy", mode="before")
    @classmethod
    def normalize_literal_fields(cls, v: str) -> str:
        """Normalize case for enum-like config loaded from environment variables."""
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @field_validator("managed_identity_client_id", mode="before")
    @classmethod
    def normalize_optional_string(cls, v: str | None) -> str | None:
        """Treat blank optional strings from env files as unset."""
        if isinstance(v, str):
            cleaned = v.strip()
            return cleaned or None
        return v

    @field_validator(
        "max_batch_size",
        "prefetch_count",
        "retry_total",
        "retry_backoff_factor",
        "retry_backoff_max",
        "load_balancing_interval",
        "partition_ownership_expiration_interval",
    )
    @classmethod
    def validate_positive_numbers(cls, v: float | int | None) -> float | int | None:
        """Validate SDK tuning values before handing them to the Azure client."""
        if v is not None and v <= 0:
            raise ValueError("Event Hub numeric SDK settings must be greater than 0")
        return v

    @field_validator("max_wait_time")
    @classmethod
    def validate_max_wait_time(cls, v: int) -> int:
        """Validate receive wait timeout.

        Azure EventHubConsumerClient.receive documents max_wait_time=None or 0
        as "wait until an event is received"; positive values call on_event
        with None on idle intervals. We allow 0 to keep that SDK mode available.
        https://learn.microsoft.com/python/api/azure-eventhub/azure.eventhub.eventhubconsumerclient
        """
        if v < 0:
            raise ValueError("max_wait_time must be greater than or equal to 0")
        return v

    @model_validator(mode="after")
    def validate_ownership_expiration_interval(self) -> "EventHubConfig":
        """Validate Event Hubs load-balancing timing as one safety unit.

        Azure's SDK default for partition_ownership_expiration_interval is six
        times load_balancing_interval. Keeping custom values at least that wide
        avoids expiring ownership between claim renewals under competing consumers.
        https://learn.microsoft.com/python/api/azure-eventhub/azure.eventhub.eventhubconsumerclient
        """
        if self.partition_ownership_expiration_interval is not None:
            minimum_expiration = self.load_balancing_interval * 6
            if self.partition_ownership_expiration_interval < minimum_expiration:
                raise ValueError(
                    "partition_ownership_expiration_interval must be at least "
                    "6 * load_balancing_interval"
                )
        return self

    @field_validator("starting_position_on_no_checkpoint")
    @classmethod
    def validate_starting_position(cls, v: str) -> str:
        """Validate starting position format."""
        valid_positions = ["-1", "@latest"]
        if v not in valid_positions:
            raise ValueError(
                f"Invalid starting_position: {v}. Must be one of: {', '.join(valid_positions)}"
            )
        return v
