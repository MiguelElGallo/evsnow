"""Mapping models between Event Hub sources and Snowflake targets."""

import re

from pydantic import BaseModel, Field, field_validator


class EventHubSnowflakeMapping(BaseModel):
    """Mapping between Event Hub and Snowflake configurations."""

    event_hub_key: str = Field(..., description="Event Hub configuration key")
    snowflake_key: str = Field(..., description="Snowflake configuration key")

    channel_name_pattern: str = "{event_hub}-{env}-{region}-{client_id}"

    @field_validator("event_hub_key", "snowflake_key")
    @classmethod
    def validate_mapping_keys(cls, v: str) -> str:
        """Validate mapping keys format."""
        if not re.match(r"^[A-Z0-9_]+$", v):
            raise ValueError(f"Mapping key must be uppercase with underscores: {v}")
        return v
