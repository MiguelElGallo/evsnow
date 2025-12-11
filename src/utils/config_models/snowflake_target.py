"""Snowflake destination configuration models."""

from pydantic import BaseModel, Field, field_validator


class SnowflakeConfig(BaseModel):
    """Configuration for a single Snowflake destination using Streaming API."""

    database: str = Field(..., description="Snowflake database name")
    schema_name: str = Field(..., description="Snowflake schema name")
    table_name: str = Field(..., description="Snowflake table name")

    batch_size: int = Field(default=1000, description="Number of records per batch insert")

    max_retry_attempts: int = Field(default=3, description="Maximum retry attempts")
    retry_delay_seconds: int = Field(default=5, description="Delay between retry attempts")
    connection_timeout_seconds: int = Field(default=30, description="Connection timeout in seconds")

    @field_validator("database", "schema_name", "table_name")
    @classmethod
    def validate_snowflake_identifiers(cls, v: str) -> str:
        """Validate Snowflake identifiers."""
        if not v.strip():
            raise ValueError(f"Snowflake identifier cannot be empty: {v}")
        v_clean = v.strip()
        if not v_clean.replace("_", "").replace("$", "").replace("-", "").isalnum():
            raise ValueError(f"Invalid Snowflake identifier: {v}")
        return v_clean
