"""Snowflake connection configuration.

Separated from `utils.config` to keep the main configuration file smaller.
"""

import os

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class SnowflakeConnectionConfig(BaseSettings):
    """Snowflake connection configuration with JWT authentication."""

    model_config = {
        "env_prefix": "SNOWFLAKE_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
        "populate_by_name": True,
    }

    account: str = Field(..., description="Snowflake account identifier")
    user: str = Field(..., description="Snowflake username")
    private_key_file: str = Field(..., description="Path to private key file")
    private_key_password: str | None = Field(
        default=None,
        description="Private key password (None for unencrypted keys)",
    )
    warehouse: str = Field(..., description="Snowflake warehouse name")
    database: str = Field(..., description="Snowflake database name")
    schema_name: str = Field(..., description="Snowflake schema name")

    role: str | None = Field(default=None, description="Snowflake role")

    pipe_name: str = Field(
        ...,
        description="PIPE object name for high-performance SDK (e.g., EVENTS_TABLE_PIPE)",
    )

    @field_validator("private_key_file")
    @classmethod
    def validate_private_key_file_exists(cls, v: str) -> str:
        """Validate that the private key file exists and is readable."""
        from pathlib import Path

        key_path = Path(v).expanduser().resolve()
        if not key_path.exists():
            raise ValueError(f"Private key file not found: {v}")
        if not key_path.is_file():
            raise ValueError(f"Private key path is not a file: {v}")
        if not os.access(key_path, os.R_OK):
            raise ValueError(f"Private key file is not readable: {v}")
        return str(key_path)

    @field_validator("account")
    @classmethod
    def validate_account_format(cls, v: str) -> str:
        """Validate Snowflake account identifier format."""
        if not v.strip():
            raise ValueError("Account identifier cannot be empty")
        if not all(c.isalnum() or c in "-._" for c in v):
            raise ValueError("Account identifier contains invalid characters")
        return v.strip()

    @field_validator("user", "warehouse", "database", "schema_name")
    @classmethod
    def validate_snowflake_identifiers(cls, v: str) -> str:
        """Validate Snowflake identifiers."""
        if not v.strip():
            raise ValueError("Snowflake identifier cannot be empty")
        v_clean = v.strip()
        if not v_clean.replace("_", "").replace("$", "").replace("-", "").isalnum():
            raise ValueError(f"Invalid Snowflake identifier: {v}")
        return v_clean
