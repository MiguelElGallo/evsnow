"""Postgres connection configuration for control table storage."""

from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class PostgresConnectionConfig(BaseSettings):
    """Postgres connection configuration for checkpoint/control table."""

    model_config = {
        "env_prefix": "CONTROL_PG_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
        "populate_by_name": True,
    }

    host: str = Field(..., description="Postgres host for control table")
    port: int = Field(default=5432, description="Postgres port for control table")
    user: str = Field(..., description="Postgres user for control table")
    password: str | None = Field(
        default=None,
        description="Postgres password for control table (required for password auth)",
    )
    sslmode: str = Field(
        default="require",
        description="Postgres SSL mode (e.g., require, verify-ca, verify-full)",
    )
    auth_mode: Literal["password", "azure_token"] = Field(
        default="password",
        description="Authentication mode for Postgres control table",
    )

    @field_validator("auth_mode", mode="before")
    @classmethod
    def normalize_auth_mode(cls, v: str) -> str:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @field_validator("host", "user")
    @classmethod
    def validate_required_text(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Value cannot be empty")
        return v.strip()

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if v < 1 or v > 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v

    @field_validator("sslmode")
    @classmethod
    def validate_sslmode(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("SSLMODE cannot be empty")
        return v.strip()

    @model_validator(mode="after")
    def validate_auth_mode(self):
        if self.auth_mode == "password" and not self.password:
            raise ValueError("CONTROL_PG_PASSWORD is required when CONTROL_PG_AUTH_MODE=password")
        return self
