"""Logfire observability configuration."""

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class LogfireConfig(BaseSettings):
    """Logfire observability configuration."""

    model_config = {
        "env_prefix": "LOGFIRE_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
    }

    enabled: bool = Field(default=False, description="Enable Logfire observability and tracing")

    token: str | None = Field(default=None, description="Logfire API token for cloud logging")

    service_name: str = Field(
        default="evsnow",
        description="Service name for Logfire identification",
    )

    environment: str = Field(
        default="development",
        description="Environment tag (development, staging, production)",
    )

    send_to_logfire: bool = Field(
        default=True,
        description="Send logs to Logfire cloud (requires token)",
    )

    console_logging: bool = Field(
        default=True,
        description="Keep Rich console logging alongside Logfire",
    )

    log_level: str = Field(
        default="INFO",
        description="Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Valid levels: {', '.join(valid_levels)}")
        return v_upper

    @model_validator(mode="after")
    def validate_token_when_enabled(self):
        """Ensure token is provided when Logfire is enabled and sending to cloud."""
        if self.enabled and self.send_to_logfire and not self.token:
            raise ValueError(
                "LOGFIRE_TOKEN is required when LOGFIRE_ENABLED=true and LOGFIRE_SEND_TO_LOGFIRE=true"
            )
        return self
