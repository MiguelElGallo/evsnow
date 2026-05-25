"""
Configuration module for EvSnow pipeline.

This module provides configuration management for Azure Event Hubs and Snowflake
integration, incorporating best practices for both services. It uses Pydantic Settings
for validation and python-dotenv for environment variable loading.

Best Practices Incorporated:
- Azure Event Hubs: Connection string management, client configuration
- Snowflake: High-performance batch ingestion, connection management, metadata tracking
"""

import os
import re
import socket
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings

from .config_file import (
    default_config_file_if_present,
    load_toml_config,
    toml_config_to_env,
    toml_config_to_kwargs,
)

# Public config models live in smaller modules under `utils.config_models`.
# This module keeps the stable public import surface: `from utils.config import ...`.
from .config_models import (
    EventHubConfig,
    EventHubSnowflakeMapping,
    LogfireConfig,
    PostgresConnectionConfig,
    SmartRetryConfig,
    SnowflakeConfig,
    SnowflakeConnectionConfig,
)

TOP_LEVEL_ENV_FIELDS = {
    "ENVIRONMENT": "environment",
    "REGION": "region",
    "EVSNOW_CLIENT_ID": "client_id",
    "TARGET_DB": "target_db",
    "TARGET_SCHEMA": "target_schema",
    "TARGET_TABLE": "target_table",
    "CONTROL_TABLE_BACKEND": "control_table_backend",
    "CONTROL_OWNERSHIP_MODE": "control_ownership_mode",
    "USE_HYBRID_TABLE": "use_hybrid_table",
    "MAX_CONCURRENT_CHANNELS": "max_concurrent_channels",
    "INGESTION_TIMEOUT_SECONDS": "ingestion_timeout_seconds",
    "MAX_CONCURRENT_MAPPINGS": "max_concurrent_mappings",
    "HEALTH_CHECK_INTERVAL_SECONDS": "health_check_interval_seconds",
    "MAX_PIPELINE_RESTART_ATTEMPTS": "max_pipeline_restart_attempts",
    "PIPELINE_RESTART_DELAY_SECONDS": "pipeline_restart_delay_seconds",
    "ENABLE_DETAILED_LOGGING": "enable_detailed_logging",
    "LOG_MESSAGE_SAMPLES": "log_message_samples",
    "METRICS_COLLECTION_ENABLED": "metrics_collection_enabled",
}

SNOWFLAKE_CONNECTION_FIELDS = {
    "account",
    "user",
    "private_key_file",
    "private_key_password",
    "warehouse",
    "database",
    "schema_name",
    "role",
    "pipe_name",
}

SNOWFLAKE_TARGET_SETTING_PATTERN = re.compile(
    r"^SNOWFLAKE_(\d+)_(DATABASE|SCHEMA|TABLE|BATCH|MAX_RETRY_ATTEMPTS|"
    r"RETRY_DELAY_SECONDS|CONNECTION_TIMEOUT_SECONDS|"
    r"CHANNEL_STATUS_INTERVAL_SECONDS|CLIENT_REFRESH_INTERVAL_SECONDS)$"
)


class EvSnowConfig(BaseSettings):
    """
    Main configuration class for EvSnow pipeline.

    This class manages the complete configuration including Event Hubs, Snowflake destinations,
    and their mappings. It incorporates best practices from both Azure Event Hubs and
    Snowflake documentation.

    Environment Variables:
    - EVENTHUB_NAMESPACE: Event Hub namespace
    - EVSNOW_CLIENT_ID: Optional stable client identifier for channel naming
    - EVENTHUBNAME_{N}: Event Hub names (where N is a number)
    - SNOWFLAKE_{N}_DATABASE: Snowflake database name
    - SNOWFLAKE_{N}_SCHEMA: Snowflake schema name
    - SNOWFLAKE_{N}_TABLE: Snowflake table name
    - EVENTHUBNAME_{N} = SNOWFLAKE_{M}: Mapping configuration
    """

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
        "extra": "ignore",
        "populate_by_name": True,
    }

    # Core Event Hub settings
    eventhub_namespace: str = Field(..., description="Event Hub namespace")

    # Environment and deployment settings
    environment: str = Field("development", description="Deployment environment")
    region: str = Field("default", description="Deployment region")
    client_id: str = Field(
        default_factory=socket.gethostname,
        validation_alias="EVSNOW_CLIENT_ID",
        description="Stable identifier for this EvSnow instance (used in channel naming).",
    )

    # Performance settings
    max_concurrent_channels: int = Field(50, description="Maximum concurrent channels")
    ingestion_timeout_seconds: int = Field(300, description="Ingestion timeout in seconds")

    # Pipeline performance tuning
    max_concurrent_mappings: int = Field(
        default=10, description="Maximum concurrent mapping processors"
    )
    health_check_interval_seconds: int = Field(
        default=60, description="Health check interval in seconds"
    )

    # Error handling and retry policies
    max_pipeline_restart_attempts: int = Field(
        default=3, description="Maximum pipeline restart attempts on failure"
    )
    pipeline_restart_delay_seconds: int = Field(
        default=30, description="Delay between pipeline restart attempts"
    )

    # Monitoring and logging
    enable_detailed_logging: bool = Field(
        default=False, description="Enable detailed debug logging"
    )
    log_message_samples: bool = Field(
        default=False, description="Log sample messages for debugging"
    )
    metrics_collection_enabled: bool = Field(default=True, description="Enable metrics collection")

    # Debugging / diagnostics
    capture_messages: bool = Field(
        default=False,
        description="Capture each raw Event Hub message to disk as JSON (messages/f_{timestamp}.json)",
    )
    capture_messages_dir: str = Field(
        default="messages",
        description="Directory (relative to repo-root/CWD) to store captured messages",
    )

    # Observability
    logfire: LogfireConfig = Field(
        default_factory=LogfireConfig,
        description="Logfire observability configuration",
    )

    # Snowflake connection settings (shared across all targets)
    snowflake_connection: SnowflakeConnectionConfig | None = Field(
        default=None,
        description="Snowflake connection configuration",
    )

    # Control table configuration (for checkpoints/watermarks)
    target_db: str = Field(
        default="CONTROL",
        description="Control database for checkpoint tables",
    )
    target_schema: str = Field(
        default="PUBLIC",
        description="Control schema for checkpoint tables",
    )
    target_table: str = Field(
        default="INGESTION_STATUS",
        description="Control table name for checkpoints",
    )
    use_hybrid_table: bool = Field(
        default=False,
        validation_alias="USE_HYBRID_TABLE",
        description="Use Hybrid Table for control table (requires paid Snowflake account)",
    )
    control_ownership_mode: Literal["durable", "local_single_consumer_smoke"] = Field(
        default="durable",
        validation_alias="CONTROL_OWNERSHIP_MODE",
        description=(
            "Partition ownership mode. durable uses the configured control backend; "
            "local_single_consumer_smoke keeps ownership in memory and persists only checkpoints."
        ),
    )
    control_table_backend: Literal["snowflake", "postgres"] = Field(
        default="snowflake",
        validation_alias="CONTROL_TABLE_BACKEND",
        description="Backend for control/checkpoint table (snowflake or postgres).",
    )
    control_postgres: PostgresConnectionConfig | None = Field(
        default=None,
        description="Postgres control table connection configuration",
    )

    # Configuration storage
    event_hubs: dict[str, EventHubConfig] = Field(default_factory=dict)
    snowflake_configs: dict[str, SnowflakeConfig] = Field(default_factory=dict)
    mappings: list[EventHubSnowflakeMapping] = Field(default_factory=list)

    def __init__(self, **kwargs):
        """Initialize configuration with dynamic parsing of environment variables."""
        source_env = kwargs.pop("_source_env", None)
        super().__init__(**kwargs)
        self._source_env = dict(source_env) if source_env is not None else dict(os.environ)
        if any(key.startswith("LOGFIRE_") for key in self._source_env):
            self.logfire = LogfireConfig(**_prefixed_model_kwargs(self._source_env, "LOGFIRE_"))
        self._parse_dynamic_config(self._source_env)
        self._configure_snowflake_connection()
        self._configure_control_backend()

    def _configure_snowflake_connection(self) -> None:
        """Build Snowflake auth/session config, deriving DB/schema from TOML when needed."""
        env_snowflake = _prefixed_model_kwargs(
            self._source_env,
            "SNOWFLAKE_",
            allowed_fields=SNOWFLAKE_CONNECTION_FIELDS,
        )
        if self.snowflake_connection is None:
            try:
                if env_snowflake:
                    connection_kwargs = self._with_derived_snowflake_context(env_snowflake)
                    self.snowflake_connection = SnowflakeConnectionConfig(**connection_kwargs)
                else:
                    self.snowflake_connection = None
            except Exception:
                # Snowflake connection is optional, may not be configured
                self.snowflake_connection = None
        elif env_snowflake:
            merged_snowflake = self.snowflake_connection.model_dump()
            merged_snowflake.update(env_snowflake)
            merged_snowflake = self._with_derived_snowflake_context(merged_snowflake)
            self.snowflake_connection = SnowflakeConnectionConfig(**merged_snowflake)

    def _with_derived_snowflake_context(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        if kwargs.get("database") and kwargs.get("schema_name"):
            return kwargs

        derived = self._derive_snowflake_connection_context()
        if derived is None:
            return kwargs

        database, schema_name = derived
        kwargs = dict(kwargs)
        kwargs.setdefault("database", database)
        kwargs.setdefault("schema_name", schema_name)
        return kwargs

    def _derive_snowflake_connection_context(self) -> tuple[str, str] | None:
        mapped_targets = []
        if self.mappings:
            for mapping in self.mappings:
                target = self.snowflake_configs.get(mapping.snowflake_key)
                if target is not None:
                    mapped_targets.append(target)
        else:
            mapped_targets = list(self.snowflake_configs.values())

        unique_contexts = {
            (target.database, target.schema_name)
            for target in mapped_targets
            if target.database and target.schema_name
        }
        if len(unique_contexts) == 1:
            return next(iter(unique_contexts))
        return None

    def _configure_control_backend(self) -> None:
        if self.control_table_backend == "postgres":
            if self.control_postgres is None:
                self.control_postgres = PostgresConnectionConfig(
                    **_prefixed_model_kwargs(self._source_env, "CONTROL_PG_")
                )
            elif any(key.startswith("CONTROL_PG_") for key in self._source_env):
                merged_postgres = self.control_postgres.model_dump()
                merged_postgres.update(_prefixed_model_kwargs(self._source_env, "CONTROL_PG_"))
                self.control_postgres = PostgresConnectionConfig(**merged_postgres)
            self.target_db = self._normalize_postgres_identifier(self.target_db)
            self.target_schema = self._normalize_postgres_identifier(self.target_schema)
            self.target_table = self._normalize_postgres_identifier(self.target_table)

    def _parse_dynamic_config(self, env_vars: Mapping[str, str] | None = None):
        """Parse dynamic Event Hub and Snowflake configurations from environment variables."""
        env_vars = dict(env_vars or os.environ)

        # Parse Event Hub configurations
        event_hub_pattern = re.compile(r"^EVENTHUBNAME_(\d+)$")
        event_hub_option_pattern = re.compile(r"^EVENTHUBNAME_(\d+)_(.+)$")
        event_hub_option_fields = {
            "CONSUMER_GROUP": "consumer_group",
            "CONNECTION_STRING": "connection_string",
            "MAX_BATCH_SIZE": "max_batch_size",
            "MAX_WAIT_TIME": "max_wait_time",
            "PREFETCH_COUNT": "prefetch_count",
            "RETRY_TOTAL": "retry_total",
            "RETRY_BACKOFF_FACTOR": "retry_backoff_factor",
            "RETRY_BACKOFF_MAX": "retry_backoff_max",
            "RETRY_MODE": "retry_mode",
            "LOAD_BALANCING_INTERVAL": "load_balancing_interval",
            "PARTITION_OWNERSHIP_EXPIRATION_INTERVAL": "partition_ownership_expiration_interval",
            "LOAD_BALANCING_STRATEGY": "load_balancing_strategy",
            "TRACK_LAST_ENQUEUED_EVENT_PROPERTIES": "track_last_enqueued_event_properties",
            "CREDENTIAL_MODE": "credential_mode",
            "MANAGED_IDENTITY_CLIENT_ID": "managed_identity_client_id",
            "STARTING_POSITION_ON_NO_CHECKPOINT": "starting_position_on_no_checkpoint",
            "USE_CONNECTION_STRING": "use_connection_string",
            "CHECKPOINT_INTERVAL_SECONDS": "checkpoint_interval_seconds",
            "MAX_MESSAGE_BATCH_SIZE": "max_message_batch_size",
            "BATCH_TIMEOUT_SECONDS": "batch_timeout_seconds",
        }
        event_hub_global_env = {
            "AZURE_EVENTHUB_CONNECTION_STRING": "connection_string",
            "EVENTHUB_CREDENTIAL_MODE": "credential_mode",
            "EVENTHUB_MANAGED_IDENTITY_CLIENT_ID": "managed_identity_client_id",
            "EVENTHUB_MAX_WAIT_TIME": "max_wait_time",
            "EVENTHUB_PREFETCH_COUNT": "prefetch_count",
            "EVENTHUB_RETRY_TOTAL": "retry_total",
            "EVENTHUB_RETRY_BACKOFF_FACTOR": "retry_backoff_factor",
            "EVENTHUB_RETRY_BACKOFF_MAX": "retry_backoff_max",
            "EVENTHUB_RETRY_MODE": "retry_mode",
            "EVENTHUB_LOAD_BALANCING_INTERVAL": "load_balancing_interval",
            "EVENTHUB_PARTITION_OWNERSHIP_EXPIRATION_INTERVAL": (
                "partition_ownership_expiration_interval"
            ),
            "EVENTHUB_LOAD_BALANCING_STRATEGY": "load_balancing_strategy",
            "EVENTHUB_TRACK_LAST_ENQUEUED_EVENT_PROPERTIES": (
                "track_last_enqueued_event_properties"
            ),
            "EVENTHUB_CHECKPOINT_INTERVAL_SECONDS": "checkpoint_interval_seconds",
            "EVENTHUB_MAX_MESSAGE_BATCH_SIZE": "max_message_batch_size",
            "EVENTHUB_BATCH_TIMEOUT_SECONDS": "batch_timeout_seconds",
        }

        # First collect all Event Hub numbers and their consumer groups
        event_hub_data: dict[str, dict[str, Any]] = {}
        event_hub_defaults = {
            field_name: env_vars[env_name]
            for env_name, field_name in event_hub_global_env.items()
            if env_vars.get(env_name) is not None
        }

        for key, value in env_vars.items():
            match = event_hub_pattern.match(key)
            if match:
                hub_num = match.group(1)
                if hub_num not in event_hub_data:
                    event_hub_data[hub_num] = {}
                event_hub_data[hub_num]["name"] = value

            match = event_hub_option_pattern.match(key)
            if match:
                hub_num = match.group(1)
                option_name = match.group(2)
                field_name = event_hub_option_fields.get(option_name)
                if field_name is None:
                    continue
                if hub_num not in event_hub_data:
                    event_hub_data[hub_num] = {}
                event_hub_data[hub_num][field_name] = value

        # Create EventHubConfig instances with consumer groups
        for hub_num, data in event_hub_data.items():
            if "name" in data:  # Only create if we have a name
                if "consumer_group" not in data:
                    raise ValueError(
                        f"EVENTHUBNAME_{hub_num}_CONSUMER_GROUP is required for EVENTHUBNAME_{hub_num}"
                    )
                event_hub_kwargs = {
                    **event_hub_defaults,
                    **data,
                    "namespace": self.eventhub_namespace,
                }
                hub_key = f"EVENTHUBNAME_{hub_num}"
                self.event_hubs[hub_key] = EventHubConfig(
                    **event_hub_kwargs,
                )

        # Parse Snowflake configurations
        snowflake_keys: dict[str, dict[str, str]] = {}
        for key, value in env_vars.items():
            match = SNOWFLAKE_TARGET_SETTING_PATTERN.match(key)
            if match:
                sf_num = match.group(1)
                setting = match.group(2).lower()
                if sf_num not in snowflake_keys:
                    snowflake_keys[sf_num] = {}
                snowflake_keys[sf_num][setting] = value

        # Create Snowflake configurations
        for sf_num, settings in snowflake_keys.items():
            if all(key in settings for key in ["database", "schema", "table"]):
                sf_key = f"SNOWFLAKE_{sf_num}"
                self.snowflake_configs[sf_key] = SnowflakeConfig(
                    database=settings["database"],
                    schema_name=settings["schema"],
                    table_name=settings["table"],
                    batch_size=int(settings.get("batch", "1000")),
                    max_retry_attempts=int(settings.get("max_retry_attempts", "3")),
                    retry_delay_seconds=int(settings.get("retry_delay_seconds", "5")),
                    connection_timeout_seconds=int(
                        settings.get("connection_timeout_seconds", "30")
                    ),
                    channel_status_interval_seconds=int(
                        settings.get("channel_status_interval_seconds", "60")
                    ),
                    client_refresh_interval_seconds=int(
                        settings.get("client_refresh_interval_seconds", "0")
                    ),
                )

        # Parse mappings - look for explicit mapping lines in env file
        # This is a simplified approach - in practice you might want more sophisticated parsing
        self._parse_mappings(env_vars)

    def _parse_mappings(self, env_vars: dict[str, str]):
        """Parse mapping configurations from environment variables."""
        # Look for mapping patterns in comments or specific variables
        # For now, auto-map based on numbers: EVENTHUBNAME_1 -> SNOWFLAKE_1
        event_hub_nums = set()
        snowflake_nums = set()

        for key in env_vars:
            if key.startswith("EVENTHUBNAME_"):
                num = key.split("_")[1]
                event_hub_nums.add(num)
            elif key.startswith("SNOWFLAKE_") and key.endswith("_DATABASE"):
                num = key.split("_")[1]
                snowflake_nums.add(num)

        # Create mappings for matching numbers
        existing_pairs = {(m.event_hub_key, m.snowflake_key) for m in self.mappings}
        for num in event_hub_nums:
            if num in snowflake_nums:
                eh_key = f"EVENTHUBNAME_{num}"
                sf_key = f"SNOWFLAKE_{num}"
                if (
                    eh_key in self.event_hubs
                    and sf_key in self.snowflake_configs
                    and (eh_key, sf_key) not in existing_pairs
                ):
                    self.mappings.append(
                        EventHubSnowflakeMapping(
                            event_hub_key=eh_key,
                            snowflake_key=sf_key,
                            channel_name_pattern="{event_hub}-{env}-{region}-{client_id}",
                        )
                    )
                    existing_pairs.add((eh_key, sf_key))

    @field_validator("eventhub_namespace")
    @classmethod
    def validate_eventhub_namespace(cls, v: str) -> str:
        """Validate Event Hub namespace format."""
        if not v.endswith(".servicebus.windows.net"):
            raise ValueError("Event Hub namespace must end with .servicebus.windows.net")
        return v

    @field_validator("client_id")
    @classmethod
    def normalize_client_id(cls, v: str) -> str:
        """Normalize client identifier for safe channel naming."""
        cleaned = re.sub(r"[^A-Za-z0-9_-]", "-", v.strip())
        cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-_")
        return cleaned or "client"

    @field_validator("control_table_backend", mode="before")
    @classmethod
    def normalize_control_table_backend(cls, v: Any) -> Any:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @field_validator("control_ownership_mode", mode="before")
    @classmethod
    def normalize_control_ownership_mode(cls, v: Any) -> Any:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @model_validator(mode="after")
    def validate_mappings_exist(self):
        """Validate that all mappings reference existing configurations."""
        if (
            self.control_ownership_mode == "local_single_consumer_smoke"
            and self.control_table_backend != "snowflake"
        ):
            raise ValueError(
                "CONTROL_OWNERSHIP_MODE=local_single_consumer_smoke is only valid with "
                "CONTROL_TABLE_BACKEND=snowflake"
            )

        for mapping in self.mappings:
            if mapping.event_hub_key not in self.event_hubs:
                raise ValueError(
                    f"Mapping references non-existent Event Hub: {mapping.event_hub_key}"
                )
            if mapping.snowflake_key not in self.snowflake_configs:
                raise ValueError(
                    f"Mapping references non-existent Snowflake config: {mapping.snowflake_key}"
                )

        return self

    def get_event_hub_config(self, key: str) -> EventHubConfig | None:
        """Get Event Hub configuration by key."""
        return self.event_hubs.get(key)

    def get_snowflake_config(self, key: str) -> SnowflakeConfig | None:
        """Get Snowflake configuration by key."""
        return self.snowflake_configs.get(key)

    def get_mapping_for_event_hub(self, event_hub_key: str) -> EventHubSnowflakeMapping | None:
        """Get mapping configuration for an Event Hub."""
        for mapping in self.mappings:
            if mapping.event_hub_key == event_hub_key:
                return mapping
        return None

    def generate_channel_name(self, event_hub_key: str, client_id: str) -> str:
        """
        Generate deterministic channel name for tracking and troubleshooting.

        Uses pattern: source-env-region-client-id for identification.
        """
        mapping = self.get_mapping_for_event_hub(event_hub_key)
        if not mapping:
            raise ValueError(f"No mapping found for Event Hub: {event_hub_key}")

        event_hub_config = self.get_event_hub_config(event_hub_key)
        if not event_hub_config:
            raise ValueError(f"No Event Hub config found: {event_hub_key}")

        values = {
            "event_hub": self._sanitize_channel_segment(event_hub_config.name),
            "env": self._sanitize_channel_segment(self.environment),
            "region": self._sanitize_channel_segment(self.region),
            "client_id": self._sanitize_channel_segment(client_id),
        }

        try:
            pattern = str(mapping.channel_name_pattern)
            return pattern.format(**values)
        except KeyError as exc:
            raise ValueError(
                "channel_name_pattern must use {event_hub}, {env}, {region}, {client_id}"
            ) from exc

    @staticmethod
    def _sanitize_channel_segment(value: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9_-]", "-", value.strip())
        cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-_")
        return cleaned or "unknown"

    @staticmethod
    def _normalize_postgres_identifier(value: str) -> str:
        cleaned = value.strip()
        if cleaned.startswith('"') and cleaned.endswith('"') and len(cleaned) > 1:
            return cleaned[1:-1]
        return cleaned.lower()

    def validate_configuration(self) -> dict[str, Any]:
        """
        Validate the complete configuration and return validation summary.

        Returns a dictionary with validation results including any warnings or issues.
        """
        warnings: list[str] = []
        errors: list[str] = []

        results = {
            "valid": True,
            "event_hubs_count": len(self.event_hubs),
            "snowflake_configs_count": len(self.snowflake_configs),
            "mappings_count": len(self.mappings),
            "warnings": warnings,
            "errors": errors,
        }

        # Check for unmapped configurations
        mapped_event_hubs = {m.event_hub_key for m in self.mappings}
        mapped_snowflake = {m.snowflake_key for m in self.mappings}

        unmapped_event_hubs = set(self.event_hubs.keys()) - mapped_event_hubs
        unmapped_snowflake = set(self.snowflake_configs.keys()) - mapped_snowflake

        if unmapped_event_hubs:
            warnings.append(f"Unmapped Event Hubs: {list(unmapped_event_hubs)}")

        if unmapped_snowflake:
            warnings.append(f"Unmapped Snowflake configs: {list(unmapped_snowflake)}")

        return results


def load_config(env_file: str | None = None, config_file: str | None = None) -> EvSnowConfig:
    """
    Load configuration from environment file.

    Args:
        env_file: Optional path to .env file. Defaults to .env in current directory.
        config_file: Optional path to structured TOML config.

    Returns:
        Configured EvSnowConfig instance.

    Raises:
        ValidationError: If configuration is invalid.
        FileNotFoundError: If specified env file doesn't exist.
    """
    resolved_config_file = Path(config_file) if config_file else default_config_file_if_present()

    toml_env: dict[str, str] = {}
    config_kwargs: dict[str, Any] = {}
    if resolved_config_file is not None:
        file_config = load_toml_config(resolved_config_file)
        toml_env = toml_config_to_env(file_config)
        config_kwargs = toml_config_to_kwargs(file_config)

    if env_file:
        env_path = Path(env_file)
        if not env_path.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        # Load environment variables from specified file
        try:
            from dotenv import load_dotenv

            load_dotenv(env_path, override=True)
        except ImportError as e:
            raise ImportError(
                "python-dotenv is required for loading .env files. Install it with: pip install python-dotenv"
            ) from e

    effective_env = {**toml_env, **dict(os.environ)}

    # Get the eventhub_namespace from merged configuration sources.
    eventhub_namespace = effective_env.get("EVENTHUB_NAMESPACE")
    if not eventhub_namespace:
        raise ValueError("EVENTHUB_NAMESPACE environment variable is required")

    config_kwargs.update(
        {
            "eventhub_namespace": eventhub_namespace,
            "_source_env": effective_env,
        }
    )
    for env_name, field_name in TOP_LEVEL_ENV_FIELDS.items():
        if env_name in effective_env:
            config_kwargs[field_name] = effective_env[env_name]

    return EvSnowConfig(
        **config_kwargs,
    )


def _prefixed_model_kwargs(
    env_vars: Mapping[str, str],
    prefix: str,
    allowed_fields: set[str] | None = None,
) -> dict[str, str]:
    kwargs: dict[str, str] = {}
    for key, value in env_vars.items():
        if key.startswith(prefix):
            field_name = key.removeprefix(prefix).lower()
            if field_name == "schema":
                field_name = "schema_name"
            if allowed_fields is not None and field_name not in allowed_fields:
                continue
            kwargs[field_name] = value
    return kwargs


__all__ = [
    "EvSnowConfig",
    # Facade exports for existing import sites
    "EventHubConfig",
    "EventHubSnowflakeMapping",
    "LogfireConfig",
    "PostgresConnectionConfig",
    "SmartRetryConfig",
    "SnowflakeConfig",
    "SnowflakeConnectionConfig",
    "load_config",
]


# Example usage and testing utilities
if __name__ == "__main__":
    try:
        config = load_config()
        validation_results = config.validate_configuration()

        print("Configuration loaded successfully!")
        print(f"Event Hubs: {validation_results['event_hubs_count']}")
        print(f"Snowflake Configs: {validation_results['snowflake_configs_count']}")
        print(f"Mappings: {validation_results['mappings_count']}")

        if validation_results["warnings"]:
            print("\nWarnings:")
            for warning in validation_results["warnings"]:
                print(f"  - {warning}")

    except Exception as e:
        print(f"Configuration error: {e}")
