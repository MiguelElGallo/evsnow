"""Structured TOML configuration support for EvSnow."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .config_models import (
    EventHubConfig,
    EventHubSnowflakeMapping,
    LogfireConfig,
    PostgresConnectionConfig,
    SnowflakeConfig,
    SnowflakeConnectionConfig,
)

DEFAULT_CONFIG_FILE = Path("config/evsnow.toml")


class ControlTableFileConfig(BaseModel):
    """Control table settings stored in TOML."""

    model_config = ConfigDict(extra="forbid")

    target_db: str = "CONTROL"
    target_schema: str = "PUBLIC"
    target_table: str = "INGESTION_STATUS"
    backend: str = "snowflake"
    ownership_mode: str = "durable"
    use_hybrid_table: bool = False
    postgres: PostgresConnectionConfig | None = None


class EventHubDefaultsFileConfig(BaseModel):
    """Optional defaults applied to every Event Hub defined in TOML."""

    model_config = ConfigDict(extra="forbid")

    connection_string: str | None = None
    max_batch_size: int | None = Field(default=None, gt=0)
    max_wait_time: int | None = Field(default=None, ge=0)
    prefetch_count: int | None = Field(default=None, gt=0)
    retry_total: int | None = Field(default=None, gt=0)
    retry_backoff_factor: float | None = Field(default=None, gt=0)
    retry_backoff_max: float | None = Field(default=None, gt=0)
    retry_mode: Literal["exponential", "fixed"] | None = None
    load_balancing_interval: float | None = Field(default=None, gt=0)
    partition_ownership_expiration_interval: float | None = Field(default=None, gt=0)
    load_balancing_strategy: Literal["greedy", "balanced"] | None = None
    track_last_enqueued_event_properties: bool | None = None
    credential_mode: Literal["default", "azure_cli"] | None = None
    managed_identity_client_id: str | None = None
    use_connection_string: bool | None = None
    checkpoint_interval_seconds: int | None = Field(default=None, gt=0)
    max_message_batch_size: int | None = Field(default=None, gt=0)
    batch_timeout_seconds: int | None = Field(default=None, gt=0)
    starting_position_on_no_checkpoint: Literal["-1", "@latest"] | None = None


class EvSnowFileConfig(BaseModel):
    """Non-secret structured configuration loaded from TOML."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    eventhub_namespace: str
    environment: str = "development"
    region: str = "default"
    client_id: str | None = None

    max_concurrent_channels: int | None = None
    ingestion_timeout_seconds: int | None = None
    max_concurrent_mappings: int | None = None
    health_check_interval_seconds: int | None = None
    max_pipeline_restart_attempts: int | None = None
    pipeline_restart_delay_seconds: int | None = None
    enable_detailed_logging: bool | None = None
    log_message_samples: bool | None = None
    metrics_collection_enabled: bool | None = None

    control: ControlTableFileConfig = Field(default_factory=ControlTableFileConfig)
    eventhub_defaults: EventHubDefaultsFileConfig = Field(
        default_factory=EventHubDefaultsFileConfig
    )
    event_hubs: dict[str, EventHubConfig] = Field(default_factory=dict)
    snowflake_configs: dict[str, SnowflakeConfig] = Field(default_factory=dict)
    mappings: list[EventHubSnowflakeMapping] = Field(default_factory=list)

    snowflake_connection: SnowflakeConnectionConfig | None = None
    logfire: LogfireConfig | None = None


def load_toml_config(config_file: str | Path) -> EvSnowFileConfig:
    """Load and validate a TOML configuration file."""
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    with config_path.open("rb") as handle:
        data = tomllib.load(handle)
    return EvSnowFileConfig.model_validate(data)


def toml_config_to_kwargs(file_config: EvSnowFileConfig) -> dict[str, Any]:
    """Convert TOML-facing config into EvSnowConfig constructor kwargs."""
    kwargs: dict[str, Any] = {
        "eventhub_namespace": file_config.eventhub_namespace,
        "environment": file_config.environment,
        "region": file_config.region,
        "target_db": file_config.control.target_db,
        "target_schema": file_config.control.target_schema,
        "target_table": file_config.control.target_table,
        "control_table_backend": file_config.control.backend,
        "control_ownership_mode": file_config.control.ownership_mode,
        "use_hybrid_table": file_config.control.use_hybrid_table,
        "event_hubs": file_config.event_hubs,
        "snowflake_configs": file_config.snowflake_configs,
        "mappings": file_config.mappings,
    }

    optional_fields = [
        "client_id",
        "max_concurrent_channels",
        "ingestion_timeout_seconds",
        "max_concurrent_mappings",
        "health_check_interval_seconds",
        "max_pipeline_restart_attempts",
        "pipeline_restart_delay_seconds",
        "enable_detailed_logging",
        "log_message_samples",
        "metrics_collection_enabled",
    ]
    for field_name in optional_fields:
        value = getattr(file_config, field_name)
        if value is not None:
            kwargs[field_name] = value

    return kwargs


def toml_config_to_env(file_config: EvSnowFileConfig) -> dict[str, str]:
    """Flatten TOML config into legacy env-style keys for override merging."""
    env: dict[str, str] = {
        "EVENTHUB_NAMESPACE": file_config.eventhub_namespace,
        "ENVIRONMENT": file_config.environment,
        "REGION": file_config.region,
        "TARGET_DB": file_config.control.target_db,
        "TARGET_SCHEMA": file_config.control.target_schema,
        "TARGET_TABLE": file_config.control.target_table,
        "CONTROL_TABLE_BACKEND": file_config.control.backend,
        "CONTROL_OWNERSHIP_MODE": file_config.control.ownership_mode,
        "USE_HYBRID_TABLE": _stringify(file_config.control.use_hybrid_table),
    }
    if file_config.client_id:
        env["EVSNOW_CLIENT_ID"] = file_config.client_id

    for field_name, value in file_config.eventhub_defaults.model_dump().items():
        if value is not None:
            env[_eventhub_global_env_key(field_name)] = _stringify(value)

    for key, hub in file_config.event_hubs.items():
        env[key] = hub.name
        for field_name, value in hub.model_dump(
            exclude={"name"},
            exclude_unset=True,
        ).items():
            if value is not None:
                env[f"{key}_{_env_option_name(field_name)}"] = _stringify(value)

    for key, target in file_config.snowflake_configs.items():
        env[f"{key}_DATABASE"] = target.database
        env[f"{key}_SCHEMA"] = target.schema_name
        env[f"{key}_TABLE"] = target.table_name
        env[f"{key}_BATCH"] = _stringify(target.batch_size)
        env[f"{key}_MAX_RETRY_ATTEMPTS"] = _stringify(target.max_retry_attempts)
        env[f"{key}_RETRY_DELAY_SECONDS"] = _stringify(target.retry_delay_seconds)
        env[f"{key}_CONNECTION_TIMEOUT_SECONDS"] = _stringify(target.connection_timeout_seconds)
        env[f"{key}_CHANNEL_STATUS_INTERVAL_SECONDS"] = _stringify(
            target.channel_status_interval_seconds
        )
        env[f"{key}_CLIENT_REFRESH_INTERVAL_SECONDS"] = _stringify(
            target.client_refresh_interval_seconds
        )

    if file_config.snowflake_connection is not None:
        _merge_prefixed_model(env, "SNOWFLAKE_", file_config.snowflake_connection)
    if file_config.control.postgres is not None:
        _merge_prefixed_model(env, "CONTROL_PG_", file_config.control.postgres)
    if file_config.logfire is not None:
        _merge_prefixed_model(env, "LOGFIRE_", file_config.logfire)
    return env


def default_config_file_if_present() -> Path | None:
    """Return the default TOML config path only when it exists."""
    return DEFAULT_CONFIG_FILE if DEFAULT_CONFIG_FILE.exists() else None


def _merge_prefixed_model(env: dict[str, str], prefix: str, model: BaseModel) -> None:
    for field_name, value in model.model_dump().items():
        if value is not None:
            env[f"{prefix}{_env_option_name(field_name)}"] = _stringify(value)


def _eventhub_global_env_key(field_name: str) -> str:
    if field_name == "connection_string":
        return "AZURE_EVENTHUB_CONNECTION_STRING"
    return f"EVENTHUB_{_env_option_name(field_name)}"


def _env_option_name(field_name: str) -> str:
    if field_name == "schema_name":
        return "SCHEMA_NAME"
    if field_name == "table_name":
        return "TABLE"
    return field_name.upper()


def _stringify(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
