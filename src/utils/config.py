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
from typing import Any

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings

# Public config models live in smaller modules under `utils.config_models`.
# This module keeps the stable public import surface: `from utils.config import ...`.
from .config_models import (
    EventHubConfig,
    EventHubSnowflakeMapping,
    LogfireConfig,
    SmartRetryConfig,
    SnowflakeConfig,
    SnowflakeConnectionConfig,
)


class EvSnowConfig(BaseSettings):
    """
    Main configuration class for EvSnow pipeline.

    This class manages the complete configuration including Event Hubs, Snowflake destinations,
    and their mappings. It incorporates best practices from both Azure Event Hubs and
    Snowflake documentation.

    Environment Variables:
    - EVENTHUB_NAMESPACE: Event Hub namespace
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
    }

    # Core Event Hub settings
    eventhub_namespace: str = Field(..., description="Event Hub namespace")

    # Environment and deployment settings
    environment: str = Field("development", description="Deployment environment")
    region: str = Field("default", description="Deployment region")

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

    # Configuration storage
    event_hubs: dict[str, EventHubConfig] = Field(default_factory=dict)
    snowflake_configs: dict[str, SnowflakeConfig] = Field(default_factory=dict)
    mappings: list[EventHubSnowflakeMapping] = Field(default_factory=list)

    def __init__(self, **kwargs):
        """Initialize configuration with dynamic parsing of environment variables."""
        super().__init__(**kwargs)
        # Try to load Snowflake connection configuration
        # This is optional - if not all required fields are present, skip it
        # Only try to load from environment if not already provided
        if self.snowflake_connection is None:
            try:
                # Check if any Snowflake connection vars are set
                if any(
                    key.startswith("SNOWFLAKE_")
                    and key
                    in [
                        "SNOWFLAKE_ACCOUNT",
                        "SNOWFLAKE_USER",
                        "SNOWFLAKE_PRIVATE_KEY_FILE",
                        "SNOWFLAKE_WAREHOUSE",
                        "SNOWFLAKE_DATABASE",
                        "SNOWFLAKE_SCHEMA",
                    ]
                    for key in os.environ
                ):
                    self.snowflake_connection = SnowflakeConnectionConfig()  # type: ignore[call-arg]
                else:
                    self.snowflake_connection = None
            except Exception:
                # Snowflake connection is optional, may not be configured
                self.snowflake_connection = None
        self._parse_dynamic_config()

    def _parse_dynamic_config(self):
        """Parse dynamic Event Hub and Snowflake configurations from environment variables."""
        env_vars = dict(os.environ)

        # Parse Event Hub configurations
        event_hub_pattern = re.compile(r"^EVENTHUBNAME_(\d+)$")
        event_hub_consumer_pattern = re.compile(r"^EVENTHUBNAME_(\d+)_CONSUMER_GROUP$")
        event_hub_connection_pattern = re.compile(r"^EVENTHUBNAME_(\d+)_CONNECTION_STRING$")

        # First collect all Event Hub numbers and their consumer groups
        event_hub_data: dict[str, dict[str, str]] = {}

        for key, value in env_vars.items():
            match = event_hub_pattern.match(key)
            if match:
                hub_num = match.group(1)
                if hub_num not in event_hub_data:
                    event_hub_data[hub_num] = {}
                event_hub_data[hub_num]["name"] = value

            match = event_hub_consumer_pattern.match(key)
            if match:
                hub_num = match.group(1)
                if hub_num not in event_hub_data:
                    event_hub_data[hub_num] = {}
                event_hub_data[hub_num]["consumer_group"] = value

            match = event_hub_connection_pattern.match(key)
            if match:
                hub_num = match.group(1)
                if hub_num not in event_hub_data:
                    event_hub_data[hub_num] = {}
                event_hub_data[hub_num]["connection_string"] = value

        # Create EventHubConfig instances with consumer groups
        for hub_num, data in event_hub_data.items():
            if "name" in data:  # Only create if we have a name
                if "consumer_group" not in data:
                    raise ValueError(
                        f"EVENTHUBNAME_{hub_num}_CONSUMER_GROUP is required for EVENTHUBNAME_{hub_num}"
                    )
                self.event_hubs[f"EVENTHUBNAME_{hub_num}"] = EventHubConfig(
                    name=data["name"],
                    namespace=self.eventhub_namespace,
                    consumer_group=data["consumer_group"],
                    connection_string=data.get("connection_string"),
                )

        # Parse Snowflake configurations
        snowflake_keys: dict[str, dict[str, str]] = {}
        for key, value in env_vars.items():
            if key.startswith("SNOWFLAKE_") and "_" in key:
                parts = key.split("_", 2)
                if len(parts) >= 3:
                    sf_num = parts[1]
                    setting = parts[2]

                    if sf_num not in snowflake_keys:
                        snowflake_keys[sf_num] = {}
                    snowflake_keys[sf_num][setting.lower()] = value

        # Create Snowflake configurations
        for sf_num, settings in snowflake_keys.items():
            if all(key in settings for key in ["database", "schema", "table"]):
                self.snowflake_configs[f"SNOWFLAKE_{sf_num}"] = SnowflakeConfig(
                    database=settings["database"],
                    schema_name=settings["schema"],
                    table_name=settings["table"],
                    batch_size=int(settings.get("batch", "1000")),
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
        for num in event_hub_nums:
            if num in snowflake_nums:
                eh_key = f"EVENTHUBNAME_{num}"
                sf_key = f"SNOWFLAKE_{num}"
                if eh_key in self.event_hubs and sf_key in self.snowflake_configs:
                    self.mappings.append(
                        EventHubSnowflakeMapping(
                            event_hub_key=eh_key,
                            snowflake_key=sf_key,
                            channel_name_pattern="{event_hub}-{env}-{region}-{client_id}",
                        )
                    )

    @field_validator("eventhub_namespace")
    @classmethod
    def validate_eventhub_namespace(cls, v: str) -> str:
        """Validate Event Hub namespace format."""
        if not v.endswith(".servicebus.windows.net"):
            raise ValueError("Event Hub namespace must end with .servicebus.windows.net")
        return v

    @model_validator(mode="after")
    def validate_mappings_exist(self):
        """Validate that all mappings reference existing configurations."""
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

        return f"{event_hub_config.name}-{self.environment}-{self.region}-{client_id}"

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


def load_config(env_file: str | None = None) -> EvSnowConfig:
    """
    Load configuration from environment file.

    Args:
        env_file: Optional path to .env file. Defaults to .env in current directory.

    Returns:
        Configured EvSnowConfig instance.

    Raises:
        ValidationError: If configuration is invalid.
        FileNotFoundError: If specified env file doesn't exist.
    """
    from pathlib import Path

    if env_file:
        env_path = Path(env_file)
        if not env_path.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        # Load environment variables from specified file
        try:
            from dotenv import load_dotenv

            load_dotenv(env_path)
        except ImportError as e:
            raise ImportError(
                "python-dotenv is required for loading .env files. Install it with: pip install python-dotenv"
            ) from e

    # Get the eventhub_namespace from environment
    eventhub_namespace = os.getenv("EVENTHUB_NAMESPACE")
    if not eventhub_namespace:
        raise ValueError("EVENTHUB_NAMESPACE environment variable is required")

    return EvSnowConfig(
        eventhub_namespace=eventhub_namespace,
        environment=os.getenv("ENVIRONMENT", "development"),
        region=os.getenv("REGION", "default"),
    )


__all__ = [
    "EvSnowConfig",
    # Facade exports for existing import sites
    "EventHubConfig",
    "EventHubSnowflakeMapping",
    "LogfireConfig",
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
