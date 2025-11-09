"""
Comprehensive unit tests for the configuration management system (src/utils/config.py).

Tests all Pydantic-based configuration classes with proper mocking to avoid
accessing real files, services, or external dependencies.
"""

import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
from pydantic import ValidationError

from src.utils.config import (
    EventHubConfig,
    EventHubSnowflakeMapping,
    EvSnowConfig,
    LogfireConfig,
    SmartRetryConfig,
    SnowflakeConfig,
    SnowflakeConnectionConfig,
    load_config,
)


class TestSnowflakeConnectionConfig:
    """Tests for SnowflakeConnectionConfig class."""

    def test_load_from_environment_variables_success(self, monkeypatch, tmp_path):
        """Test loading configuration from environment variables with valid data."""
        # Create a temporary private key file
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
        monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(key_file))
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "test_db")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA_NAME", "test_schema")
        monkeypatch.setenv("SNOWFLAKE_PIPE_NAME", "TEST_PIPE")

        config = SnowflakeConnectionConfig()  # type: ignore[call-arg]

        assert config.account == "test-account"
        assert config.user == "test_user"
        assert config.warehouse == "test_warehouse"
        assert config.database == "test_db"
        assert config.schema_name == "test_schema"
        assert config.pipe_name == "TEST_PIPE"

    def test_validate_private_key_file_not_exists(self, monkeypatch):
        """Test that validation fails when private key file doesn't exist."""
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
        monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", "/nonexistent/key.pem")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "test_db")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA_NAME", "test_schema")
        monkeypatch.setenv("SNOWFLAKE_PIPE_NAME", "TEST_PIPE")

        with pytest.raises(ValidationError) as exc_info:
            SnowflakeConnectionConfig()  # type: ignore[call-arg]

        assert "Private key file not found" in str(exc_info.value)

    def test_validate_account_format_valid(self, monkeypatch, tmp_path):
        """Test that valid account formats are accepted."""
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("test key")

        valid_accounts = [
            "myaccount",
            "my-account",
            "my_account",
            "account123",
            "account.region",
        ]

        for account in valid_accounts:
            monkeypatch.setenv("SNOWFLAKE_ACCOUNT", account)
            monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
            monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(key_file))
            monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
            monkeypatch.setenv("SNOWFLAKE_DATABASE", "test_db")
            monkeypatch.setenv("SNOWFLAKE_SCHEMA_NAME", "test_schema")
            monkeypatch.setenv("SNOWFLAKE_PIPE_NAME", "TEST_PIPE")

            config = SnowflakeConnectionConfig()  # type: ignore[call-arg]
            assert config.account == account

    def test_validate_account_format_invalid(self, monkeypatch, tmp_path):
        """Test that invalid account formats are rejected."""
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("test key")

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "invalid@account")
        monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(key_file))
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "test_db")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA_NAME", "test_schema")
        monkeypatch.setenv("SNOWFLAKE_PIPE_NAME", "TEST_PIPE")

        with pytest.raises(ValidationError) as exc_info:
            SnowflakeConnectionConfig()  # type: ignore[call-arg]

        assert "invalid characters" in str(exc_info.value)

    def test_validate_account_empty(self, monkeypatch, tmp_path):
        """Test that empty account identifier is rejected."""
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("test key")

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "   ")
        monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(key_file))
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "test_db")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA_NAME", "test_schema")
        monkeypatch.setenv("SNOWFLAKE_PIPE_NAME", "TEST_PIPE")

        with pytest.raises(ValidationError) as exc_info:
            SnowflakeConnectionConfig()  # type: ignore[call-arg]

        assert "cannot be empty" in str(exc_info.value)

    def test_validate_identifiers_valid(self, monkeypatch, tmp_path):
        """Test that valid Snowflake identifiers are accepted."""
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("test key")

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
        monkeypatch.setenv("SNOWFLAKE_USER", "test_user_123")
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(key_file))
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "warehouse_$name")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "database-name")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA_NAME", "schema_123")  # Use SCHEMA_NAME not SCHEMA
        monkeypatch.setenv("SNOWFLAKE_PIPE_NAME", "TEST_PIPE")

        config = SnowflakeConnectionConfig()  # type: ignore[call-arg]

        assert config.user == "test_user_123"
        assert config.warehouse == "warehouse_$name"
        assert config.database == "database-name"
        assert config.schema_name == "schema_123"

    def test_validate_identifiers_empty(self, monkeypatch, tmp_path):
        """Test that empty identifiers are rejected."""
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("test key")

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
        monkeypatch.setenv("SNOWFLAKE_USER", "   ")
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(key_file))
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "test_db")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA_NAME", "test_schema")
        monkeypatch.setenv("SNOWFLAKE_PIPE_NAME", "TEST_PIPE")

        with pytest.raises(ValidationError) as exc_info:
            SnowflakeConnectionConfig()  # type: ignore[call-arg]

        assert "cannot be empty" in str(exc_info.value)

    def test_handle_missing_required_fields(self, monkeypatch):
        """Test that missing required fields raise appropriate errors."""
        with pytest.raises(ValidationError) as exc_info:
            SnowflakeConnectionConfig()  # type: ignore[call-arg]

        error_str = str(exc_info.value)
        assert "account" in error_str.lower() or "Field required" in error_str

    def test_private_key_password_optional(self, monkeypatch, tmp_path):
        """Test that private key password is optional."""
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("test key")

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
        monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(key_file))
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "test_db")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA_NAME", "test_schema")
        monkeypatch.setenv("SNOWFLAKE_PIPE_NAME", "TEST_PIPE")

        config = SnowflakeConnectionConfig()  # type: ignore[call-arg]

        assert config.private_key_password is None

    def test_role_optional(self, monkeypatch, tmp_path):
        """Test that role is optional."""
        key_file = tmp_path / "test_key.pem"
        key_file.write_text("test key")

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
        monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(key_file))
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "test_db")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA_NAME", "test_schema")
        monkeypatch.setenv("SNOWFLAKE_PIPE_NAME", "TEST_PIPE")

        config = SnowflakeConnectionConfig()  # type: ignore[call-arg]

        assert config.role is None


class TestSmartRetryConfig:
    """Tests for SmartRetryConfig class."""

    def test_load_retry_settings_defaults(self, monkeypatch):
        """Test loading smart retry config with default values."""
        config = SmartRetryConfig()  # type: ignore[call-arg]

        assert config.enabled is False
        assert config.llm_provider == "openai"
        assert config.llm_model == "gpt-4o-mini"
        assert config.max_attempts == 3
        assert config.timeout_seconds == 10
        assert config.enable_caching is True

    def test_load_retry_settings_from_env(self, monkeypatch):
        """Test loading smart retry settings from environment variables."""
        monkeypatch.setenv("SMART_RETRY_ENABLED", "true")
        monkeypatch.setenv("SMART_RETRY_LLM_PROVIDER", "anthropic")
        monkeypatch.setenv("SMART_RETRY_LLM_MODEL", "claude-3-sonnet")
        monkeypatch.setenv("SMART_RETRY_LLM_API_KEY", "test-api-key")
        monkeypatch.setenv("SMART_RETRY_MAX_ATTEMPTS", "5")
        monkeypatch.setenv("SMART_RETRY_TIMEOUT_SECONDS", "30")
        monkeypatch.setenv("SMART_RETRY_ENABLE_CACHING", "false")

        config = SmartRetryConfig()  # type: ignore[call-arg]

        assert config.enabled is True
        assert config.llm_provider == "anthropic"
        assert config.llm_model == "claude-3-sonnet"
        assert config.llm_api_key == "test-api-key"
        assert config.max_attempts == 5
        assert config.timeout_seconds == 30
        assert config.enable_caching is False

    def test_validate_llm_provider_valid(self, monkeypatch):
        """Test that valid LLM providers are accepted."""
        valid_providers = ["openai", "azure", "anthropic", "gemini", "groq", "cohere"]

        for provider in valid_providers:
            monkeypatch.setenv("SMART_RETRY_LLM_PROVIDER", provider)
            config = SmartRetryConfig()  # type: ignore[call-arg]
            assert config.llm_provider == provider.lower()

    def test_validate_llm_provider_invalid(self, monkeypatch):
        """Test that invalid LLM providers are rejected."""
        monkeypatch.setenv("SMART_RETRY_LLM_PROVIDER", "invalid_provider")

        with pytest.raises(ValidationError) as exc_info:
            SmartRetryConfig()  # type: ignore[call-arg]

        assert "Unsupported LLM provider" in str(exc_info.value)

    def test_validate_api_key_format_empty(self, monkeypatch):
        """Test that empty API key is rejected when provided."""
        monkeypatch.setenv("SMART_RETRY_LLM_API_KEY", "   ")

        with pytest.raises(ValidationError) as exc_info:
            SmartRetryConfig()  # type: ignore[call-arg]

        assert "cannot be empty" in str(exc_info.value)

    def test_validate_api_key_none(self, monkeypatch):
        """Test that None API key is accepted."""
        config = SmartRetryConfig()  # type: ignore[call-arg]
        assert config.llm_api_key is None

    def test_max_attempts_range(self, monkeypatch):
        """Test that max_attempts must be within valid range."""
        # Test valid range
        monkeypatch.setenv("SMART_RETRY_MAX_ATTEMPTS", "5")
        config = SmartRetryConfig()  # type: ignore[call-arg]
        assert config.max_attempts == 5

        # Test below minimum
        monkeypatch.setenv("SMART_RETRY_MAX_ATTEMPTS", "0")
        with pytest.raises(ValidationError):
            SmartRetryConfig()  # type: ignore[call-arg]

        # Test above maximum
        monkeypatch.setenv("SMART_RETRY_MAX_ATTEMPTS", "11")
        with pytest.raises(ValidationError):
            SmartRetryConfig()  # type: ignore[call-arg]

    def test_timeout_seconds_range(self, monkeypatch):
        """Test that timeout_seconds must be within valid range."""
        # Test valid range
        monkeypatch.setenv("SMART_RETRY_TIMEOUT_SECONDS", "30")
        config = SmartRetryConfig()  # type: ignore[call-arg]
        assert config.timeout_seconds == 30

        # Test below minimum
        monkeypatch.setenv("SMART_RETRY_TIMEOUT_SECONDS", "0")
        with pytest.raises(ValidationError):
            SmartRetryConfig()  # type: ignore[call-arg]

        # Test above maximum
        monkeypatch.setenv("SMART_RETRY_TIMEOUT_SECONDS", "61")
        with pytest.raises(ValidationError):
            SmartRetryConfig()  # type: ignore[call-arg]


class TestLogfireConfig:
    """Tests for LogfireConfig class."""

    def test_load_observability_settings_defaults(self, monkeypatch):
        """Test loading Logfire config with default values."""
        config = LogfireConfig()  # type: ignore[call-arg]

        assert config.enabled is False
        assert config.token is None
        assert config.service_name == "evsnow"
        assert config.environment == "development"
        assert config.send_to_logfire is True
        assert config.console_logging is True
        assert config.log_level == "INFO"

    def test_load_observability_settings_from_env(self, monkeypatch):
        """Test loading Logfire settings from environment variables."""
        monkeypatch.setenv("LOGFIRE_ENABLED", "true")
        monkeypatch.setenv("LOGFIRE_TOKEN", "test-token-123")
        monkeypatch.setenv("LOGFIRE_SERVICE_NAME", "evsnow-prod")
        monkeypatch.setenv("LOGFIRE_ENVIRONMENT", "production")
        monkeypatch.setenv("LOGFIRE_SEND_TO_LOGFIRE", "true")
        monkeypatch.setenv("LOGFIRE_CONSOLE_LOGGING", "false")
        monkeypatch.setenv("LOGFIRE_LOG_LEVEL", "DEBUG")

        config = LogfireConfig()  # type: ignore[call-arg]

        assert config.enabled is True
        assert config.token == "test-token-123"
        assert config.service_name == "evsnow-prod"
        assert config.environment == "production"
        assert config.send_to_logfire is True
        assert config.console_logging is False
        assert config.log_level == "DEBUG"

    def test_validate_log_level_valid(self, monkeypatch):
        """Test that valid log levels are accepted."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        for level in valid_levels:
            monkeypatch.setenv("LOGFIRE_LOG_LEVEL", level)
            config = LogfireConfig()  # type: ignore[call-arg]
            assert config.log_level == level

    def test_validate_log_level_case_insensitive(self, monkeypatch):
        """Test that log level validation is case-insensitive."""
        monkeypatch.setenv("LOGFIRE_LOG_LEVEL", "debug")
        config = LogfireConfig()  # type: ignore[call-arg]
        assert config.log_level == "DEBUG"

    def test_validate_log_level_invalid(self, monkeypatch):
        """Test that invalid log levels are rejected."""
        monkeypatch.setenv("LOGFIRE_LOG_LEVEL", "INVALID")

        with pytest.raises(ValidationError) as exc_info:
            LogfireConfig()  # type: ignore[call-arg]

        assert "Invalid log level" in str(exc_info.value)

    def test_require_token_when_sending_to_cloud(self, monkeypatch):
        """Test that token is required when enabled and sending to cloud."""
        monkeypatch.setenv("LOGFIRE_ENABLED", "true")
        monkeypatch.setenv("LOGFIRE_SEND_TO_LOGFIRE", "true")

        with pytest.raises(ValidationError) as exc_info:
            LogfireConfig()  # type: ignore[call-arg]

        assert "LOGFIRE_TOKEN is required" in str(exc_info.value)

    def test_token_not_required_when_disabled(self, monkeypatch):
        """Test that token is not required when Logfire is disabled."""
        monkeypatch.setenv("LOGFIRE_ENABLED", "false")
        monkeypatch.setenv("LOGFIRE_SEND_TO_LOGFIRE", "true")

        config = LogfireConfig()  # type: ignore[call-arg]
        assert config.enabled is False
        assert config.token is None

    def test_token_not_required_when_not_sending_to_cloud(self, monkeypatch):
        """Test that token is not required when not sending to cloud."""
        monkeypatch.setenv("LOGFIRE_ENABLED", "true")
        monkeypatch.setenv("LOGFIRE_SEND_TO_LOGFIRE", "false")

        config = LogfireConfig()  # type: ignore[call-arg]
        assert config.enabled is True
        assert config.send_to_logfire is False
        assert config.token is None

    def test_handle_disabled_state(self, monkeypatch):
        """Test that disabled state works correctly."""
        monkeypatch.setenv("LOGFIRE_ENABLED", "false")

        config = LogfireConfig()  # type: ignore[call-arg]
        assert config.enabled is False


class TestEventHubConfig:
    """Tests for EventHubConfig class."""

    def test_create_with_required_fields(self):
        """Test creating EventHubConfig with all required fields."""
        config = EventHubConfig(
            name="test-hub",
            namespace="test.servicebus.windows.net",
            consumer_group="$Default",
        )

        assert config.name == "test-hub"
        assert config.namespace == "test.servicebus.windows.net"
        assert config.consumer_group == "$Default"

    def test_validate_namespace_format_valid(self):
        """Test that valid namespace format is accepted."""
        config = EventHubConfig(
            name="test-hub",
            namespace="mynamespace.servicebus.windows.net",
            consumer_group="$Default",
        )

        assert config.namespace == "mynamespace.servicebus.windows.net"

    def test_validate_namespace_format_invalid(self):
        """Test that invalid namespace format is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            EventHubConfig(
                name="test-hub",
                namespace="invalid-namespace",
                consumer_group="$Default",
            )

        assert "must end with .servicebus.windows.net" in str(exc_info.value)

    def test_validate_eventhub_name_valid(self):
        """Test that valid EventHub names are accepted."""
        valid_names = ["test-hub", "hub123", "my_hub", "hub.name", "hub-1"]

        for name in valid_names:
            config = EventHubConfig(
                name=name,
                namespace="test.servicebus.windows.net",
                consumer_group="$Default",
            )
            assert config.name == name

    def test_validate_eventhub_name_invalid(self):
        """Test that invalid EventHub names are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            EventHubConfig(
                name="@invalid",
                namespace="test.servicebus.windows.net",
                consumer_group="$Default",
            )

        assert "invalid characters" in str(exc_info.value)

    def test_require_consumer_group(self):
        """Test that consumer group is required."""
        with pytest.raises(ValidationError):
            EventHubConfig(
                name="test-hub",
                namespace="test.servicebus.windows.net",
            )  # type: ignore[call-arg]

    def test_validate_consumer_group_not_empty(self):
        """Test that consumer group cannot be empty."""
        with pytest.raises(ValidationError) as exc_info:
            EventHubConfig(
                name="test-hub",
                namespace="test.servicebus.windows.net",
                consumer_group="   ",
            )

        assert "cannot be empty" in str(exc_info.value)

    def test_default_values_for_performance_settings(self):
        """Test that default values are applied for performance settings."""
        config = EventHubConfig(
            name="test-hub",
            namespace="test.servicebus.windows.net",
            consumer_group="$Default",
        )

        assert config.max_batch_size == 1000
        assert config.max_wait_time == 60
        assert config.prefetch_count == 300
        assert config.checkpoint_interval_seconds == 300
        assert config.max_message_batch_size == 1000
        assert config.batch_timeout_seconds == 300

    def test_use_connection_string_default(self):
        """Test that use_connection_string defaults to False."""
        config = EventHubConfig(
            name="test-hub",
            namespace="test.servicebus.windows.net",
            consumer_group="$Default",
        )

        assert config.use_connection_string is False

    def test_connection_string_optional(self):
        """Test that connection string is optional."""
        config = EventHubConfig(
            name="test-hub",
            namespace="test.servicebus.windows.net",
            consumer_group="$Default",
        )

        assert config.connection_string is None


class TestSnowflakeConfig:
    """Tests for SnowflakeConfig class."""

    def test_create_with_required_fields(self):
        """Test creating SnowflakeConfig with required fields."""
        config = SnowflakeConfig(
            database="TEST_DB",
            schema_name="TEST_SCHEMA",
            table_name="TEST_TABLE",
        )

        assert config.database == "TEST_DB"
        assert config.schema_name == "TEST_SCHEMA"
        assert config.table_name == "TEST_TABLE"

    def test_validate_snowflake_identifiers_valid(self):
        """Test that valid Snowflake identifiers are accepted."""
        valid_identifiers = [
            ("TEST_DB", "TEST_SCHEMA", "TEST_TABLE"),
            ("db_123", "schema_456", "table_789"),
            ("DB$NAME", "SCHEMA$NAME", "TABLE$NAME"),
            ("db-name", "schema-name", "table-name"),
        ]

        for db, schema, table in valid_identifiers:
            config = SnowflakeConfig(
                database=db,
                schema_name=schema,
                table_name=table,
            )
            assert config.database == db
            assert config.schema_name == schema
            assert config.table_name == table

    def test_validate_snowflake_identifiers_empty(self):
        """Test that empty Snowflake identifiers are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            SnowflakeConfig(
                database="   ",
                schema_name="TEST_SCHEMA",
                table_name="TEST_TABLE",
            )

        assert "cannot be empty" in str(exc_info.value)

    def test_validate_snowflake_identifiers_invalid(self):
        """Test that invalid Snowflake identifiers are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            SnowflakeConfig(
                database="TEST@DB",
                schema_name="TEST_SCHEMA",
                table_name="TEST_TABLE",
            )

        assert "Invalid Snowflake identifier" in str(exc_info.value)

    def test_default_batch_size(self):
        """Test that default batch size is applied."""
        config = SnowflakeConfig(
            database="TEST_DB",
            schema_name="TEST_SCHEMA",
            table_name="TEST_TABLE",
        )

        assert config.batch_size == 1000

    def test_default_retry_settings(self):
        """Test that default retry settings are applied."""
        config = SnowflakeConfig(
            database="TEST_DB",
            schema_name="TEST_SCHEMA",
            table_name="TEST_TABLE",
        )

        assert config.max_retry_attempts == 3
        assert config.retry_delay_seconds == 5
        assert config.connection_timeout_seconds == 30

    def test_custom_batch_size(self):
        """Test setting custom batch size."""
        config = SnowflakeConfig(
            database="TEST_DB",
            schema_name="TEST_SCHEMA",
            table_name="TEST_TABLE",
            batch_size=5000,
        )

        assert config.batch_size == 5000


class TestEventHubSnowflakeMapping:
    """Tests for EventHubSnowflakeMapping class."""

    def test_create_mapping_with_valid_keys(self):
        """Test creating mapping with valid keys."""
        mapping = EventHubSnowflakeMapping(
            event_hub_key="EVENTHUBNAME_1",
            snowflake_key="SNOWFLAKE_1",
        )

        assert mapping.event_hub_key == "EVENTHUBNAME_1"
        assert mapping.snowflake_key == "SNOWFLAKE_1"

    def test_validate_mapping_key_format_valid(self):
        """Test that valid mapping key formats are accepted."""
        valid_keys = [
            ("EVENTHUBNAME_1", "SNOWFLAKE_1"),
            ("HUB_CONFIG_2", "SF_CONFIG_2"),
            ("TEST_123", "CONFIG_456"),
        ]

        for eh_key, sf_key in valid_keys:
            mapping = EventHubSnowflakeMapping(
                event_hub_key=eh_key,
                snowflake_key=sf_key,
            )
            assert mapping.event_hub_key == eh_key
            assert mapping.snowflake_key == sf_key

    def test_validate_mapping_key_format_invalid(self):
        """Test that invalid mapping key formats are rejected."""
        invalid_keys = [
            ("lowercase", "SNOWFLAKE_1"),
            ("MIXED_Case", "SNOWFLAKE_1"),
            ("INVALID-KEY", "SNOWFLAKE_1"),
            ("INVALID.KEY", "SNOWFLAKE_1"),
        ]

        for eh_key, sf_key in invalid_keys:
            with pytest.raises(ValidationError) as exc_info:
                EventHubSnowflakeMapping(
                    event_hub_key=eh_key,
                    snowflake_key=sf_key,
                )
            assert "uppercase with underscores" in str(exc_info.value)

    def test_channel_name_pattern_default(self):
        """Test that default channel name pattern is set."""
        mapping = EventHubSnowflakeMapping(
            event_hub_key="EVENTHUBNAME_1",
            snowflake_key="SNOWFLAKE_1",
        )

        assert mapping.channel_name_pattern == "{event_hub}-{env}-{region}-{client_id}"


class TestEvSnowConfig:
    """Tests for EvSnowConfig main configuration class."""

    def test_parse_dynamic_eventhub_configs_from_env(self, monkeypatch):
        """Test parsing dynamic EventHub configurations from environment variables."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("EVENTHUBNAME_1", "hub1")
        monkeypatch.setenv("EVENTHUBNAME_1_CONSUMER_GROUP", "group1")
        monkeypatch.setenv("EVENTHUBNAME_2", "hub2")
        monkeypatch.setenv("EVENTHUBNAME_2_CONSUMER_GROUP", "group2")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        assert len(config.event_hubs) == 2
        assert "EVENTHUBNAME_1" in config.event_hubs
        assert "EVENTHUBNAME_2" in config.event_hubs
        assert config.event_hubs["EVENTHUBNAME_1"].name == "hub1"
        assert config.event_hubs["EVENTHUBNAME_1"].consumer_group == "group1"
        assert config.event_hubs["EVENTHUBNAME_2"].name == "hub2"
        assert config.event_hubs["EVENTHUBNAME_2"].consumer_group == "group2"

    def test_parse_dynamic_snowflake_configs_from_env(self, monkeypatch):
        """Test parsing dynamic Snowflake configurations from environment variables."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("SNOWFLAKE_1_DATABASE", "DB1")
        monkeypatch.setenv("SNOWFLAKE_1_SCHEMA", "SCHEMA1")
        monkeypatch.setenv("SNOWFLAKE_1_TABLE", "TABLE1")
        monkeypatch.setenv("SNOWFLAKE_2_DATABASE", "DB2")
        monkeypatch.setenv("SNOWFLAKE_2_SCHEMA", "SCHEMA2")
        monkeypatch.setenv("SNOWFLAKE_2_TABLE", "TABLE2")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        assert len(config.snowflake_configs) == 2
        assert "SNOWFLAKE_1" in config.snowflake_configs
        assert "SNOWFLAKE_2" in config.snowflake_configs
        assert config.snowflake_configs["SNOWFLAKE_1"].database == "DB1"
        assert config.snowflake_configs["SNOWFLAKE_1"].schema_name == "SCHEMA1"
        assert config.snowflake_configs["SNOWFLAKE_1"].table_name == "TABLE1"

    def test_auto_create_mappings(self, monkeypatch):
        """Test automatic creation of mappings based on matching numbers."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("EVENTHUBNAME_1", "hub1")
        monkeypatch.setenv("EVENTHUBNAME_1_CONSUMER_GROUP", "group1")
        monkeypatch.setenv("SNOWFLAKE_1_DATABASE", "DB1")
        monkeypatch.setenv("SNOWFLAKE_1_SCHEMA", "SCHEMA1")
        monkeypatch.setenv("SNOWFLAKE_1_TABLE", "TABLE1")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        assert len(config.mappings) == 1
        assert config.mappings[0].event_hub_key == "EVENTHUBNAME_1"
        assert config.mappings[0].snowflake_key == "SNOWFLAKE_1"

    def test_validate_mappings_reference_existing_configs(self, monkeypatch):
        """Test that validation fails if mappings reference non-existent configs."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("EVENTHUBNAME_1", "hub1")
        monkeypatch.setenv("EVENTHUBNAME_1_CONSUMER_GROUP", "group1")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        # Manually add invalid mapping
        config.mappings.append(
            EventHubSnowflakeMapping(
                event_hub_key="EVENTHUBNAME_1",
                snowflake_key="NONEXISTENT",
            )
        )

        with pytest.raises(ValidationError) as exc_info:
            config.model_validate(config)

        assert "non-existent" in str(exc_info.value).lower()

    def test_validate_configuration_completeness(self, monkeypatch):
        """Test validate_configuration method returns correct results."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("EVENTHUBNAME_1", "hub1")
        monkeypatch.setenv("EVENTHUBNAME_1_CONSUMER_GROUP", "group1")
        monkeypatch.setenv("SNOWFLAKE_1_DATABASE", "DB1")
        monkeypatch.setenv("SNOWFLAKE_1_SCHEMA", "SCHEMA1")
        monkeypatch.setenv("SNOWFLAKE_1_TABLE", "TABLE1")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")
        results = config.validate_configuration()

        assert results["valid"] is True
        assert results["event_hubs_count"] == 1
        assert results["snowflake_configs_count"] == 1
        assert results["mappings_count"] == 1
        assert isinstance(results["warnings"], list)
        assert isinstance(results["errors"], list)

    def test_generate_channel_names(self, monkeypatch):
        """Test channel name generation."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("EVENTHUBNAME_1", "hub1")
        monkeypatch.setenv("EVENTHUBNAME_1_CONSUMER_GROUP", "group1")
        monkeypatch.setenv("SNOWFLAKE_1_DATABASE", "DB1")
        monkeypatch.setenv("SNOWFLAKE_1_SCHEMA", "SCHEMA1")
        monkeypatch.setenv("SNOWFLAKE_1_TABLE", "TABLE1")

        config = EvSnowConfig(
            eventhub_namespace="test.servicebus.windows.net",
            environment="production",
            region="us-east",
        )

        channel_name = config.generate_channel_name("EVENTHUBNAME_1", "client123")

        assert channel_name == "hub1-production-us-east-client123"

    def test_get_event_hub_config(self, monkeypatch):
        """Test getting EventHub config by key."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("EVENTHUBNAME_1", "hub1")
        monkeypatch.setenv("EVENTHUBNAME_1_CONSUMER_GROUP", "group1")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        hub_config = config.get_event_hub_config("EVENTHUBNAME_1")
        assert hub_config is not None
        assert hub_config.name == "hub1"

        non_existent = config.get_event_hub_config("NONEXISTENT")
        assert non_existent is None

    def test_get_snowflake_config(self, monkeypatch):
        """Test getting Snowflake config by key."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("SNOWFLAKE_1_DATABASE", "DB1")
        monkeypatch.setenv("SNOWFLAKE_1_SCHEMA", "SCHEMA1")
        monkeypatch.setenv("SNOWFLAKE_1_TABLE", "TABLE1")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        sf_config = config.get_snowflake_config("SNOWFLAKE_1")
        assert sf_config is not None
        assert sf_config.database == "DB1"

        non_existent = config.get_snowflake_config("NONEXISTENT")
        assert non_existent is None

    def test_get_mapping_for_event_hub(self, monkeypatch):
        """Test getting mapping for EventHub."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("EVENTHUBNAME_1", "hub1")
        monkeypatch.setenv("EVENTHUBNAME_1_CONSUMER_GROUP", "group1")
        monkeypatch.setenv("SNOWFLAKE_1_DATABASE", "DB1")
        monkeypatch.setenv("SNOWFLAKE_1_SCHEMA", "SCHEMA1")
        monkeypatch.setenv("SNOWFLAKE_1_TABLE", "TABLE1")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        mapping = config.get_mapping_for_event_hub("EVENTHUBNAME_1")
        assert mapping is not None
        assert mapping.event_hub_key == "EVENTHUBNAME_1"
        assert mapping.snowflake_key == "SNOWFLAKE_1"

        non_existent = config.get_mapping_for_event_hub("NONEXISTENT")
        assert non_existent is None

    def test_require_consumer_group_for_eventhub(self, monkeypatch):
        """Test that consumer group is required for EventHub configs."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("EVENTHUBNAME_1", "hub1")
        # Missing EVENTHUBNAME_1_CONSUMER_GROUP

        with pytest.raises(ValueError) as exc_info:
            EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        assert "CONSUMER_GROUP is required" in str(exc_info.value)

    def test_eventhub_namespace_validation(self, monkeypatch):
        """Test EventHub namespace validation."""
        with pytest.raises(ValidationError) as exc_info:
            EvSnowConfig(eventhub_namespace="invalid-namespace")

        assert "must end with .servicebus.windows.net" in str(exc_info.value)

    def test_default_values_applied(self, monkeypatch):
        """Test that default values are applied correctly."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        assert config.environment == "development"
        assert config.region == "default"
        assert config.max_concurrent_channels == 50
        assert config.ingestion_timeout_seconds == 300
        assert config.max_concurrent_mappings == 10
        assert config.health_check_interval_seconds == 60
        assert config.max_pipeline_restart_attempts == 3
        assert config.pipeline_restart_delay_seconds == 30
        assert config.enable_detailed_logging is False
        assert config.log_message_samples is False
        assert config.metrics_collection_enabled is True

    def test_logfire_config_included(self, monkeypatch):
        """Test that Logfire config is included."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        assert config.logfire is not None
        assert isinstance(config.logfire, LogfireConfig)

    def test_snowflake_connection_optional(self, monkeypatch):
        """Test that Snowflake connection config is optional."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")

        # Should not fail if Snowflake connection vars are not set
        assert config.snowflake_connection is None

    def test_unmapped_configurations_warning(self, monkeypatch):
        """Test that unmapped configurations generate warnings."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("EVENTHUBNAME_1", "hub1")
        monkeypatch.setenv("EVENTHUBNAME_1_CONSUMER_GROUP", "group1")
        monkeypatch.setenv("EVENTHUBNAME_2", "hub2")
        monkeypatch.setenv("EVENTHUBNAME_2_CONSUMER_GROUP", "group2")
        monkeypatch.setenv("SNOWFLAKE_1_DATABASE", "DB1")
        monkeypatch.setenv("SNOWFLAKE_1_SCHEMA", "SCHEMA1")
        monkeypatch.setenv("SNOWFLAKE_1_TABLE", "TABLE1")

        config = EvSnowConfig(eventhub_namespace="test.servicebus.windows.net")
        results = config.validate_configuration()

        # hub2 is unmapped
        assert len(results["warnings"]) > 0
        warnings_str = " ".join(results["warnings"])
        assert "EVENTHUBNAME_2" in warnings_str


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_config_with_env_file(self, monkeypatch, tmp_path):
        """Test loading config from environment file."""
        # Clear any potentially polluted environment variables first
        for key in list(os.environ.keys()):
            if key.startswith(("EVENTHUB", "SNOWFLAKE_", "SMART_RETRY", "LOGFIRE")):
                monkeypatch.delenv(key, raising=False)
        
        env_file = tmp_path / ".env"
        env_file.write_text(
            "EVENTHUB_NAMESPACE=test.servicebus.windows.net\n"
            "ENVIRONMENT=production\n"
            "REGION=us-east\n"
        )

        config = load_config(str(env_file))

        assert config.eventhub_namespace == "test.servicebus.windows.net"
        assert config.environment == "production"
        assert config.region == "us-east"

    def test_load_config_without_env_file(self, monkeypatch):
        """Test loading config from environment variables without file."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        monkeypatch.setenv("ENVIRONMENT", "staging")
        monkeypatch.setenv("REGION", "eu-west")

        config = load_config()

        assert config.eventhub_namespace == "test.servicebus.windows.net"
        assert config.environment == "staging"
        assert config.region == "eu-west"

    def test_load_config_missing_eventhub_namespace(self, monkeypatch):
        """Test that missing EVENTHUB_NAMESPACE raises error."""
        # Clear the environment variable
        monkeypatch.delenv("EVENTHUB_NAMESPACE", raising=False)

        with pytest.raises(ValueError) as exc_info:
            load_config()

        assert "EVENTHUB_NAMESPACE" in str(exc_info.value)

    def test_load_config_nonexistent_file(self):
        """Test that loading non-existent file raises error."""
        with pytest.raises(FileNotFoundError) as exc_info:
            load_config("/nonexistent/.env")

        assert "not found" in str(exc_info.value)

    def test_load_config_default_values(self, monkeypatch):
        """Test that default values are used when not specified."""
        monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
        # Clear environment variables to test defaults
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        monkeypatch.delenv("REGION", raising=False)

        config = load_config()

        assert config.environment == "development"
        assert config.region == "default"
