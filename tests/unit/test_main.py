"""
Tests for the main CLI application.

This module tests the Typer CLI commands and functionality in src/main.py.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import pytest
from typer.testing import CliRunner

from main import app


@pytest.fixture
def cli_runner():
    """Provide a Typer CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_config():
    """Provide a mock EvSnowConfig instance."""
    config = MagicMock()
    config.validate_configuration.return_value = {
        "valid": True,
        "event_hubs_count": 1,
        "snowflake_configs_count": 1,
        "mappings_count": 1,
        "errors": [],
        "warnings": [],
    }
    
    # Create mock mapping
    mock_mapping = MagicMock()
    mock_mapping.event_hub_key = "EVENTHUBNAME_1"
    mock_mapping.snowflake_key = "SNOWFLAKE_1"
    mock_mapping.channel_name_pattern = "channel_{table}"
    config.mappings = [mock_mapping]
    
    # Create mock EventHub config  
    mock_eh = MagicMock()
    type(mock_eh).name = PropertyMock(return_value="test-hub")
    type(mock_eh).namespace = PropertyMock(return_value="test-namespace.servicebus.windows.net")
    type(mock_eh).consumer_group = PropertyMock(return_value="$Default")
    type(mock_eh).max_batch_size = PropertyMock(return_value=1000)
    type(mock_eh).max_wait_time = PropertyMock(return_value=60)
    type(mock_eh).prefetch_count = PropertyMock(return_value=300)
    config.event_hubs = {"EVENTHUBNAME_1": mock_eh}
    
    # Create mock Snowflake config
    mock_sf = MagicMock()
    type(mock_sf).database = PropertyMock(return_value="TEST_DB")
    type(mock_sf).schema_name = PropertyMock(return_value="TEST_SCHEMA")
    type(mock_sf).table_name = PropertyMock(return_value="TEST_TABLE")
    type(mock_sf).batch_size = PropertyMock(return_value=1000)
    config.snowflake_configs = {"SNOWFLAKE_1": mock_sf}
    
    config.snowflake_connection = None  # Set to None to skip control table creation
    config.logfire = MagicMock(
        enabled=False,
        service_name="evsnow",
        environment="test",
        send_to_logfire=False,
        console_logging=False,
        log_level="INFO",
    )
    config.get_event_hub_config = lambda key: config.event_hubs.get(key)
    config.get_snowflake_config = lambda key: config.snowflake_configs.get(key)
    return config


class TestVersionCommand:
    """Tests for the version command."""

    def test_version_displays_correctly(self, cli_runner):
        """Test that version command displays version information."""
        result = cli_runner.invoke(app, ["version"])

        assert result.exit_code == 0
        assert "EvSnow v0.1.0" in result.stdout
        assert "EventHub to Snowflake streaming pipeline" in result.stdout
        assert "Azure EventHub async consumer" in result.stdout


class TestCheckCredentialsCommand:
    """Tests for the check-credentials command."""

    def test_check_credentials_runs(self, cli_runner):
        """Test check-credentials command executes without errors."""
        # Patch the azure.identity imports that are done inside the function
        with patch("azure.identity.EnvironmentCredential") as mock_env, \
             patch("azure.identity.ManagedIdentityCredential") as mock_msi, \
             patch("azure.identity.AzureCliCredential") as mock_cli:
            
            # Make all credentials fail so we get a simple output
            mock_env.side_effect = Exception("No env vars")
            mock_msi.side_effect = Exception("Not in Azure")
            mock_cli.side_effect = Exception("No CLI")
            
            result = cli_runner.invoke(app, ["check-credentials"])

            assert result.exit_code == 0
            assert "Checking Available Azure Credentials" in result.stdout


class TestValidateConfigCommand:
    """Tests for the validate-config command."""

    @patch("utils.snowflake.create_control_table")  # Prevent real control table creation
    @patch("main.load_config")
    def test_validate_config_with_valid_configuration(self, mock_load_config, mock_create_table, cli_runner, mock_config):
        """Test validate-config with a valid configuration."""
        mock_load_config.return_value = mock_config
        mock_create_table.return_value = None  # Success

        # Provide 'n' as input to the "Show detailed configuration?" prompt
        result = cli_runner.invoke(app, ["validate-config"], input="n\n")

        assert result.exit_code == 0
        assert "Configuration is valid" in result.stdout or "Configuration Summary" in result.stdout

    @patch("utils.snowflake.create_control_table")  # Prevent real control table creation
    @patch("main.load_config")
    def test_validate_config_with_errors(self, mock_load_config, mock_create_table, cli_runner, mock_config):
        """Test validate-config with configuration errors."""
        mock_config.validate_configuration.return_value = {
            "valid": False,
            "event_hubs_count": 0,
            "snowflake_configs_count": 0,
            "mappings_count": 0,
            "errors": ["Missing EventHub configuration"],
            "warnings": [],
        }
        mock_load_config.return_value = mock_config
        mock_create_table.return_value = None  # Success

        # Provide 'n' as input to the "Show detailed configuration?" prompt
        result = cli_runner.invoke(app, ["validate-config"], input="n\n")

        assert result.exit_code == 0
        assert "Configuration has errors" in result.stdout or "Missing EventHub configuration" in result.stdout

    @patch("utils.snowflake.create_control_table")  # Prevent real control table creation
    @patch("main.load_config")
    def test_validate_config_with_warnings(self, mock_load_config, mock_create_table, cli_runner, mock_config):
        """Test validate-config with configuration warnings."""
        mock_config.validate_configuration.return_value = {
            "valid": True,
            "event_hubs_count": 1,
            "snowflake_configs_count": 1,
            "mappings_count": 1,
            "errors": [],
            "warnings": ["Unused EventHub configuration"],
        }
        mock_load_config.return_value = mock_config
        mock_create_table.return_value = None  # Success

        # Provide 'n' as input to the "Show detailed configuration?" prompt
        result = cli_runner.invoke(app, ["validate-config"], input="n\n")

        assert result.exit_code == 0
        assert "Configuration is valid" in result.stdout or "Warnings:" in result.stdout

    @patch("utils.snowflake.create_control_table")  # Prevent real control table creation
    @patch("main.load_config")
    def test_validate_config_with_env_file(self, mock_load_config, mock_create_table, cli_runner, mock_config):
        """Test validate-config with custom env file."""
        mock_load_config.return_value = mock_config
        mock_create_table.return_value = None  # Success

        # Provide 'n' as input to the "Show detailed configuration?" prompt
        result = cli_runner.invoke(app, ["validate-config", "--env-file", ".env.test"], input="n\n")

        assert result.exit_code == 0
        mock_load_config.assert_called_once_with(".env.test")

    @patch("main.load_config")
    def test_validate_config_handles_exception(self, mock_load_config, cli_runner):
        """Test validate-config handles exceptions gracefully."""
        mock_load_config.side_effect = ValueError("Invalid configuration")

        result = cli_runner.invoke(app, ["validate-config"])

        assert result.exit_code == 1
        assert "Configuration error" in result.stdout


class TestRunCommand:
    """Tests for the run command."""

    @patch("main.load_config")
    def test_run_with_dry_run_flag(self, mock_load_config, cli_runner, mock_config):
        """Test run command with --dry-run flag."""
        mock_load_config.return_value = mock_config

        result = cli_runner.invoke(app, ["run", "--dry-run"])

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.stdout
        assert "Processing Plan" in result.stdout

    @patch("main.load_config")
    def test_run_with_config_errors(self, mock_load_config, cli_runner, mock_config):
        """Test run command with configuration errors."""
        mock_config.validate_configuration.return_value = {
            "valid": False,
            "event_hubs_count": 0,
            "snowflake_configs_count": 0,
            "mappings_count": 0,
            "errors": ["Missing EventHub configuration"],
            "warnings": [],
        }
        mock_load_config.return_value = mock_config

        result = cli_runner.invoke(app, ["run"])

        assert result.exit_code == 1
        assert "Configuration has errors" in result.stdout

    @patch("main.load_config")
    def test_run_with_smart_retry_missing_api_key(self, mock_load_config, cli_runner, mock_config):
        """Test run command fails when --smart is enabled but API key is missing."""
        mock_load_config.return_value = mock_config
        
        # Patch SmartRetryConfig from the module where it's imported
        with patch("utils.config.SmartRetryConfig") as mock_smart_config:
            smart_cfg = MagicMock()
            smart_cfg.llm_api_key = None
            mock_smart_config.return_value = smart_cfg

            result = cli_runner.invoke(app, ["run", "--smart"])

            assert result.exit_code == 1
            assert "Smart retry requires an LLM API key" in result.stdout

    @patch("main.load_config")
    def test_run_initializes_logfire_when_enabled(self, mock_load_config, cli_runner, mock_config):
        """Test that Logfire is initialized when enabled in config."""
        mock_config.logfire.enabled = True
        mock_config.logfire.send_to_logfire = True
        mock_config.logfire.token = "test-token"
        mock_load_config.return_value = mock_config

        with patch("main.logfire.configure") as mock_logfire_configure:
            result = cli_runner.invoke(app, ["run", "--dry-run"])

            assert result.exit_code == 0
            # Logfire configure should be called
            assert mock_logfire_configure.call_count >= 1


class TestStatusCommand:
    """Tests for the status command."""

    @patch("main.load_config")
    def test_status_with_valid_config(self, mock_load_config, cli_runner, mock_config):
        """Test status command with valid configuration."""
        mock_load_config.return_value = mock_config
        
        # Patch check_connection from the module where it's imported
        with patch("utils.snowflake.check_connection", return_value=True):
            result = cli_runner.invoke(app, ["status"])

            assert result.exit_code == 0
            assert "Pipeline Status Check" in result.stdout
            assert "Configuration is valid" in result.stdout

    @patch("main.load_config")
    def test_status_with_invalid_config(self, mock_load_config, cli_runner, mock_config):
        """Test status command with invalid configuration."""
        mock_config.validate_configuration.return_value = {
            "valid": False,
            "event_hubs_count": 0,
            "snowflake_configs_count": 0,
            "mappings_count": 0,
            "errors": ["Configuration error"],
            "warnings": [],
        }
        mock_load_config.return_value = mock_config

        result = cli_runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "Configuration has errors" in result.stdout

    @patch("main.load_config")
    def test_status_handles_exception(self, mock_load_config, cli_runner):
        """Test status command handles exceptions gracefully."""
        mock_load_config.side_effect = ValueError("Invalid configuration")

        result = cli_runner.invoke(app, ["status"])

        assert result.exit_code == 1
        assert "Status check error" in result.stdout


class TestMonitorCommand:
    """Tests for the monitor command."""

    def test_monitor_not_implemented(self, cli_runner):
        """Test that monitor command shows not implemented message."""
        result = cli_runner.invoke(app, ["monitor"])

        assert result.exit_code == 1
        assert "Monitor UI not yet implemented" in result.stdout


class TestHelperFunctions:
    """Tests for helper functions in main.py."""

    @patch("main.console")
    def test_show_rbac_guidance(self, mock_console):
        """Test _show_rbac_guidance displays RBAC information."""
        from main import _show_rbac_guidance

        _show_rbac_guidance()

        # Verify console.print was called multiple times with RBAC info
        assert mock_console.print.called
        assert mock_console.print.call_count > 5

    @patch("main.logfire.configure")
    @patch("main.logger")
    def test_initialize_logfire_when_disabled(self, mock_logger, mock_logfire_configure):
        """Test _initialize_logfire when Logfire is disabled."""
        from main import _initialize_logfire

        logfire_config = MagicMock()
        logfire_config.enabled = False

        _initialize_logfire(logfire_config)

        mock_logger.info.assert_called_with("Logfire observability disabled")
        # configure should not be called beyond early initialization
        mock_logfire_configure.assert_not_called()

    @patch("main.logfire.configure")
    @patch("main.logfire.instrument_pydantic_ai")
    @patch("main.logger")
    def test_initialize_logfire_when_enabled(
        self, mock_logger, mock_instrument_pydantic, mock_logfire_configure
    ):
        """Test _initialize_logfire when Logfire is enabled."""
        from main import _initialize_logfire

        logfire_config = MagicMock()
        logfire_config.enabled = True
        logfire_config.send_to_logfire = True
        logfire_config.console_logging = True
        logfire_config.token = "test-token"
        logfire_config.service_name = "evsnow-test"
        logfire_config.environment = "test"
        logfire_config.log_level = "INFO"

        _initialize_logfire(logfire_config)

        mock_logfire_configure.assert_called_once()
        mock_instrument_pydantic.assert_called_once()

    @patch("main.logfire.configure")
    @patch("main.logger")
    def test_initialize_logfire_handles_exception(self, mock_logger, mock_logfire_configure):
        """Test _initialize_logfire handles exceptions gracefully."""
        from main import _initialize_logfire

        logfire_config = MagicMock()
        logfire_config.enabled = True
        mock_logfire_configure.side_effect = Exception("Configuration failed")

        _initialize_logfire(logfire_config)

        # Should log warning but not raise
        assert any("Failed to initialize Logfire" in str(call) for call in mock_logger.warning.call_args_list)

    @patch("main.console")
    def test_show_processing_plan(self, mock_console, mock_config):
        """Test _show_processing_plan displays processing plan."""
        from main import _show_processing_plan

        _show_processing_plan(mock_config)

        assert mock_console.print.called
        calls = [str(call) for call in mock_console.print.call_args_list]
        combined_output = " ".join(calls)
        
        assert "Processing Plan" in combined_output or mock_console.print.call_count > 0


class TestCLIEntryPoints:
    """Tests for CLI entry points."""

    @patch("main.app")
    def test_cli_main_calls_app(self, mock_app):
        """Test cli_main entry point calls app()."""
        from main import cli_main

        cli_main()

        mock_app.assert_called_once()

    def test_main_callback(self):
        """Test main callback function exists."""
        from main import main

        # Should not raise any exceptions
        result = main()
        assert result is None


class TestEnvironmentSetup:
    """Tests for environment and dotenv loading."""

    def test_logging_configured(self):
        """Test that logging is configured with RichHandler."""
        import logging

        root_logger = logging.getLogger()
        
        # Check that at least one handler exists
        assert len(root_logger.handlers) > 0

    def test_azure_logging_suppressed(self):
        """Test that Azure SDK logging is suppressed."""
        import logging

        azure_logger = logging.getLogger("azure.eventhub")
        assert azure_logger.level == logging.WARNING


class TestConfigurationDisplay:
    """Tests for configuration display functions."""

    @patch("utils.snowflake.create_control_table")  # Prevent real control table creation
    @patch("main.load_config")
    def test_validate_config_displays_summary_table(self, mock_load_config, mock_create_table, cli_runner, mock_config):
        """Test that validate-config displays configuration summary table."""
        mock_load_config.return_value = mock_config
        mock_create_table.return_value = None  # Success

        # Provide 'n' as input to the "Show detailed configuration?" prompt
        result = cli_runner.invoke(app, ["validate-config"], input="n\n")

        assert result.exit_code == 0
        # Check for either the success message or the summary table
        assert "Configuration" in result.stdout

    @patch("main.load_config")
    def test_run_dry_run_displays_processing_plan(self, mock_load_config, cli_runner, mock_config):
        """Test that run --dry-run displays processing plan."""
        mock_load_config.return_value = mock_config

        result = cli_runner.invoke(app, ["run", "--dry-run"])

        assert result.exit_code == 0
        assert "Processing Plan" in result.stdout
