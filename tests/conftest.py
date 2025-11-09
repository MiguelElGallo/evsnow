"""
Pytest configuration and shared fixtures for EvSnow tests.

This module provides reusable fixtures for mocking external dependencies
and creating test data consistently across all test modules.

Based on TESTING_STANDARDS.md - all fixtures mock external services
(Snowflake, EventHub, Azure, Logfire) to ensure fast, isolated tests.
"""

import asyncio
import json
import time
from datetime import datetime, UTC
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from src.consumers.eventhub import EventHubMessage
from src.utils.config import (
    EventHubConfig,
    EventHubSnowflakeMapping,
    EvSnowConfig,
    SnowflakeConfig,
    SnowflakeConnectionConfig,
    LogfireConfig,
    SmartRetryConfig,
)


# ============================================================================
# Fixtures: Configuration Objects
# ============================================================================


@pytest.fixture
def sample_eventhub_config() -> EventHubConfig:
    """Create a sample EventHub configuration for testing."""
    return EventHubConfig(
        name="test-hub",
        namespace="test-namespace.servicebus.windows.net",
        consumer_group="test-group",
        max_batch_size=100,
        max_wait_time=30,
        prefetch_count=50,
        checkpoint_interval_seconds=60,
        max_message_batch_size=100,
        batch_timeout_seconds=60,
    )


@pytest.fixture
def sample_snowflake_config() -> SnowflakeConfig:
    """Create a sample Snowflake configuration for testing."""
    return SnowflakeConfig(
        database="TEST_DB",
        schema_name="TEST_SCHEMA",
        table_name="TEST_TABLE",
        batch_size=1000,
        max_retry_attempts=3,
        retry_delay_seconds=5,
        connection_timeout_seconds=30,
    )


@pytest.fixture
def sample_snowflake_connection_config(tmp_path) -> SnowflakeConnectionConfig:
    """Create a sample Snowflake connection configuration for testing."""
    # Create a valid temporary private key file for testing
    # Generate a real RSA key so cryptography library can load it
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    
    # Generate RSA private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    
    # Serialize to PEM format
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    # Write to file
    key_file = tmp_path / "test_key.pem"
    key_file.write_bytes(pem)
    
    return SnowflakeConnectionConfig(
        account="test-account",
        user="test_user",
        private_key_file=str(key_file),
        private_key_password=None,
        warehouse="TEST_WH",
        database="TEST_DB",
        schema_name="TEST_SCHEMA",
        role="TEST_ROLE",
        pipe_name="TEST_PIPE",
    )


@pytest.fixture
def sample_mapping() -> EventHubSnowflakeMapping:
    """Create a sample EventHub to Snowflake mapping."""
    return EventHubSnowflakeMapping(
        event_hub_key="EVENTHUBNAME_1",
        snowflake_key="SNOWFLAKE_1",
    )


@pytest.fixture
def sample_logfire_config() -> LogfireConfig:
    """Create a sample Logfire configuration for testing."""
    return LogfireConfig(
        enabled=False,  # Disabled for tests
        token=None,
        service_name="evsnow-test",
        environment="test",
        send_to_logfire=False,
        console_logging=False,
        log_level="INFO",
    )


@pytest.fixture
def sample_smart_retry_config() -> SmartRetryConfig:
    """Create a sample SmartRetry configuration for testing."""
    return SmartRetryConfig(
        enabled=False,
        llm_provider="openai",
        llm_model="gpt-4o-mini",
        llm_api_key=None,
        max_attempts=3,
        timeout_seconds=10,
        enable_caching=True,
    )


# ============================================================================
# Fixtures: Message Objects
# ============================================================================


def _create_mock_event_data(body_dict: dict[str, Any], enqueued_time=None) -> MagicMock:
    """Create a mock EventData object for testing."""
    if enqueued_time is None:
        enqueued_time = datetime.now(UTC)
    
    mock_event = MagicMock()
    mock_event.body_as_str.return_value = json.dumps(body_dict)
    mock_event.enqueued_time = enqueued_time
    mock_event.properties = {"key": "value"}
    mock_event.system_properties = {"offset": "12345", "sequence_number": 100}
    mock_event.offset = "12345"
    mock_event.sequence_number = 100
    return mock_event


@pytest.fixture
def sample_event_data() -> MagicMock:
    """Create sample EventData for testing."""
    return _create_mock_event_data({
        "source": "test-source",
        "timestamp": "2024-11-08T10:00:00Z",
        "message_id": "msg-123",
        "payload": {"value": 42},
    })


@pytest.fixture
def sample_eventhub_message(sample_event_data) -> EventHubMessage:
    """Create a sample EventHub message for testing."""
    return EventHubMessage(
        event_data=sample_event_data,
        partition_id="0",
        sequence_number=100,
    )


@pytest.fixture
def sample_eventhub_messages() -> list[EventHubMessage]:
    """Create multiple sample EventHub messages for batch testing."""
    messages = []
    for i in range(10):
        mock_event_data = _create_mock_event_data({
            "source": "test-source",
            "message_id": f"msg-{i}",
            "value": i,
        })
        msg = EventHubMessage(
            event_data=mock_event_data,
            partition_id=str(i % 3),  # Distribute across 3 partitions
            sequence_number=100 + i,
        )
        messages.append(msg)
    return messages


# ============================================================================
# Fixtures: Mock Azure SDK Components
# ============================================================================


@pytest.fixture
def mock_eventhub_client(mocker):
    """Mock EventHub consumer client."""
    import asyncio
    mock_client = mocker.MagicMock()
    # Make receive immediately raise CancelledError to prevent tests from hanging
    mock_client.receive = mocker.AsyncMock(side_effect=asyncio.CancelledError())
    mock_client.close = mocker.AsyncMock()
    
    # Patch the EventHub client creation - need to patch where it's USED not where it's defined
    mocker.patch(
        "src.consumers.eventhub.EventHubConsumerClient",
        return_value=mock_client
    )
    mocker.patch(
        "src.consumers.eventhub.EventHubConsumerClient.from_connection_string",
        return_value=mock_client
    )
    
    return mock_client


@pytest.fixture
def mock_azure_credential(mocker):
    """Mock Azure DefaultAzureCredential and AzureCliCredential."""
    mock_cred = mocker.AsyncMock()
    mock_cred.get_token = mocker.AsyncMock(
        return_value=mocker.MagicMock(
            token="mock_token",
            expires_on=time.time() + 3600
        )
    )
    mock_cred.close = mocker.AsyncMock()
    
    # Create a mock module for azure.identity.aio with the credentials
    mock_aio_module = mocker.MagicMock()
    mock_aio_module.DefaultAzureCredential = mocker.MagicMock(return_value=mock_cred)
    mock_aio_module.AzureCliCredential = mocker.MagicMock(return_value=mock_cred)
    
    # Patch the module so imports work correctly
    # ALWAYS set the mock, even if modules already exist
    import sys
    import types
    if 'azure' not in sys.modules:
        sys.modules['azure'] = types.ModuleType('azure')
    if 'azure.identity' not in sys.modules:
        azure_identity = types.ModuleType('azure.identity')
        sys.modules['azure.identity'] = azure_identity
        sys.modules['azure'].identity = azure_identity
    
    # Always override the aio module to ensure tests use mocked credentials
    sys.modules['azure.identity'].aio = mock_aio_module
    sys.modules['azure.identity.aio'] = mock_aio_module
    
    return mock_cred


@pytest.fixture
def mock_partition_context(mocker):
    """Mock EventHub partition context."""
    mock_context = mocker.MagicMock()
    mock_context.partition_id = "0"
    mock_context.update_checkpoint = mocker.AsyncMock()
    
    return mock_context


# ============================================================================
# Fixtures: Mock Snowflake Components
# ============================================================================


@pytest.fixture
def mock_snowflake_connection(mocker):
    """Mock Snowflake connection."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock common query results
    mock_cursor.execute.return_value = None
    mock_cursor.fetchone.return_value = ("8.0.0",)
    mock_cursor.fetchall.return_value = []
    mock_cursor.close.return_value = None
    
    # Patch the get_connection function
    mocker.patch("utils.snowflake.get_connection", return_value=mock_conn)
    mocker.patch("src.utils.snowflake.get_connection", return_value=mock_conn)
    
    return mock_conn


@pytest.fixture
def mock_snowpark_session(mocker):
    """Mock Snowpark session."""
    mock_session = mocker.MagicMock()
    mock_df = mocker.MagicMock()
    mock_df.collect.return_value = []
    mock_session.sql.return_value = mock_df
    mock_session.table.return_value = mock_df
    mock_session.close.return_value = None
    
    mocker.patch("utils.snowflake.get_snowpark_session", return_value=mock_session)
    mocker.patch("src.utils.snowflake.get_snowpark_session", return_value=mock_session)
    
    return mock_session


@pytest.fixture
def mock_snowflake_streaming_client(mocker):
    """Mock Snowflake StreamingIngestClient (high-performance SDK)."""
    mock_client = mocker.MagicMock()
    mock_channel = mocker.MagicMock()
    mock_channel.append_row.return_value = None
    mock_channel.get_latest_committed_offset_token.return_value = "12345"
    mock_channel.close.return_value = None
    
    mock_client.open_channel.return_value = (mock_channel, "OPEN")
    mock_client.close.return_value = None
    
    mocker.patch(
        "snowflake.ingest.streaming.StreamingIngestClient",
        return_value=mock_client
    )
    
    return mock_client


# ============================================================================
# Fixtures: Mock Logfire
# ============================================================================


@pytest.fixture
def mock_logfire(mocker):
    """Mock Logfire for observability."""
    # Mock logfire module functions
    mocker.patch("logfire.configure")
    mocker.patch("logfire.info")
    mocker.patch("logfire.error")
    mocker.patch("logfire.warn")
    
    # Mock span context manager
    mock_span = mocker.MagicMock()
    mock_span.__enter__ = mocker.MagicMock(return_value=mock_span)
    mock_span.__exit__ = mocker.MagicMock(return_value=None)
    mock_span.set_attribute = mocker.MagicMock()
    
    mocker.patch("logfire.span", return_value=mock_span)
    mocker.patch("logfire.instrument_pydantic_ai")
    
    return mock_span


# ============================================================================
# Fixtures: Mock LLM Components
# ============================================================================


@pytest.fixture
def mock_pydantic_ai_agent(mocker):
    """Mock Pydantic AI Agent for smart retry."""
    mock_agent = mocker.MagicMock()
    mock_result = mocker.MagicMock()
    mock_result.output = mocker.MagicMock(
        should_retry=True,
        reasoning="Test retry decision",
        suggested_wait_seconds=2,
        confidence=0.8
    )
    mock_agent.run = mocker.AsyncMock(return_value=mock_result)
    
    mocker.patch("pydantic_ai.Agent", return_value=mock_agent)
    
    return mock_agent


# ============================================================================
# Fixtures: Async Utilities
# ============================================================================


@pytest.fixture(scope="session")
def event_loop_policy():
    """Set event loop policy for the test session."""
    return asyncio.DefaultEventLoopPolicy()


# ============================================================================
# Fixtures: Environment Variables
# ============================================================================


@pytest.fixture(autouse=True)
def isolate_env_for_config_tests(request, monkeypatch, tmp_path):
    """
    Automatically isolate environment for config tests.
    
    This fixture runs for config tests and prevents them from loading the real .env file
    by temporarily changing to a directory without a .env file.
    """
    # Apply to test_config.py, test_snowflake_utils.py, and test_orchestrator.py tests
    if ("test_config.py" in request.fspath.strpath or 
        "test_snowflake_utils.py" in request.fspath.strpath or
        "test_orchestrator.py" in request.fspath.strpath):
        import os
        
        # Save original directory
        original_dir = os.getcwd()
        
        # Change to temp directory (no .env file there)
        os.chdir(str(tmp_path))
        
        # Clear all environment variables that might be set in .env
        env_vars_to_clear = [
            # Snowflake connection
            "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_FILE",
            "SNOWFLAKE_PRIVATE_KEY_PASSWORD", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA", "SNOWFLAKE_SCHEMA_NAME", "SNOWFLAKE_ROLE", "SNOWFLAKE_PIPE_NAME",
            # Smart Retry
            "SMART_RETRY_ENABLED", "SMART_RETRY_LLM_PROVIDER", "SMART_RETRY_LLM_MODEL",
            "SMART_RETRY_LLM_API_KEY", "SMART_RETRY_LLM_ENDPOINT", "SMART_RETRY_MAX_ATTEMPTS",
            "SMART_RETRY_TIMEOUT_SECONDS", "SMART_RETRY_ENABLE_CACHING",
            # Logfire
            "LOGFIRE_ENABLED", "LOGFIRE_TOKEN", "LOGFIRE_SERVICE_NAME", "LOGFIRE_ENVIRONMENT",
            "LOGFIRE_SEND_TO_LOGFIRE", "LOGFIRE_CONSOLE_LOGGING", "LOGFIRE_LOG_LEVEL",
        ]
        for var in env_vars_to_clear:
            monkeypatch.delenv(var, raising=False)
        
        # Also clear any dynamically created EVENTHUBNAME_* and SNOWFLAKE_* variables
        # that might have leaked from previous tests
        for key in list(os.environ.keys()):
            if key.startswith(("EVENTHUBNAME_", "SNOWFLAKE_")) and key != "EVENTHUB_NAMESPACE":
                monkeypatch.delenv(key, raising=False)
        
        yield
        
        # Restore original directory
        os.chdir(original_dir)
    else:
        yield


@pytest.fixture
def env_setup(monkeypatch):
    """Set up environment variables for testing."""
    test_env = {
        "EVENTHUB_NAMESPACE": "test-namespace.servicebus.windows.net",
        "EVENTHUBNAME_1": "test-hub",
        "EVENTHUBNAME_1_CONSUMER_GROUP": "test-group",
        "SNOWFLAKE_1_DATABASE": "TEST_DB",
        "SNOWFLAKE_1_SCHEMA": "TEST_SCHEMA",
        "SNOWFLAKE_1_TABLE": "TEST_TABLE",
        "SNOWFLAKE_ACCOUNT": "test-account",
        "SNOWFLAKE_USER": "test_user",
        "SNOWFLAKE_WAREHOUSE": "TEST_WH",
        "SNOWFLAKE_DATABASE": "TEST_DB",
        "SNOWFLAKE_SCHEMA_NAME": "TEST_SCHEMA",
        "SNOWFLAKE_PIPE_NAME": "TEST_PIPE",
        "TARGET_DB": "CONTROL",
        "TARGET_SCHEMA": "PUBLIC",
        "TARGET_TABLE": "INGESTION_STATUS",
    }
    for key, value in test_env.items():
        monkeypatch.setenv(key, value)
    return test_env


# ============================================================================
# Pytest Configuration
# ============================================================================


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "asyncio: mark test as an async test")
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
