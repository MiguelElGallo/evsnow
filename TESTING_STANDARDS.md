# EvSnow Testing Standards

## Overview

This document defines the testing standards for the EvSnow project after the major refactoring from Motherduck to Snowflake. All tests must use mocks instead of real services to ensure fast, reliable, and isolated test execution.

## Testing Philosophy

1. **No Real Services**: Tests must not interact with real services (Snowflake, EventHub, Logfire, Azure services)
2. **Fast Execution**: Unit tests should run in milliseconds, integration tests in seconds
3. **Isolated**: Each test should be independent and not affect other tests
4. **Readable**: Test names and structure should clearly communicate intent
5. **Comprehensive**: Aim for high code coverage (>80%) with meaningful tests

## Test Organization

### Directory Structure

```
tests/
├── __init__.py
├── conftest.py                  # Shared fixtures and configuration
├── unit/                        # Unit tests for individual modules
│   ├── __init__.py
│   ├── test_config.py
│   ├── test_eventhub.py
│   ├── test_orchestrator.py
│   ├── test_snowflake_client.py
│   ├── test_snowflake_utils.py
│   ├── test_smart_retry.py
│   └── test_cli.py
└── integration/                 # Integration tests (with mocks)
    ├── __init__.py
    ├── test_pipeline_integration.py
    └── test_end_to_end_flow.py
```

## Naming Conventions

### Test Files

- **Unit tests**: `test_<module_name>.py` in `tests/unit/`
- **Integration tests**: `test_<feature>_integration.py` in `tests/integration/`

**Examples:**
- `src/consumers/eventhub.py` → `tests/unit/test_eventhub.py`
- `src/utils/config.py` → `tests/unit/test_config.py`
- `src/streaming/snowflake_high_performance.py` → `tests/unit/test_snowflake_client.py`

### Test Functions

Pattern: `test_<function_or_method>_<scenario>_<expected_result>`

**Examples:**
```python
def test_load_config_with_valid_env_returns_config()
def test_eventhub_consumer_start_without_credentials_raises_error()
def test_batch_ready_when_size_exceeded_returns_true()
def test_checkpoint_save_with_invalid_partition_raises_error()
def test_ingest_batch_with_connection_error_retries()
```

### Test Classes

Pattern: `Test<ClassName>`

**Examples:**
```python
class TestEventHubAsyncConsumer:
    """Tests for EventHubAsyncConsumer class."""
    
class TestSnowflakeStreamingClient:
    """Tests for Snowflake streaming client."""
    
class TestPipelineOrchestrator:
    """Tests for pipeline orchestrator."""
```

### Mock and Fixture Naming

- **Mock fixtures**: `mock_<service>` or `mock_<component>`
- **Data fixtures**: `<entity>_data` or `sample_<entity>`

**Examples:**
```python
@pytest.fixture
def mock_snowflake_connection():
    """Mock Snowflake connection."""
    
@pytest.fixture
def mock_eventhub_client():
    """Mock EventHub client."""
    
@pytest.fixture
def sample_event_data():
    """Sample EventHub event data."""
    
@pytest.fixture
def config_data():
    """Configuration data for tests."""
```

## Testing Patterns

### Mocking External Services

#### Snowflake Mocking

```python
@pytest.fixture
def mock_snowflake_connection(mocker):
    """Mock Snowflake connection."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock specific methods
    mock_cursor.execute.return_value = None
    mock_cursor.fetchone.return_value = ("8.0.0",)
    
    # Patch the get_connection function
    mocker.patch("utils.snowflake.get_connection", return_value=mock_conn)
    
    return mock_conn
```

#### EventHub Mocking

```python
@pytest.fixture
def mock_eventhub_client(mocker):
    """Mock EventHub consumer client."""
    mock_client = mocker.MagicMock()
    mock_client.receive = mocker.AsyncMock()
    mock_client.close = mocker.AsyncMock()
    
    # Patch the EventHub client
    mocker.patch(
        "azure.eventhub.aio.EventHubConsumerClient",
        return_value=mock_client
    )
    
    return mock_client
```

#### Logfire Mocking

```python
@pytest.fixture
def mock_logfire(mocker):
    """Mock Logfire for observability."""
    # Mock logfire module
    mock_logfire = mocker.MagicMock()
    mock_logfire.configure = mocker.MagicMock()
    mock_logfire.info = mocker.MagicMock()
    mock_logfire.error = mocker.MagicMock()
    mock_logfire.span = mocker.MagicMock()
    
    # Patch logfire imports
    mocker.patch("logfire.configure")
    mocker.patch("logfire.info")
    mocker.patch("logfire.error")
    mocker.patch("logfire.span", return_value=mocker.MagicMock())
    
    return mock_logfire
```

#### Azure Credential Mocking

```python
@pytest.fixture
def mock_azure_credential(mocker):
    """Mock Azure credential."""
    mock_cred = mocker.MagicMock()
    mock_cred.get_token = mocker.AsyncMock(
        return_value=mocker.MagicMock(
            token="mock_token",
            expires_on=9999999999
        )
    )
    
    mocker.patch(
        "azure.identity.aio.DefaultAzureCredential",
        return_value=mock_cred
    )
    
    return mock_cred
```

### Async Testing

Use `pytest-asyncio` for async tests:

```python
import pytest

@pytest.mark.asyncio
async def test_async_function():
    """Test async function."""
    result = await some_async_function()
    assert result is not None
```

### Testing Exceptions

```python
def test_function_raises_error_on_invalid_input():
    """Test that function raises ValueError for invalid input."""
    with pytest.raises(ValueError, match="Invalid input"):
        some_function(invalid_input)
```

### Parametrized Tests

```python
@pytest.mark.parametrize("input_value,expected", [
    (1, "one"),
    (2, "two"),
    (3, "three"),
])
def test_function_with_various_inputs(input_value, expected):
    """Test function with multiple input/output pairs."""
    result = some_function(input_value)
    assert result == expected
```

## Test Markers

Use pytest markers to categorize tests:

```python
@pytest.mark.unit
def test_unit_test():
    """Unit test."""
    pass

@pytest.mark.integration
def test_integration_test():
    """Integration test."""
    pass

@pytest.mark.slow
def test_slow_test():
    """Slow running test."""
    pass

@pytest.mark.asyncio
async def test_async_test():
    """Async test."""
    pass
```

Run specific test categories:
```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Exclude slow tests
pytest -m "not slow"
```

## Coverage Requirements

- **Minimum Coverage**: 80% overall
- **Critical Modules**: 90%+ coverage
  - `src/utils/config.py`
  - `src/consumers/eventhub.py`
  - `src/pipeline/orchestrator.py`
  - `src/streaming/snowflake_high_performance.py`

Run coverage:
```bash
pytest --cov=src --cov-report=html --cov-report=term
```

## Common Testing Scenarios

### Testing Configuration Loading

```python
def test_load_config_with_valid_env_returns_config(monkeypatch):
    """Test loading config with valid environment variables."""
    monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
    monkeypatch.setenv("EVENTHUBNAME_1", "test-hub")
    monkeypatch.setenv("EVENTHUBNAME_1_CONSUMER_GROUP", "test-group")
    
    config = load_config()
    
    assert config.eventhub_namespace == "test.servicebus.windows.net"
    assert "EVENTHUBNAME_1" in config.event_hubs
```

### Testing EventHub Consumer

```python
@pytest.mark.asyncio
async def test_consumer_processes_messages_successfully(
    mock_eventhub_client,
    mock_snowflake_connection,
    sample_event_data
):
    """Test that consumer processes EventHub messages."""
    # Setup
    consumer = EventHubAsyncConsumer(
        eventhub_config=config,
        target_db="TEST_DB",
        target_schema="TEST_SCHEMA",
        target_table="TEST_TABLE",
        message_processor=lambda msgs: True
    )
    
    # Execute
    await consumer.start()
    
    # Verify
    assert consumer.running
    mock_eventhub_client.receive.assert_called_once()
```

### Testing Retry Logic

```python
def test_smart_retry_with_retryable_error_retries(mock_llm_analyzer):
    """Test that smart retry retries on retryable errors."""
    # Setup mock LLM to return "should retry"
    mock_llm_analyzer.analyze_exception.return_value = RetryDecision(
        should_retry=True,
        reasoning="Transient network error",
        confidence=0.9
    )
    
    retry_manager = RetryManager(smart_enabled=True, max_attempts=3)
    
    # Test function that fails then succeeds
    attempt_count = 0
    
    @retry_manager.get_retry_decorator()
    def failing_function():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 2:
            raise ConnectionError("Network timeout")
        return "success"
    
    # Execute
    result = failing_function()
    
    # Verify
    assert result == "success"
    assert attempt_count == 2
```

## Test Data Fixtures

Create reusable test data in `conftest.py`:

```python
@pytest.fixture
def sample_eventhub_config():
    """Sample EventHub configuration."""
    return EventHubConfig(
        name="test-hub",
        namespace="test.servicebus.windows.net",
        consumer_group="test-group",
        max_batch_size=100,
        max_wait_time=30,
        prefetch_count=50
    )

@pytest.fixture
def sample_snowflake_config():
    """Sample Snowflake configuration."""
    return SnowflakeConfig(
        database="TEST_DB",
        schema_name="TEST_SCHEMA",
        table_name="TEST_TABLE",
        batch_size=1000
    )

@pytest.fixture
def sample_event_message():
    """Sample EventHub event message."""
    from azure.eventhub import EventData
    
    event = EventData(b'{"key": "value"}')
    event.sequence_number = 12345
    event.offset = "67890"
    event.enqueued_time = datetime.now(UTC)
    
    return event
```

## CI/CD Integration

Tests should run automatically in CI/CD:

```yaml
# .github/workflows/tests.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.13'
      - name: Install dependencies
        run: |
          pip install uv
          uv sync --all-groups
      - name: Run tests
        run: |
          pytest --cov=src --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

## Best Practices

1. **Test One Thing**: Each test should verify one specific behavior
2. **Arrange-Act-Assert**: Structure tests with clear setup, execution, and verification sections
3. **Use Descriptive Names**: Test names should explain what is being tested
4. **Mock External Dependencies**: Never hit real services in tests
5. **Keep Tests Fast**: Unit tests should run in <1s, integration tests in <10s
6. **Clean Up Resources**: Use fixtures and context managers to ensure cleanup
7. **Test Edge Cases**: Cover error conditions, boundary values, and edge cases
8. **Avoid Test Interdependence**: Tests should not depend on each other
9. **Use Fixtures for Common Setup**: Share setup code via pytest fixtures
10. **Document Complex Tests**: Add docstrings explaining complex test scenarios

## Example Test File Structure

```python
"""
Tests for EventHub async consumer.

This module tests the EventHub consumer functionality including:
- Consumer initialization and lifecycle
- Message batching and processing
- Checkpoint management
- Error handling and retries
"""

import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock

from consumers.eventhub import (
    EventHubAsyncConsumer,
    EventHubMessage,
    MessageBatch,
    SnowflakeCheckpointManager,
)
from utils.config import EventHubConfig


class TestEventHubAsyncConsumer:
    """Tests for EventHubAsyncConsumer class."""
    
    @pytest.mark.asyncio
    async def test_start_initializes_consumer_successfully(
        self,
        mock_eventhub_client,
        mock_snowflake_connection,
        sample_eventhub_config
    ):
        """Test that consumer starts and initializes correctly."""
        # Arrange
        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=lambda msgs: True
        )
        
        # Act
        await consumer.start()
        
        # Assert
        assert consumer.running
        assert consumer.client is not None
        mock_eventhub_client.receive.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_stop_cleans_up_resources(
        self,
        mock_eventhub_client,
        sample_eventhub_config
    ):
        """Test that consumer stops and cleans up resources."""
        # Arrange
        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="TEST_DB",
            target_schema="TEST_SCHEMA",
            target_table="TEST_TABLE",
            message_processor=lambda msgs: True
        )
        await consumer.start()
        
        # Act
        await consumer.stop()
        
        # Assert
        assert not consumer.running
        assert consumer.client is None
        mock_eventhub_client.close.assert_called_once()


class TestMessageBatch:
    """Tests for MessageBatch class."""
    
    def test_add_message_returns_true_when_batch_full(self, sample_event_message):
        """Test that adding messages returns True when batch is full."""
        # Arrange
        batch = MessageBatch(max_size=2, max_wait_seconds=60)
        message1 = EventHubMessage(sample_event_message, "0", 1)
        message2 = EventHubMessage(sample_event_message, "0", 2)
        
        # Act
        result1 = batch.add_message(message1)
        result2 = batch.add_message(message2)
        
        # Assert
        assert not result1  # First message doesn't fill batch
        assert result2      # Second message fills batch
        assert len(batch.messages) == 2


# More test classes...
```

## References

- [pytest documentation](https://docs.pytest.org/)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)
- [pytest-mock](https://pytest-mock.readthedocs.io/)
- [Python unittest.mock](https://docs.python.org/3/library/unittest.mock.html)
