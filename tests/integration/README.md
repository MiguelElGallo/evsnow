# Integration Tests

This directory contains integration tests for the EvSnow pipeline that verify end-to-end functionality using mocks for all external services.

## Overview

Integration tests verify the complete pipeline flow from EventHub to Snowflake, including:
- EventHub message consumption
- Message batching and processing
- Snowflake data ingestion
- Checkpoint management
- Error recovery
- Multi-mapping concurrent processing
- Configuration loading and validation

## Test Files

### `test_pipeline_integration.py`

Comprehensive integration tests covering the complete pipeline flow.

**Test Classes:**

1. **TestEndToEndPipeline** (6 tests)
   - Complete pipeline flow with all components
   - Message batching by size and timeout
   - Snowflake ingestion with data validation
   - Checkpoint persistence
   - Pipeline statistics tracking

2. **TestMultiMappingPipeline** (3 tests)
   - Multiple concurrent mappings
   - Independent mapping operation
   - State isolation between mappings

3. **TestErrorRecovery** (4 tests)
   - Transient error retry logic
   - Fatal error handling
   - Checkpoint preservation on errors
   - Graceful shutdown with message processing

4. **TestConfigurationFlow** (6 tests)
   - Configuration loading from environment
   - Configuration validation
   - Mapping creation from config
   - Component initialization
   - Multiple mapping support

## Running Tests

### Run All Integration Tests

```bash
pytest tests/integration/ -v
```

### Run Specific Test Class

```bash
pytest tests/integration/test_pipeline_integration.py::TestEndToEndPipeline -v
```

### Run Specific Test

```bash
pytest tests/integration/test_pipeline_integration.py::TestEndToEndPipeline::test_complete_pipeline_flow_with_all_components -v
```

### Run with Coverage

```bash
pytest tests/integration/ --cov=src --cov-report=html --cov-report=term
```

### Run Only Integration Tests (Using Markers)

```bash
pytest -m integration -v
```

## Test Markers

All integration tests are marked with `@pytest.mark.integration` and async tests with `@pytest.mark.asyncio`.

Available markers:
- `integration`: Integration tests
- `asyncio`: Async tests
- `unit`: Unit tests (in `tests/unit/`)
- `slow`: Slow-running tests

## Mocking Strategy

All integration tests use mocks following `TESTING_STANDARDS.md`:

### Mocked Services
- **Azure EventHub SDK**: `EventHubConsumerClient`, `EventData`, `PartitionContext`
- **Azure Identity**: `DefaultAzureCredential`, `AzureCliCredential`
- **Snowflake SDK**: `StreamingIngestClient`, connections, cursors, sessions
- **Logfire**: All observability functions (configure, info, error, span)
- **Custom Components**: `SnowflakeCheckpointStore`, `SnowflakeCheckpointManager`

### Mock Fixtures

Common mock fixtures are defined in `tests/conftest.py`:
- `mock_eventhub_client`: Mocked EventHub client
- `mock_azure_credential`: Mocked Azure credentials
- `mock_snowflake_connection`: Mocked Snowflake connection
- `mock_logfire`: Mocked Logfire observability
- `sample_eventhub_config`: Sample EventHub configuration
- `sample_snowflake_config`: Sample Snowflake configuration
- `sample_eventhub_messages`: Sample test messages

## Test Design Principles

1. **No Real Services**: All tests use mocks, no external dependencies
2. **Fast Execution**: Tests complete in <30 seconds total
3. **Isolated**: Each test is independent and can run standalone
4. **Comprehensive**: Tests cover success paths, error paths, and edge cases
5. **Realistic**: Mocks simulate real service behavior accurately
6. **Clear**: Extensive documentation and descriptive test names

## Test Data

Test data (configurations, messages, events) is created using fixtures from `conftest.py`:

```python
# Example: Using fixtures in tests
async def test_example(
    mock_eventhub_client,
    mock_snowflake_connection,
    sample_eventhub_messages,
):
    # Test implementation using fixtures
    pass
```

## Expected Behavior

### Successful Tests
All tests should pass and complete in <30 seconds total. Each test verifies specific integration scenarios using assertions.

### Test Failures
If tests fail:
1. Check that all fixtures are properly configured in `conftest.py`
2. Verify mock setup in failing test
3. Check for missing dependencies (pytest, pytest-asyncio, pytest-mock)
4. Review error messages for assertion failures

## Adding New Integration Tests

When adding new integration tests:

1. **Follow Naming Convention**: `test_<feature>_<scenario>_<expected_result>`
2. **Use Fixtures**: Reuse fixtures from `conftest.py`
3. **Add Docstrings**: Explain what the test verifies
4. **Mock External Services**: Never call real services
5. **Mark Tests**: Use `@pytest.mark.integration` and `@pytest.mark.asyncio` (for async tests)
6. **Keep Tests Fast**: Each test should complete in <5 seconds
7. **Verify Independently**: Tests should not depend on each other

Example:
```python
@pytest.mark.integration
@pytest.mark.asyncio
async def test_new_feature_with_valid_input_succeeds(
    mock_eventhub_client,
    mock_snowflake_connection,
    sample_eventhub_config,
):
    """
    Test that new feature succeeds with valid input.
    
    Verifies:
    - Feature initializes correctly
    - Processing completes successfully
    - Expected results are produced
    """
    # Arrange
    # ... setup test data and mocks
    
    # Act
    # ... execute the feature
    
    # Assert
    # ... verify expected behavior
```

## Troubleshooting

### Import Errors

If you see import errors, ensure all dependencies are installed:

```bash
pip install pytest pytest-asyncio pytest-mock pytest-cov
pip install azure-eventhub azure-identity pydantic pydantic-settings
```

### Async Errors

For async test issues, verify:
1. `pytest-asyncio` is installed
2. Tests are marked with `@pytest.mark.asyncio`
3. `asyncio_mode = "strict"` is set in `pytest.ini`

### Mock Issues

If mocks aren't working:
1. Check that `pytest-mock` is installed
2. Verify mock patches target correct import paths
3. Use `mocker` fixture from `pytest-mock`

## Performance

Integration tests are designed to be fast:
- **Target**: <30 seconds for all tests
- **Individual Tests**: <5 seconds each
- **Parallelization**: Tests can run in parallel with `pytest-xdist`

Run tests in parallel:
```bash
pytest tests/integration/ -n auto
```

## CI/CD Integration

These tests are designed to run in CI/CD pipelines:
- No external service dependencies
- Fast execution
- Deterministic results
- Clear failure messages

Example GitHub Actions workflow:
```yaml
- name: Run Integration Tests
  run: |
    pytest tests/integration/ -v --cov=src --cov-report=xml
```

## References

- [TESTING_STANDARDS.md](../../TESTING_STANDARDS.md) - Project testing standards
- [pytest documentation](https://docs.pytest.org/)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)
- [pytest-mock](https://pytest-mock.readthedocs.io/)
