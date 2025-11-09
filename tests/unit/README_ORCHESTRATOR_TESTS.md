# Pipeline Orchestrator Tests

## Overview

Comprehensive unit test suite for `src/pipeline/orchestrator.py` covering all classes and functions with extensive mocking of external dependencies.

## Test File

- **Location**: `tests/unit/test_orchestrator.py`
- **Test Count**: 42 test methods across 3 test classes
- **Lines of Code**: ~1,200 lines

## Test Classes

### 1. TestPipelineMapping (19 tests)

Tests for the `PipelineMapping` class that manages a single EventHub->Snowflake mapping.

**Test Coverage:**
- `test_init_with_valid_config_creates_mapping` - Validates successful initialization
- `test_init_with_invalid_eventhub_key_raises_error` - Error handling for bad EventHub key
- `test_init_with_invalid_snowflake_key_raises_error` - Error handling for bad Snowflake key
- `test_start_initializes_components_successfully` - Component startup verification
- `test_start_when_already_running_logs_warning` - Duplicate start detection
- `test_start_without_snowflake_connection_raises_error` - Missing connection validation
- `test_start_cleans_up_on_error` - Error recovery and cleanup
- `test_start_async_starts_eventhub_consumer` - Async component initialization
- `test_start_async_without_start_raises_error` - Pre-condition validation
- `test_process_messages_ingests_to_snowflake_successfully` - Successful message processing
- `test_process_messages_without_snowflake_client_returns_false` - Missing client handling
- `test_process_messages_handles_ingest_failure` - Ingestion failure recovery
- `test_process_messages_handles_exception` - Exception handling in processing
- `test_stop_cleans_up_resources` - Graceful shutdown
- `test_stop_when_not_running_does_nothing` - Idempotent stop
- `test_get_stats_returns_correct_statistics` - Statistics calculation
- `test_get_stats_without_started_excludes_runtime` - Statistics before start
- `test_health_check_returns_status` - Health check reporting
- `test_health_check_reports_missing_components` - Health check with missing components

### 2. TestPipelineOrchestrator (17 tests)

Tests for the `PipelineOrchestrator` class that manages multiple mappings.

**Test Coverage:**
- `test_init_creates_orchestrator` - Orchestrator initialization
- `test_initialize_creates_all_mappings` - Single mapping creation
- `test_initialize_with_multiple_mappings` - Multiple mapping creation
- `test_initialize_raises_error_on_mapping_failure` - Initialization error handling
- `test_start_initializes_and_sets_running` - Orchestrator startup
- `test_start_when_already_running_logs_warning` - Duplicate start detection
- `test_run_async_starts_all_mapping_tasks` - Async task creation
- `test_run_async_without_start_raises_error` - Pre-condition validation
- `test_run_async_handles_cancelled_error` - Cancellation handling
- `test_stop_cancels_all_tasks` - Task cancellation
- `test_stop_cleans_up_all_mappings` - Complete cleanup
- `test_stop_when_not_running_does_nothing` - Idempotent stop
- `test_get_stats_aggregates_mapping_stats` - Statistics aggregation
- `test_health_check_aggregates_mapping_health` - Health aggregation
- `test_setup_signal_handlers_registers_handlers` - Signal handler registration
- `test_signal_handler_cancels_tasks_on_first_signal` - Graceful shutdown signal
- `test_signal_handler_forces_exit_on_second_signal` - Force exit on repeated signal

### 3. TestRunPipeline (6 tests)

Tests for the `run_pipeline()` function - main entry point.

**Test Coverage:**
- `test_run_pipeline_creates_orchestrator` - Orchestrator creation and lifecycle
- `test_run_pipeline_handles_cancelled_error` - Cancellation handling
- `test_run_pipeline_handles_keyboard_interrupt` - Keyboard interrupt handling
- `test_run_pipeline_handles_generic_exception` - Generic exception handling
- `test_run_pipeline_always_calls_stop` - Cleanup in finally block
- `test_run_pipeline_with_retry_manager` - Retry manager integration

## Dependencies

The tests require the following dependencies to run:

```bash
pip install pytest pytest-asyncio pytest-mock pytest-cov
pip install pydantic pydantic-settings
pip install azure-eventhub azure-identity
pip install logfire
pip install snowflake-connector-python snowpipe-streaming
```

Or using the project's dependency management:

```bash
uv sync --all-groups  # Install all dependencies including dev/test
```

## Running the Tests

### Run All Orchestrator Tests

```bash
pytest tests/unit/test_orchestrator.py -v
```

### Run Specific Test Class

```bash
# Test PipelineMapping only
pytest tests/unit/test_orchestrator.py::TestPipelineMapping -v

# Test PipelineOrchestrator only  
pytest tests/unit/test_orchestrator.py::TestPipelineOrchestrator -v

# Test run_pipeline() only
pytest tests/unit/test_orchestrator.py::TestRunPipeline -v
```

### Run Specific Test

```bash
pytest tests/unit/test_orchestrator.py::TestPipelineMapping::test_start_initializes_components_successfully -v
```

### Run with Coverage

```bash
# Generate coverage report
pytest tests/unit/test_orchestrator.py --cov=src/pipeline/orchestrator --cov-report=term-missing

# Generate HTML coverage report
pytest tests/unit/test_orchestrator.py --cov=src/pipeline/orchestrator --cov-report=html

# View HTML report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

### Run Async Tests Only

```bash
pytest tests/unit/test_orchestrator.py -m asyncio -v
```

## Test Design Principles

All tests follow the project's `TESTING_STANDARDS.md`:

1. **No Real Services**: All external dependencies (EventHub, Snowflake, Logfire, Azure services) are mocked
2. **Fast Execution**: Tests run in milliseconds, targeting <10 seconds for the entire suite
3. **Isolated**: Each test is independent with no shared state
4. **Descriptive Names**: Test names follow `test_<function>_<scenario>_<expected_result>` pattern
5. **Async Support**: Proper use of `pytest-asyncio` for async function testing
6. **Comprehensive**: Covers success paths, error paths, edge cases, and cleanup

## Mocking Strategy

The tests use `pytest-mock` (wrapper around `unittest.mock`) to mock:

### External Services
- **EventHub**: `EventHubAsyncConsumer` mocked with AsyncMock for async operations
- **Snowflake**: `SnowflakeStreamingClient` and `create_snowflake_streaming_client` factory
- **Logfire**: Observability spans and logging

### Async Operations
- `asyncio.gather` for task coordination
- `asyncio.create_task` for task creation
- Event loop signal handlers

### System Calls
- `signal.SIGINT` and `signal.SIGTERM` handlers
- `sys.exit` for forced shutdown

## Expected Coverage

The test suite aims for >85% code coverage of `src/pipeline/orchestrator.py`:

- **PipelineMapping**: ~95% coverage (all public methods and critical paths)
- **PipelineOrchestrator**: ~90% coverage (including signal handling edge cases)
- **run_pipeline()**: ~95% coverage (all exception paths)

## Fixtures

The test file defines local fixtures for:

- `mock_eventhub_consumer`: Mocked EventHub consumer with async operations
- `mock_snowflake_client`: Mocked Snowflake streaming client
- `mock_create_snowflake_client`: Mocked factory function
- `complete_pipeline_config`: Complete EvSnowConfig for testing

Additional fixtures are imported from `tests/conftest.py`:

- `sample_eventhub_config`: EventHub configuration
- `sample_snowflake_config`: Snowflake configuration
- `sample_snowflake_connection_config`: Connection settings
- `sample_mapping`: EventHub->Snowflake mapping
- `sample_eventhub_messages`: Sample message batch
- `mock_logfire`: Mocked Logfire spans

## Test Scenarios Covered

### Initialization & Configuration
- ✅ Valid configuration handling
- ✅ Invalid EventHub key detection
- ✅ Invalid Snowflake key detection
- ✅ Missing connection configuration

### Component Lifecycle
- ✅ Component startup
- ✅ Component shutdown
- ✅ Cleanup on errors
- ✅ Idempotent operations

### Message Processing
- ✅ Successful message ingestion
- ✅ Batch processing
- ✅ Ingestion failures
- ✅ Exception handling
- ✅ Statistics tracking

### Async Operations
- ✅ Task creation and management
- ✅ Task cancellation
- ✅ CancelledError handling
- ✅ Async context managers

### Signal Handling
- ✅ SIGINT registration
- ✅ SIGTERM registration
- ✅ Graceful shutdown on first signal
- ✅ Forced exit on second signal

### Error Recovery
- ✅ Component initialization failures
- ✅ Message processing exceptions
- ✅ KeyboardInterrupt handling
- ✅ Generic exception handling
- ✅ Cleanup in finally blocks

### Statistics & Monitoring
- ✅ Message/batch counters
- ✅ Runtime calculations
- ✅ Throughput metrics
- ✅ Health check reporting
- ✅ Statistics aggregation

## Continuous Integration

These tests are designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run Orchestrator Tests
  run: |
    pytest tests/unit/test_orchestrator.py \
      --cov=src/pipeline/orchestrator \
      --cov-report=xml \
      --cov-report=term \
      -v
```

## Known Limitations

1. **Network Dependencies**: Tests require network access to install dependencies but not to run
2. **Python Version**: Requires Python 3.13+ as per project requirements
3. **Async Runtime**: Requires `asyncio` support (standard in Python 3.7+)

## Troubleshooting

### Import Errors

If you see import errors:
```bash
# Ensure you're in the project root
cd /path/to/evsnow

# Install dependencies
uv sync --all-groups

# Run tests
pytest tests/unit/test_orchestrator.py -v
```

### Async Test Failures

If async tests fail with event loop errors:
```bash
# Ensure pytest-asyncio is installed
pip install pytest-asyncio

# Check pytest configuration
cat pyproject.toml | grep asyncio_mode
# Should show: asyncio_mode = "strict"
```

### Coverage Not Generated

If coverage reports aren't generated:
```bash
# Ensure pytest-cov is installed
pip install pytest-cov

# Run with explicit coverage settings
pytest tests/unit/test_orchestrator.py \
  --cov=src/pipeline/orchestrator \
  --cov-branch \
  --cov-report=term-missing
```

## Contributing

When adding new tests:

1. Follow the naming convention: `test_<function>_<scenario>_<expected>`
2. Add docstrings explaining what is being tested
3. Use appropriate fixtures from `conftest.py`
4. Mock all external dependencies
5. Test both success and failure paths
6. Ensure tests are isolated and independent

## References

- [TESTING_STANDARDS.md](../../TESTING_STANDARDS.md) - Project testing standards
- [pytest documentation](https://docs.pytest.org/)
- [pytest-asyncio documentation](https://pytest-asyncio.readthedocs.io/)
- [pytest-mock documentation](https://pytest-mock.readthedocs.io/)
- [unittest.mock documentation](https://docs.python.org/3/library/unittest.mock.html)
