# Test Orchestrator Note

## Current Status

The comprehensive test suite for `src/pipeline/orchestrator.py` has been created in `test_orchestrator.py`.

## Dependencies Required

To run these tests, the following dependencies must be installed:
- pytest
- pytest-asyncio  
- pytest-mock
- pydantic
- pydantic-settings
- azure-eventhub
- azure-identity
- logfire
- snowflake-connector-python
- snowpipe-streaming

## Test Coverage

The test file includes comprehensive coverage for:

### PipelineMapping (24 tests)
- Initialization with valid/invalid configurations
- Component startup (EventHub, Snowflake)
- Message processing and batch ingestion
- Statistics tracking
- Health checks
- Graceful shutdown

### PipelineOrchestrator (14 tests)
- Initialization and mapping creation
- Multi-mapping orchestration
- Async task management
- Signal handling (SIGINT/SIGTERM)
- Graceful shutdown
- Statistics aggregation
- Health monitoring

### run_pipeline() (6 tests)
- Pipeline creation and configuration
- Signal handler setup
- Error handling (CancelledError, KeyboardInterrupt, generic exceptions)
- Resource cleanup
- Retry manager integration

## Running Tests

Once dependencies are installed:

```bash
# Run all orchestrator tests
pytest tests/unit/test_orchestrator.py -v

# Run with coverage
pytest tests/unit/test_orchestrator.py --cov=src/pipeline --cov-report=term

# Run specific test class
pytest tests/unit/test_orchestrator.py::TestPipelineMapping -v
```

## Test Design Principles

All tests follow TESTING_STANDARDS.md:
- No real services called (all mocked)
- Fast execution (<10 seconds total)
- Isolated and independent tests
- Descriptive test names
- Proper async/await patterns
- Comprehensive error handling coverage
