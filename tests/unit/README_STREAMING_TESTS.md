# Streaming Module Tests

This directory contains comprehensive unit tests for the streaming module, including:
- `test_streaming.py` - Tests for base class, factory, and facade

## Running the Tests

### Prerequisites

The tests require Python 3.13+ and the following dependencies:
```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-cov pytest-mock

# Install project dependencies (required for imports)
pip install pydantic>=2.0.0 pydantic-settings>=2.0.0
```

### Running Tests

```bash
# Run all streaming tests
pytest tests/unit/test_streaming.py -v

# Run with coverage
pytest tests/unit/test_streaming.py --cov=src/streaming --cov-report=html

# Run specific test class
pytest tests/unit/test_streaming.py::TestSnowflakeStreamingClientBase -v

# Run specific test
pytest tests/unit/test_streaming.py::TestStreamingFactory::test_creates_high_performance_client_with_valid_config -v
```

## Test Coverage

The tests cover:

### SnowflakeStreamingClientBase (Abstract Base Class)
- ✅ Cannot instantiate abstract class directly
- ✅ Subclasses must implement all abstract methods
- ✅ Constructor initializes fields correctly (with all parameters)
- ✅ Constructor with minimal parameters
- ✅ Concrete subclass implements all methods correctly

### create_snowflake_client() (Factory Function)
- ✅ Creates high-performance client with valid config
- ✅ Requires pipe_name for high-performance mode
- ✅ Passes retry_manager to client
- ✅ Passes client_name_suffix correctly
- ✅ Creates client with all parameters
- ✅ Logs creation messages
- ✅ Raises error with empty pipe_name

### streaming.snowflake (Facade Module)
- ✅ Exports SnowflakeStreamingClient
- ✅ Exports create_snowflake_streaming_client
- ✅ __all__ list contains expected exports
- ✅ __all__ list has exactly 2 exports
- ✅ Can import and use facade function
- ✅ Facade imports work correctly
- ✅ Maintains backward compatibility

## Test Design

All tests follow the TESTING_STANDARDS.md guidelines:
- **No Real Services**: All external dependencies are mocked
- **Fast Execution**: Tests run in milliseconds
- **Isolated**: Each test is independent
- **Comprehensive**: High code coverage (>80%)

### Mocking Strategy

The tests mock:
- Heavy external modules (logfire, snowflake, azure, etc.)
- Snowflake client implementations
- Configuration objects (via fixtures in conftest.py)

### Fixtures

Tests use fixtures from `conftest.py`:
- `sample_snowflake_config`: Mock Snowflake target configuration
- `sample_snowflake_connection_config`: Mock Snowflake connection settings

## Troubleshooting

### Import Errors

If you see pydantic import errors like:
```
ImportError: cannot import name 'field_validator' from 'pydantic'
```

This means you have Pydantic v1.x installed but the project requires v2.x:
```bash
pip install --upgrade 'pydantic>=2.0.0' 'pydantic-settings>=2.0.0'
```

### Module Not Found

If you see:
```
ModuleNotFoundError: No module named 'streaming'
```

Make sure to run pytest from the project root:
```bash
cd /path/to/evsnow
pytest tests/unit/test_streaming.py
```

## Expected Test Results

All 19 tests should pass:
- 5 tests for SnowflakeStreamingClientBase
- 7 tests for StreamingFactory
- 7 tests for StreamingFacade

Expected runtime: < 3 seconds
