# EvSnow Testing

## Overview

This directory contains the test suite for the EvSnow project (EventHub to Snowflake streaming pipeline). After the major refactoring from Motherduck to Snowflake, all tests have been redesigned to use mocks instead of real services.

## Quick Start

### Running Tests

```bash
# Run all tests
pytest

# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Run with coverage
pytest --cov=src --cov-report=html --cov-report=term

# Run specific test file
pytest tests/unit/test_config.py

# Run tests matching pattern
pytest -k "test_load_config"
```

### Test Structure

```
tests/
├── README.md                    # This file
├── conftest.py                  # Shared fixtures and mocks
├── unit/                        # Unit tests
│   ├── __init__.py
│   └── test_*.py               # Unit test files
└── integration/                 # Integration tests
    ├── __init__.py
    └── test_*_integration.py   # Integration test files
```

## Documentation

### Primary Documents

1. **[TESTING_STANDARDS.md](../TESTING_STANDARDS.md)** - Complete testing standards and guidelines
   - Testing philosophy and principles
   - Naming conventions
   - Mocking patterns for all services
   - Best practices and examples

2. **[TEST_ISSUES.md](../TEST_ISSUES.md)** - GitHub issue templates for writing tests
   - Issue templates for each major source file
   - Test requirements and scenarios
   - Acceptance criteria
   - Priority ordering

## Writing Tests

### Test Naming Convention

Follow this pattern: `test_<function>_<scenario>_<expected_result>`

**Examples:**
```python
def test_load_config_with_valid_env_returns_config()
def test_eventhub_consumer_start_without_credentials_raises_error()
def test_batch_ready_when_size_exceeded_returns_true()
```

### Using Fixtures

All common mocks are available in `conftest.py`:

```python
def test_example(
    mock_snowflake_connection,
    mock_eventhub_client,
    mock_logfire,
    sample_eventhub_config
):
    """Test using shared fixtures."""
    # Your test code here
    pass
```

### Async Tests

Use `pytest.mark.asyncio` for async tests:

```python
import pytest

@pytest.mark.asyncio
async def test_async_function(mock_eventhub_client):
    """Test async function."""
    result = await some_async_function()
    assert result is not None
```

### Mocking Services

**Never call real services!** All external services must be mocked:

```python
def test_with_mocks(mocker):
    """Example of mocking Snowflake connection."""
    # Mock Snowflake
    mock_conn = mocker.MagicMock()
    mocker.patch("utils.snowflake.get_connection", return_value=mock_conn)
    
    # Your test code
    pass
```

## Test Requirements by Module

### Priority High (Critical - Write First)

1. **CLI Application** (`src/main.py`)
   - All Typer commands
   - Error handling
   - Configuration validation

2. **EventHub Consumer** (`src/consumers/eventhub.py`)
   - Message processing
   - Checkpoint management
   - Async operations

3. **Pipeline Orchestrator** (`src/pipeline/orchestrator.py`)
   - Multiple mappings
   - Concurrent processing
   - Graceful shutdown

4. **Snowflake Client** (`src/streaming/snowflake_high_performance.py`)
   - Connection management
   - Data ingestion
   - Channel operations

5. **Configuration** (`src/utils/config.py`)
   - Environment parsing
   - Validation logic
   - Pydantic models

6. **Snowflake Utils** (`src/utils/snowflake.py`)
   - Connection utilities
   - Checkpoint operations
   - SQL operations

### Priority Medium

7. **Smart Retry** (`src/utils/smart_retry.py`)
   - LLM integration
   - Retry decorators
   - Decision caching

8. **Streaming Modules** (`src/streaming/`)
   - Factory functions
   - Base classes
   - Facade pattern

9. **Integration Tests**
   - End-to-end flow
   - Error recovery
   - Multi-mapping scenarios

## Coverage Goals

- **Overall Coverage**: >80%
- **Critical Modules**: >90%
  - `src/utils/config.py`
  - `src/consumers/eventhub.py`
  - `src/pipeline/orchestrator.py`
  - `src/streaming/snowflake_high_performance.py`

Check coverage:
```bash
pytest --cov=src --cov-report=term-missing
```

## Test Markers

Use markers to categorize tests:

```python
@pytest.mark.unit          # Unit test
@pytest.mark.integration   # Integration test
@pytest.mark.slow          # Slow running test
@pytest.mark.asyncio       # Async test
```

Run specific categories:
```bash
pytest -m unit              # Only unit tests
pytest -m integration       # Only integration tests
pytest -m "not slow"        # Exclude slow tests
```

## Common Issues

### Import Errors

If you get import errors, ensure you're running pytest from the project root:
```bash
cd /home/runner/work/evsnow/evsnow
pytest
```

### Async Test Failures

Make sure to:
1. Mark tests with `@pytest.mark.asyncio`
2. Use `pytest-asyncio` plugin
3. Mock async methods with `mocker.AsyncMock()`

### Mock Not Working

Check that you're patching the correct import path:
```python
# Wrong - patches the module
mocker.patch("snowflake.connector.connect")

# Right - patches the import location
mocker.patch("utils.snowflake.get_connection")
```

## Contributing Tests

### Before Writing Tests

1. Read [TESTING_STANDARDS.md](../TESTING_STANDARDS.md)
2. Find your module's issue in [TEST_ISSUES.md](../TEST_ISSUES.md)
3. Review existing fixtures in `conftest.py`
4. Check similar test files for patterns

### Test Checklist

- [ ] Test file follows naming convention (`test_<module>.py`)
- [ ] Test functions follow naming pattern
- [ ] All external services are mocked
- [ ] Async tests use `@pytest.mark.asyncio`
- [ ] Tests are independent (no shared state)
- [ ] Tests are fast (<1s for unit, <10s for integration)
- [ ] Edge cases and errors are tested
- [ ] Docstrings explain complex scenarios
- [ ] Coverage meets requirements

### Submitting Tests

1. Write tests following the standards
2. Run tests locally: `pytest -v`
3. Check coverage: `pytest --cov=src`
4. Run linting: `ruff check src tests`
5. Commit with clear message
6. Create PR linking to the test issue

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)
- [pytest-mock](https://pytest-mock.readthedocs.io/)
- [unittest.mock](https://docs.python.org/3/library/unittest.mock.html)
- [TESTING_STANDARDS.md](../TESTING_STANDARDS.md) - Project testing guidelines
- [TEST_ISSUES.md](../TEST_ISSUES.md) - Test work breakdown

## Questions?

- Check [TESTING_STANDARDS.md](../TESTING_STANDARDS.md) for detailed examples
- Review existing test files in similar projects
- Ask in team chat or code review
- Create a discussion in GitHub Discussions

---

**Remember:** All tests must use mocks. No real Snowflake, EventHub, Azure, or Logfire connections!
