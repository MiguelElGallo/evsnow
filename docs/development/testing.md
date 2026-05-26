# EvSnow Testing

## Overview

This guide covers the EvSnow test suite for the Event Hub to Snowflake
streaming pipeline. Unit and integration tests use mocks for external services;
live Snowflake checks belong in the quickstart harness, not in pytest.

## Quick Start

### Running Tests

```bash
# Run all tests
uv run pytest

# Run only unit tests
uv run pytest tests/unit/

# Run only integration tests
uv run pytest tests/integration/

# Run with coverage
uv run pytest --cov=src --cov-report=html --cov-report=term

# Run specific test file
uv run pytest tests/unit/test_config.py

# Run tests matching pattern
uv run pytest -k "test_load_config"
```

### Test Structure

```
tests/
├── README.md                    # Short repository test overview
├── conftest.py                  # Shared fixtures and mocks
├── benchmarks/                  # CodSpeed benchmark tests
├── unit/                        # Unit tests
│   ├── __init__.py
│   └── test_*.py               # Unit test files
└── integration/                 # Integration tests
    ├── __init__.py
    └── test_*_integration.py   # Integration test files
```

## Documentation

### Primary Documents

1. **This testing guide** - Complete testing standards and guidelines
   - Testing philosophy and principles
   - Naming conventions
   - Mocking patterns for all services
   - Best practices and examples

2. **[GitHub Actions workflows](../project/workflows.md)** - CI and local parity
   commands for tests, docs, and the Snowflake quickstart harness

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
uv run pytest --cov=src --cov-report=term-missing
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
uv run pytest tests/unit/          # Only unit tests
uv run pytest tests/integration/   # Only integration tests
uv run pytest -m "not slow"        # Exclude slow tests
```

CI selects unit and integration tests by directory, not by `unit` or
`integration` markers. Use markers for extra filtering such as `slow`.

## Common Issues

### Import Errors

If you get import errors, ensure you're running pytest from the project root:
```bash
cd /home/runner/work/evsnow/evsnow
uv run pytest
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

1. Read testing guidance on this page.
2. Review existing fixtures in `tests/conftest.py` and the relevant
   `tests/unit/` or `tests/integration/` file.
3. Keep service calls mocked unless you are extending the documented
   quickstart harness.
4. Check similar test files for patterns before adding new fixtures.

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
2. Run tests locally: `uv run pytest -v`
3. Check coverage when behavior changes: `uv run pytest --cov=src`
4. Run linting: `uv run ruff check src tests`
5. Commit with a clear message and include validation output in the PR notes

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)
- [pytest-mock](https://pytest-mock.readthedocs.io/)
- [unittest.mock](https://docs.python.org/3/library/unittest.mock.html)
- [GitHub Actions workflows](../project/workflows.md) - CI and local parity

## Questions?

- Check testing guidance on this page for detailed examples
- Review existing test files in similar projects
- Ask in team chat or code review
- Create a discussion in GitHub Discussions

---

**Remember:** All tests must use mocks. No real Snowflake, EventHub, Azure, or Logfire connections!
