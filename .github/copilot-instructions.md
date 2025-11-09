# Copilot Instructions for EvSnow

## Project Overview

EvSnow is an Azure Event Hub to Snowflake streaming pipeline built with Python 3.13. The project provides a CLI tool for continuously reading from Event Hub topics and sending messages to Snowflake tables with support for batch ingestion and checkpointing.

## Architecture

The project follows a modular async architecture:
- **CLI Layer**: Typer-based command-line interface (`src/main.py`)
- **Configuration**: Pydantic-based config management with environment variables (`src/utils/config.py`)
- **Consumers**: Azure Event Hub async consumers with checkpointing (`src/consumers/eventhub.py`)
- **Streaming**: Snowflake batch ingestion and control table management (`src/streaming/motherduck.py`)
- **Orchestration**: Async pipeline orchestrator for managing mappings (`src/pipeline/orchestrator.py`)

## Code Style and Conventions

### Python Version and Standards
- **Python Version**: 3.13+ (strictly enforced via `requires-python` in `pyproject.toml`)
- **Type Hints**: Always use type hints for function parameters and return values
- **Async/Await**: Use async/await patterns for I/O operations (Event Hub, Snowflake)
- **Docstrings**: Use triple-quoted docstrings with clear descriptions for modules, classes, and functions

### Code Organization
- Place new modules in appropriate subdirectories under `src/`:
  - `consumers/` - Event Hub consumers and related functionality
  - `streaming/` - Snowflake streaming and ingestion logic
  - `utils/` - Shared utilities, config, and helper functions
  - `pipeline/` - Orchestration and pipeline management
- Use `__init__.py` files to expose public APIs from modules
- Keep CLI commands in `src/main.py`

### Configuration Management
- Use Pydantic models for configuration validation (see `src/utils/config.py`)
- Load environment variables via `python-dotenv` from `.env` files
- Use `pydantic-settings` for BaseSettings classes
- Validate configurations before use

### Error Handling
- Use try-except blocks with specific exception types
- Log errors using the `logging` module with Rich handlers
- Provide user-friendly error messages in CLI commands
- Use `typer.Exit(1)` for command failures

### Naming Conventions
- **Functions/Variables**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private members**: Prefix with single underscore `_private_method`

## Development Workflow

### Setup
```bash
# Clone repository
git clone <repository-url>
cd evsnow

# Copy environment example
cp .env.example .env
# Edit .env with your credentials

# Install dependencies (uv is used as package manager)
# For development (includes test dependencies):
uv sync --all-groups

# For production (no test dependencies):
# uv sync

# Verify configuration
uv run python src/main.py validate-config
```

### Running the Project
```bash
# Validate configuration
uv run python src/main.py validate-config

# Run the pipeline
uv run python src/main.py run

# Show status
uv run python src/main.py status

# Dry run mode
uv run python src/main.py run --dry-run

# Show version
uv run python src/main.py version
```

### Linting and Type Checking
```bash
# Run mypy type checker
uv run mypy src/

# MyPy configuration is in pyproject.toml under [tool.mypy]
# Settings: strict equality, warn on redundant casts, check untyped defs
```

### Testing
- Currently no test framework is configured
- When adding tests, place them in a `tests/` directory at the project root
- Use pytest as the testing framework if implementing tests

## Key Dependencies

### Required Packages
- **azure-eventhub**: Event Hub SDK for consuming messages
- **azure-identity**: Azure authentication with DefaultAzureCredential
- **snowflake-connector-python**: Snowflake/DuckDB database interface
- **pydantic**: Data validation and settings management
- **typer**: CLI framework with Rich integration
- **rich**: Beautiful terminal output and logging
- **mypy**: Static type checking

### Package Management
- Use `uv` as the package manager (modern, fast Python package installer)
- Dependencies are defined in `pyproject.toml` under `[project.dependencies]`

## Azure Event Hub Best Practices

When working with Event Hub consumers:
- Use `DefaultAzureCredential` for authentication (preferred over connection strings)
- Configure appropriate `max_batch_size` (default: 1000)
- Set reasonable `max_wait_time` for batch collection (default: 60s)
- Use `prefetch_count` for performance optimization (default: 300)
- Implement checkpoint management every N seconds (default: 300s)
- Handle consumer group configuration properly

## Snowflake Integration Best Practices

When working with Snowflake:
- Use batch ingestion for high performance
- Maintain control tables for watermark checkpointing
- Configure appropriate `batch_size` for ingestion
- Use DuckDB connection with Snowflake token
- Handle schema and table creation gracefully
- Verify connections before starting pipeline

## Important Files

- `src/main.py` - Main CLI entry point with Typer commands
- `src/utils/config.py` - Configuration models and validation
- `src/consumers/eventhub.py` - Event Hub consumer implementation
- `src/streaming/motherduck.py` - Snowflake streaming logic
- `src/pipeline/orchestrator.py` - Pipeline orchestration
- `pyproject.toml` - Project metadata and dependencies
- `.env.example` - Environment variable template
- `README.md` - Project documentation

## When Making Changes

1. **Adding New Features**:
   - Place code in appropriate subdirectories under `src/`
   - Add type hints to all functions and methods
   - Use async/await for I/O operations
   - Add docstrings explaining purpose and usage
   - Update CLI commands in `src/main.py` if needed

2. **Modifying Configuration**:
   - Update Pydantic models in `src/utils/config.py`
   - Add validation logic using `@field_validator` or `@model_validator`
   - Update `.env.example` with new environment variables
   - Document changes in docstrings

3. **Working with Event Hubs**:
   - Maintain async patterns with `asyncio`
   - Use proper error handling for network operations
   - Follow checkpoint management patterns
   - Test with both connection strings and DefaultAzureCredential

4. **Working with Snowflake**:
   - Use batch operations for efficiency
   - Handle connection lifecycle properly
   - Maintain control table consistency
   - Validate schema/table existence before operations

5. **CLI Changes**:
   - Use Typer decorators (`@app.command()`)
   - Provide helpful descriptions and options
   - Use Rich console for formatted output
   - Handle errors gracefully with appropriate exit codes

6. **Type Checking**:
   - Run `uv run mypy src/` before committing
   - Fix any type errors or add proper ignores
   - Follow mypy configuration in `pyproject.toml`

## Code Quality Standards

- Write clear, self-documenting code
- Keep functions focused and concise
- Avoid deep nesting (max 3-4 levels)
- Use meaningful variable names
- Add comments for complex logic only
- Follow Python PEP 8 style guide
- Prefer composition over inheritance
- Use context managers for resource handling

## Internet Access and Research

When working on this repository, you have full access to the internet and should use it to:
- Research latest documentation for Azure Event Hub, Snowflake, and other dependencies
- Look up current best practices and API changes
- Check for updated package versions and compatibility
- Find solutions to errors or edge cases
- Verify syntax and usage patterns for libraries
- Search for security vulnerabilities or known issues

Always prefer official documentation and reliable sources when researching. Use web search freely to stay current with evolving technologies and practices.
