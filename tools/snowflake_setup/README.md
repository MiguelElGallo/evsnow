# Snowflake Setup CLI

Automated Snowflake setup CLI with Copilot Agent assistance for the EvSnow project.

## Overview

This CLI tool guides you through complete Snowflake infrastructure setup using an AI-powered agent (GitHub Copilot SDK). It automates the entire process described in `SNOWFLAKE_COMPLETE_SETUP.md`.

## Prerequisites

1. **Python 3.13+**
2. **uv** package manager
3. **Snowflake account** with ACCOUNTADMIN access
4. **GitHub Copilot CLI** installed and authenticated
5. **Snow CLI** installed (`pip install snowflake-cli` or via homebrew)

## Installation

```bash
# Navigate to the tool directory
cd tools/snowflake_setup

# Install dependencies with uv
uv sync

# Or install in development mode
uv pip install -e .
```

## Usage

### Interactive Setup

```bash
# Run the setup wizard (will prompt for inputs)
uv run python -m main setup

# Or with arguments
uv run python -m main setup --account VWBIVWS-SV93109 --user ADMIN
```

### Command Options

```bash
uv run python -m main setup --help
```

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--account` | `-a` | Snowflake account identifier | (prompted) |
| `--user` | `-u` | Snowflake username | (prompted) |
| `--token-name` | `-t` | Name for the PAT token | `EVSNOW_SETUP_PAT` |
| `--days` | `-d` | Days until PAT expires | `90` |

## How It Works

### Step 1: PAT Generation

The CLI generates SQL to create a Programmatic Access Token (PAT) with ACCOUNTADMIN privileges:

```sql
ALTER USER <username> ADD PROGRAMMATIC ACCESS TOKEN <token_name>
    ROLE_RESTRICTION = 'ACCOUNTADMIN'
    DAYS_TO_EXPIRY = 90;
```

You run this SQL in Snowflake and provide the resulting token.

### Step 2: AI Agent Setup

The Copilot agent then:

1. **Creates Snow CLI connection** using PAT authentication
2. **Tests the connection** and troubleshoots any issues
3. **Gathers required info** (Azure Storage Account, Container, Tenant ID)
4. **Executes full setup** following `SNOWFLAKE_COMPLETE_SETUP.md`:
   - Creates STREAM role and STREAMEV user
   - Generates RSA keys
   - Creates INGESTION and CONTROL databases
   - Creates External Volume (EXVOL)
   - Creates Iceberg table and streaming pipe
   - Sets up all necessary grants

### Step 3: Output

The agent provides:

- Summary of all created objects
- `.env` file content for configuration
- Any manual steps needed

## Architecture

```markdown
snowflake_setup/
├── __init__.py       # Package metadata
├── main.py           # Typer CLI entry point
├── sql_templates.py  # PAT SQL generation
├── prompts.py        # Agent system prompts
├── agent.py          # Copilot SDK wrapper
├── pyproject.toml    # Package dependencies
└── README.md         # This file
```

## Security Notes

- PAT tokens are stored in temporary files with `0600` permissions
- Token files are deleted after use
- Never commit tokens to version control
- PAT with ACCOUNTADMIN should only be used for setup

## Troubleshooting

### Snow CLI not found

```bash
# Install Snow CLI
pip install snowflake-cli
# or
brew install snowflake-cli
```

### Copilot CLI not authenticated

```bash
# Authenticate Copilot CLI
copilot auth login
```

### Connection fails

- Verify account format (e.g., `VWBIVWS-SV93109`)
- Check network connectivity
- Ensure PAT hasn't expired
- Verify ACCOUNTADMIN role access

## Related Files

- [SNOWFLAKE_COMPLETE_SETUP.md](../../SNOWFLAKE_COMPLETE_SETUP.md) - Full setup guide
- [.env.example](../../.env.example) - Environment variable template
