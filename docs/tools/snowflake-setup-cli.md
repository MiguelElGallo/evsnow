---
search:
  exclude: true
---

# Snowflake Setup CLI

Archived experimental setup CLI with Copilot Agent assistance for the EvSnow
project.

!!! warning "Archived tool"

    This page is kept for project history. The current supported setup path is
    [Snowflake quickstart](../getting-started/snowflake-quickstart.md), which
    uses checked-in SQL and the Snowflake CLI. Do not use this page as the
    first-run setup guide.

This tool is experimental and setup-only. It can generate Programmatic Access
Token SQL restricted to an administrative setup role. Do not use those tokens
for runtime ingestion or query access.

## Overview

This CLI tool guides you through complete Snowflake infrastructure setup using
an AI-powered agent (GitHub Copilot SDK). It automates the process described in
[Complete Snowflake setup](../snowflake/complete-setup.md).

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
uv run python -m main setup --account <account_identifier> --user <admin_user>
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

The CLI generates SQL to create a setup-only Programmatic Access Token (PAT).
Use the narrowest administrative setup role your organization allows.

```sql
ALTER USER <username> ADD PROGRAMMATIC ACCESS TOKEN <token_name>
    ROLE_RESTRICTION = '<SETUP_ADMIN_ROLE>'
    DAYS_TO_EXPIRY = 90;
```

You run this SQL in Snowflake and provide the resulting token.

### Step 2: AI Agent Setup

The Copilot agent then:

1. **Creates Snow CLI connection** using PAT authentication
2. **Tests the connection** and troubleshoots any issues
3. **Uses Snowflake-managed internal Iceberg storage by default**
4. **Executes full setup** following [Complete Snowflake setup](../snowflake/complete-setup.md):
   - Creates STREAM role and STREAMEV user
   - Generates RSA keys
   - Creates INGESTION and CONTROL databases
   - Creates a Snowflake-managed internal Iceberg table and streaming pipe
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
- Administrative PATs should only be used for one-time setup

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

- Verify account format (e.g., `<account_identifier>`)
- Check network connectivity
- Ensure PAT hasn't expired
- Verify ACCOUNTADMIN role access

## Related Files

- [Complete Snowflake setup](../snowflake/complete-setup.md) - Full setup guide
- [`.env.example`](https://github.com/MiguelElGallo/evsnow/blob/main/.env.example) - Environment variable template
