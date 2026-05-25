---
search:
  exclude: true
---

# 🚀 Snowflake Setup CLI with Copilot Agent - Implementation Plan

## 📋 Overview

Create a Python CLI tool based on Typer that guides users through complete Snowflake setup using an AI-powered agent (GitHub Copilot SDK).

### User Flow

```markdown
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  User provides  │────▶│  CLI generates  │────▶│  User runs SQL  │
│  account + user │     │  PAT SQL        │     │  in Snowflake   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Agent asks for │◀────│  Agent uses     │◀────│  User provides  │
│  storage info   │     │  Snow CLI       │     │  PAT token      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                │
                                ▼
                        ┌─────────────────┐
                        │  Agent runs     │
                        │  full setup     │
                        │  from guide     │
                        └─────────────────┘
```

---

## 📁 Project Structure

```markdown
tools/
└── snowflake_setup/
    ├── __init__.py
    ├── main.py           # CLI entry point with Typer
    ├── prompts.py        # Agent system prompts and instructions
    ├── sql_templates.py  # SQL generation for PAT and setup
    └── agent.py          # Copilot SDK agent wrapper
```

---

## 🔧 Dependencies to Add

```toml
# In pyproject.toml under [project.dependencies]
github-copilot-sdk = ">=0.1.0"
```

Or install separately:

```bash
pip install github-copilot-sdk
```

**Note**: The Copilot CLI must be installed separately. The SDK communicates with the Copilot CLI in server mode.

---

## 📝 Implementation Details

### Phase 1: CLI Entry Point (`main.py`)

```python
#!/usr/bin/env python3
"""
Snowflake Setup CLI - Automated Snowflake configuration with Copilot Agent.

This CLI guides users through:
1. Generating PAT (Programmatic Access Token) SQL
2. Creating Snow CLI connection
3. Setting up complete Snowflake infrastructure from SNOWFLAKE_COMPLETE_SETUP.md
"""

import asyncio
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.syntax import Syntax

app = typer.Typer(
    name="snowflake-setup",
    help="Automated Snowflake setup with Copilot Agent assistance",
    add_completion=False,
)
console = Console()


@app.command()
def setup(
    account: str = typer.Option(
        None,
        "--account",
        "-a",
        help="Snowflake account identifier (e.g., <account_identifier>)",
        prompt="Enter your Snowflake account identifier",
    ),
    user: str = typer.Option(
        None,
        "--user",
        "-u",
        help="Snowflake username",
        prompt="Enter your Snowflake username",
    ),
    token_name: str = typer.Option(
        "EVSNOW_SETUP_PAT",
        "--token-name",
        "-t",
        help="Name for the PAT token",
    ),
    days_to_expiry: int = typer.Option(
        90,
        "--days",
        "-d",
        help="Days until PAT expires",
    ),
) -> None:
    """
    Start the interactive Snowflake setup wizard.

    This command will:
    1. Generate SQL to create a PAT for your user
    2. Ask you to run the SQL and provide the token
    3. Start an AI agent to complete the setup automatically
    """
    asyncio.run(_run_setup(account, user, token_name, days_to_expiry))


async def _run_setup(
    account: str,
    user: str,
    token_name: str,
    days_to_expiry: int,
) -> None:
    """Main setup orchestration."""
    from .sql_templates import generate_pat_sql
    from .agent import run_setup_agent

    console.print(Panel.fit(
        "[bold blue]Snowflake Setup Wizard[/bold blue]\n"
        "This wizard will help you set up your complete Snowflake infrastructure.",
        title="🏔️ EvSnow Setup"
    ))

    # Step 1: Generate and display PAT SQL
    pat_sql = generate_pat_sql(user, token_name, days_to_expiry)

    console.print("\n[bold yellow]Step 1: Create Programmatic Access Token (PAT)[/bold yellow]")
    console.print("\nRun the following SQL in your Snowflake worksheet as ACCOUNTADMIN:\n")

    syntax = Syntax(pat_sql, "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="SQL to Run", border_style="green"))

    console.print("\n[bold]After running the SQL, copy the 'token_secret' value from the output.[/bold]")
    console.print("[dim]The token appears ONLY ONCE - save it immediately![/dim]\n")

    # Step 2: Get the PAT from user
    pat_token = Prompt.ask(
        "[bold cyan]Paste the token_secret here[/bold cyan]",
        password=True,  # Hide input for security
    )

    if not pat_token or len(pat_token) < 10:
        console.print("[bold red]Error: Invalid token provided. Please run the setup again.[/bold red]")
        raise typer.Exit(1)

    console.print("\n[bold green]✓ Token received![/bold green]")

    # Step 3: Start the Copilot agent
    console.print("\n[bold yellow]Step 2: Starting AI Agent for automated setup...[/bold yellow]\n")

    await run_setup_agent(
        account=account,
        user=user,
        pat_token=pat_token,
    )


@app.command()
def validate() -> None:
    """Validate the current Snowflake setup."""
    console.print("[bold]Validating Snowflake setup...[/bold]")
    # TODO: Implement validation


if __name__ == "__main__":
    app()
```

---

### Phase 2: SQL Templates (`sql_templates.py`)

```python
"""SQL template generation for Snowflake PAT and setup."""


def generate_pat_sql(
    user: str,
    token_name: str,
    days_to_expiry: int = 90,
) -> str:
    """
    Generate SQL to create a PAT for the specified user.

    Args:
        user: Snowflake username
        token_name: Name for the PAT
        days_to_expiry: Token validity period

    Returns:
        SQL statement to execute as ACCOUNTADMIN
    """
    return f"""\
-- Create Programmatic Access Token (PAT) for automated setup
-- Run this as ACCOUNTADMIN in a Snowflake SQL worksheet

-- First, ensure you're using ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create the PAT
ALTER USER {user} ADD PROGRAMMATIC ACCESS TOKEN {token_name}
    ROLE_RESTRICTION = 'ACCOUNTADMIN'
    DAYS_TO_EXPIRY = {days_to_expiry}
    COMMENT = 'PAT for EvSnow automated setup';

-- IMPORTANT: The token_secret appears ONLY in this output!
-- Copy it immediately and store securely.
-- You cannot retrieve it again later.
"""


def generate_verify_pat_sql(user: str) -> str:
    """Generate SQL to verify PAT tokens for a user."""
    return f"""\
-- List all PATs for user
SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER {user};
"""
```

---

### Phase 3: Agent System Prompts (`prompts.py`)

```python
"""System prompts and instructions for the Copilot agent."""

from pathlib import Path


def get_setup_guide_content() -> str:
    """Read the SNOWFLAKE_COMPLETE_SETUP.md file content."""
    guide_path = Path(__file__).parent.parent.parent / "SNOWFLAKE_COMPLETE_SETUP.md"
    if guide_path.exists():
        return guide_path.read_text()
    return ""


def get_agent_system_prompt(account: str, user: str) -> str:
    """
    Generate the system prompt for the setup agent.

    The agent will:
    1. Use Snow CLI to create and test a connection
    2. Follow SNOWFLAKE_COMPLETE_SETUP.md to set up everything
    """
    setup_guide = get_setup_guide_content()

    return f"""\
You are an expert Snowflake setup assistant. Your task is to help the user set up their complete Snowflake infrastructure for the EvSnow project.

## Context
- Snowflake Account: {account}
- Snowflake User: {user}
- The user has already created a PAT (Programmatic Access Token) with ACCOUNTADMIN role restriction

## Your Tasks (Execute in Order)

### Task 1: Create Snow CLI Connection
Use the Snow CLI to create a connection using the PAT token.

1. Create a connection using:
```bash
snow connection add \\
    --connection-name evsnow-setup \\
    --account {account} \\
    --user {user} \\
    --authenticator PROGRAMMATIC_ACCESS_TOKEN \\
    --token-file-path <path-to-token-file>
```

1. First, save the PAT token to a secure temp file, then create the connection.

2. Test the connection:

```bash
snow connection test -c evsnow-setup
```

1. **IMPORTANT**: Do NOT ask the user for help unless the connection repeatedly fails.
   - If there are errors, try to diagnose and fix them automatically
   - Common issues: wrong account format, network issues, token expired
   - Only ask the user if you've exhausted troubleshooting options

### Task 2: Gather Required Information

Once the connection works, ask the user for the following information needed for setup:

1. Confirm the default Snowflake-managed internal Iceberg storage model
2. Do not request Azure Storage Account, Container, or Tenant ID unless the user explicitly chooses customer-managed external-volume Iceberg

Ask for these in a clear, conversational manner. Explain why each is needed.

### Task 3: Execute Complete Setup

Follow the SNOWFLAKE_COMPLETE_SETUP.md guide below to set up:

1. Create Role (STREAM) and User (STREAMEV) - Step 2.1
2. Generate RSA keys using the existing script - Step 1
3. Create INGESTION and CONTROL databases - Steps 2.3, 2.4
4. Create INGESTION_STATUS control table - Step 2.5
5. Create Snowflake-managed internal Iceberg Table (EVENTS_TABLE1) - Step 4.1
7. Create Streaming Pipe (EVENTS_TABLE_PIPE) - Step 4.2
8. Set up all necessary grants

Use `snow sql -c evsnow-setup -q "<SQL>"` to execute SQL commands.

## SNOWFLAKE_COMPLETE_SETUP.md Reference

{setup_guide}

## Important Guidelines

1. **Be autonomous**: Complete as much as possible without asking the user
2. **Be informative**: Explain what you're doing at each step
3. **Handle errors gracefully**: Try to fix issues before asking for help
4. **Verify each step**: Test that each component was created successfully
5. **Generate .env content**: At the end, provide the user with the .env variables they need

## Output Format

When complete, provide:

1. Summary of what was created
2. Any manual steps the user needs to complete
3. The .env file content for their configuration
"""

---

### Phase 4: Copilot Agent Wrapper (`agent.py`)

```python
"""GitHub Copilot SDK agent for Snowflake setup automation."""

import asyncio
import tempfile
from pathlib import Path

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

from copilot import CopilotClient
from copilot.generated.session_events import SessionEventType

from .prompts import get_agent_system_prompt

console = Console()


async def run_setup_agent(
    account: str,
    user: str,
    pat_token: str,
) -> None:
    """
    Run the Copilot agent to complete Snowflake setup.

    Args:
        account: Snowflake account identifier
        user: Snowflake username
        pat_token: PAT token for authentication
    """
    # Save PAT token to a temporary file for Snow CLI
    token_file = Path(tempfile.mktemp(suffix=".token"))
    token_file.write_text(pat_token)
    token_file.chmod(0o600)  # Secure permissions

    try:
        # Create Copilot client
        client = CopilotClient()
        await client.start()

        console.print("[dim]Agent started. Processing setup...[/dim]\n")

        # Get system prompt with setup guide
        system_prompt = get_agent_system_prompt(account, user)

        # Create session with the agent
        session = await client.create_session({
            "model": "gpt-4.1",  # Or claude-sonnet-4.5
            "streaming": True,
            "system_message": {
                "mode": "append",
                "content": system_prompt,
            },
            "on_user_input_request": handle_user_input,
        })

        # Set up event handlers for streaming output
        done = asyncio.Event()

        def on_event(event):
            if event.type.value == "assistant.message_delta":
                # Stream the response as it comes
                delta = event.data.delta_content or ""
                console.print(delta, end="")
            elif event.type.value == "assistant.message":
                # Final message
                console.print()  # New line after streaming
            elif event.type.value == "tool.execution_start":
                tool_name = event.data.tool_name if hasattr(event.data, 'tool_name') else 'tool'
                console.print(f"\n[dim]→ Executing: {tool_name}[/dim]")
            elif event.type.value == "tool.execution_complete":
                console.print("[dim]  ✓ Complete[/dim]")
            elif event.type.value == "session.idle":
                done.set()

        session.on(on_event)

        # Send initial prompt to start the setup
        initial_prompt = f"""\
Start the Snowflake setup process.

The PAT token has been saved to: {token_file}

Please begin by:
1. Creating a Snow CLI connection using this token file
2. Testing the connection
3. Then proceeding with the full setup

Account: {account}
User: {user}
Token file: {token_file}
"""

        await session.send({"prompt": initial_prompt})

        # Wait for completion
        await done.wait()

        # Cleanup
        await session.destroy()
        await client.stop()

        console.print(Panel.fit(
            "[bold green]✓ Setup process completed![/bold green]\n\n"
            "Review the output above for any manual steps needed.",
            title="🎉 Complete"
        ))

    finally:
        # Clean up token file
        if token_file.exists():
            token_file.unlink()


async def handle_user_input(request, invocation):
    """
    Handle requests from the agent that need user input.

    This is called when the agent uses the ask_user tool.
    """
    question = request.get("question", "")
    choices = request.get("choices", [])
    allow_freeform = request.get("allowFreeform", True)

    console.print(f"\n[bold cyan]Agent asks:[/bold cyan] {question}")

    if choices:
        console.print("[dim]Options:[/dim]")
        for i, choice in enumerate(choices, 1):
            console.print(f"  {i}. {choice}")

    from rich.prompt import Prompt
    answer = Prompt.ask("[bold cyan]Your answer[/bold cyan]")

    return {
        "answer": answer,
        "wasFreeform": allow_freeform,
    }
```

---

## 🔄 Execution Flow

### Step-by-Step Flow

```markdown
1. User runs: uv run python -m tools.snowflake_setup.main setup
   ↓
2. CLI prompts for:
   - Snowflake account (e.g., <account_identifier>)
   - Snowflake username (e.g., ADMIN)
   ↓
3. CLI generates PAT SQL and displays it
   - User must run this SQL in Snowflake as ACCOUNTADMIN
   - SQL creates a PAT with ROLE_RESTRICTION='ACCOUNTADMIN'
   ↓
4. User pastes the token_secret from SQL output
   ↓
5. Copilot Agent starts with tasks:

   a) CREATE SNOW CLI CONNECTION
      - Save PAT to temp file
      - Run: snow connection add --authenticator PROGRAMMATIC_ACCESS_TOKEN
      - Run: snow connection test
      - Troubleshoot if needed (without asking user)

   b) ONCE CONNECTION WORKS
      - Confirm default Snowflake-managed internal Iceberg storage

   c) EXECUTE FULL SETUP (following SNOWFLAKE_COMPLETE_SETUP.md)
      - Create STREAM role
      - Create STREAMEV user
      - Run generate_snowflake_keys.sh
      - Create INGESTION database
      - Create CONTROL database
      - Create INGESTION_STATUS table
      - Create Iceberg table EVENTS_TABLE1
      - Create Pipe EVENTS_TABLE_PIPE
      - Set up all grants

   d) OUTPUT SUMMARY
      - What was created
      - .env file content
      - Any manual steps needed
```

---

## 🛠️ Key Technical Decisions

### 1. Why PAT with ACCOUNTADMIN?

- PAT allows programmatic access without interactive login
- ACCOUNTADMIN role is needed for the initial database, user, role, and grant setup
- Token can be scoped with expiry for security

### 2. Why Copilot SDK instead of raw LLM?

- Built-in tool execution (bash, file operations)
- Handles context management automatically
- Streaming support for real-time feedback
- User input handling for interactive prompts

### 3. Snow CLI vs Direct SQL

- Snow CLI provides connection management and testing
- Easier to handle authentication with PAT
- Built-in error reporting and diagnostics

---

## 📦 Installation & Usage

### Install Dependencies

```bash
# Add to project
uv add github-copilot-sdk

# Or manually
pip install github-copilot-sdk
```

### Ensure Copilot CLI is installed

```bash
# The Copilot CLI must be installed separately
# Follow GitHub Copilot CLI installation instructions
```

### Run the Setup Wizard

```bash
# Interactive mode
uv run python -m tools.snowflake_setup.main setup

# With arguments
uv run python -m tools.snowflake_setup.main setup \
    --account <account_identifier> \
    --user ADMIN \
    --token-name MY_SETUP_PAT \
    --days 30
```

---

## ⚠️ Important Notes

### Security Considerations

1. **PAT token is sensitive** - Never log or display it
2. **Token file permissions** - Set to 0600 (owner read/write only)
3. **Clean up tokens** - Delete token file after use
4. **ACCOUNTADMIN scope** - Only use for setup, not runtime

### Snow CLI PAT Authentication

From the docs, to use PAT with Snow CLI:

```toml
# In config.toml
[connections.myconnection]
account = "my_account"
user = "jdoe"
authenticator = "PROGRAMMATIC_ACCESS_TOKEN"
token_file_path = "path-to-pat-token"
```

Or via command line:

```bash
snow connection add \
    --connection-name evsnow-setup \
    --account my_account \
    --user jdoe \
    --authenticator PROGRAMMATIC_ACCESS_TOKEN \
    --token-file-path /path/to/token
```

---

## 🧪 Testing Plan

1. **Unit Tests**: Mock Copilot SDK responses
2. **Integration Tests**: Test against Snowflake trial account
3. **Manual Testing**: Full end-to-end flow

---

## 📅 Implementation Timeline

| Phase | Description | Estimated Time |
|-------|-------------|----------------|
| 1 | CLI scaffolding with Typer | 2 hours |
| 2 | SQL template generation | 1 hour |
| 3 | Copilot SDK integration | 4 hours |
| 4 | Agent prompt engineering | 3 hours |
| 5 | Error handling & polish | 2 hours |
| 6 | Testing & documentation | 2 hours |

**Total Estimated Time**: ~14 hours

---

## 🔗 References

- [Snowflake PAT Documentation](https://docs.snowflake.com/en/sql-reference/sql/alter-user-add-programmatic-access-token)
- [Snow CLI Connection Management](https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections)
- [GitHub Copilot SDK (Python)](https://github.com/github/copilot-sdk/tree/main/python)
- [SNOWFLAKE_COMPLETE_SETUP.md](../snowflake/complete-setup.md)
