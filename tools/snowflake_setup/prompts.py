"""System prompts and instructions for the Copilot agent."""

from pathlib import Path


def get_setup_guide_content() -> str:
    """Read the SNOWFLAKE_COMPLETE_SETUP.md file content."""
    # Navigate from tools/snowflake_setup to project root
    guide_path = Path(__file__).parent.parent.parent / "SNOWFLAKE_COMPLETE_SETUP.md"
    if guide_path.exists():
        return guide_path.read_text()
    return ""


def get_connection_prompt(account: str, user: str, token_file_path: str) -> str:
    """
    Generate the system prompt for the connection agent.

    This agent ONLY handles creating and testing the Snow CLI connection.
    """
    return f"""\
You are a Snowflake CLI connection assistant. Your ONLY task is to create and test a Snow CLI connection.

## Context
- Snowflake Account: {account}
- Snowflake User: {user}
- PAT Token File: {token_file_path}

## Your Task: Create and Test Snow CLI Connection

You must execute these two commands:

### Step 1: Add the connection

Run this exact command:
```bash
snow connection add \\
    --connection-name evsnow-setup \\
    --account {account} \\
    --user {user} \\
    --authenticator PROGRAMMATIC_ACCESS_TOKEN \\
    --token-file-path {token_file_path} \\
    --no-interactive
```

If the connection already exists, you may see an error. In that case, try removing it first:
```bash
snow connection remove evsnow-setup
```
Then add it again.

### Step 2: Test the connection

Run this command:
```bash
snow connection test -c evsnow-setup
```

## Expected Output

If successful, you should see output like:
```
+--------------------------------------------------+
| key             | value                          |
|-----------------+--------------------------------|
| Connection name | evsnow-setup                   |
| Status          | OK                             |
| Host            | ...snowflakecomputing.com      |
| Account         | {account}                      |
| User            | {user}                         |
| Role            | ACCOUNTADMIN                   |
+--------------------------------------------------+
```

## Important Instructions

1. **Execute the commands** - Do not just explain them, actually run them
2. **Report the result clearly** - After testing, tell me if it worked or failed
3. **If successful**, include the text "CONNECTION_SUCCESS" in your response
4. **If it fails**, explain what went wrong so the user can fix it

Common issues:
- Account format should be like "ORGNAME-ACCOUNTNAME" (e.g., "VWBIVWS-SV93109")
- The token file must exist and be readable
- Snow CLI must be installed (`brew install snowflake-snowpark-python` or `pipx install snowflake-cli`)
"""


def get_setup_prompt(account: str, user: str, token_file_path: str) -> str:
    """
    Generate the system prompt for the full setup agent.

    This agent handles the complete Snowflake infrastructure setup.
    """
    setup_guide = get_setup_guide_content()

    return f"""\
You are an expert Snowflake setup assistant. The Snow CLI connection is already established and working.

## Context
- Snowflake Account: {account}
- Snowflake User: {user}
- Connection Name: evsnow-setup (already configured and tested)

## Your Tasks

### Task 1: Confirm Default Storage Model
Use Snowflake-managed internal Iceberg storage by default. Do not ask for Azure
Storage Account, Azure Storage Container, or Azure Tenant ID unless the user
explicitly chooses customer-managed external-volume Iceberg storage.

### Task 2: Execute Complete Setup
Follow the SNOWFLAKE_COMPLETE_SETUP.md guide to set up:

1. Create Role (STREAM) and User (STREAMEV)
2. Create INGESTION and CONTROL databases
3. Create INGESTION_STATUS control table
4. Create Snowflake-managed internal Iceberg Table (EVENTS_TABLE1)
6. Create Streaming Pipe (EVENTS_TABLE_PIPE)
7. Set up all necessary grants

Use this command format to execute SQL:
```bash
snow sql -c evsnow-setup -q "<SQL>"
```

## SNOWFLAKE_COMPLETE_SETUP.md Reference

{setup_guide}

## Important Guidelines

1. **Execute commands** - Don't just explain, actually run them
2. **Verify each step** - Check that objects were created successfully
3. **Handle errors** - If something fails, try to fix it or explain the issue
4. **Generate .env content** - At the end, provide the .env variables needed

## Output Format
When complete, provide:
1. Summary of what was created
2. Any manual steps the user needs to complete
3. The .env file content for their configuration
"""


# Keep the old function for backwards compatibility
def get_agent_system_prompt(account: str, user: str, token_file_path: str) -> str:
    """Generate the combined system prompt (deprecated, use separate prompts)."""
    return get_connection_prompt(account, user, token_file_path)
