"""GitHub Copilot SDK agent for Snowflake setup automation."""

import asyncio
import sys
import tempfile
from pathlib import Path
from typing import Any

from copilot import CopilotClient
from copilot.types import UserInputRequest, UserInputResponse
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

# Handle imports for both module and direct execution
sys.path.insert(0, str(Path(__file__).parent))
from prompts import get_connection_prompt, get_setup_prompt

console = Console()


def log_event(event_type: str, message: str, data: Any = None) -> None:
    """Log agent events with detailed information."""
    if data:
        console.print(f"[dim][{event_type}][/dim] {message}: [cyan]{data}[/cyan]")
    else:
        console.print(f"[dim][{event_type}][/dim] {message}")


async def handle_user_input(
    request: UserInputRequest, invocation: dict[str, str]
) -> UserInputResponse:
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

    answer = Prompt.ask("[bold cyan]Your answer[/bold cyan]")

    return UserInputResponse(
        answer=answer,
        wasFreeform=allow_freeform,
    )


async def run_connection_agent(
    client: CopilotClient,
    account: str,
    user: str,
    token_file: Path,
) -> bool:
    """
    Run the agent to create and test Snow CLI connection.

    Returns True if connection was successful, False otherwise.
    """
    console.print(
        Panel(
            "[bold yellow]Phase 1: Creating Snow CLI Connection[/bold yellow]\n\n"
            "[dim]Using default model[/dim]\n\n"
            "The agent will now:\n"
            "1. Add a Snow CLI connection with PAT authentication\n"
            "2. Test the connection to verify it works",
            title="🔗 Connection Setup",
        )
    )

    # Get the connection-specific prompt
    system_prompt = get_connection_prompt(account, user, str(token_file))

    # Create session with the agent - use minimal config to avoid API issues
    log_event("SESSION", "Creating agent session")
    session = await client.create_session(
        {
            "system_message": {
                "mode": "append",
                "content": system_prompt,
            },
            "on_user_input_request": handle_user_input,
        }
    )

    # Set up event handlers for streaming output with verbose logging
    done = asyncio.Event()
    connection_success = False
    last_output = ""
    reasoning_buffer = ""

    def on_event(event: Any) -> None:
        nonlocal connection_success, last_output, reasoning_buffer
        event_type = event.type.value if hasattr(event.type, "value") else str(event.type)

        if event_type == "assistant.message_delta":
            delta = getattr(event.data, "delta_content", None) or ""
            console.print(delta, end="", markup=False)
            last_output += delta
        elif event_type == "assistant.reasoning_delta":
            # Show reasoning/thinking in a distinct way
            delta = (
                getattr(event.data, "delta_content", None)
                or getattr(event.data, "delta", None)
                or ""
            )
            if delta:
                reasoning_buffer += delta
                # Print reasoning in dim yellow to distinguish from regular output
                console.print(f"[dim yellow]{delta}[/dim yellow]", end="", markup=True)
        elif event_type == "assistant.reasoning":
            # End of reasoning block
            if reasoning_buffer:
                console.print()  # New line after reasoning
                reasoning_buffer = ""
        elif event_type == "assistant.message":
            console.print()  # New line after streaming
            log_event("MESSAGE", "Assistant completed response")
            # Check for success indicators
            if (
                "Status" in last_output and "OK" in last_output
            ) or "CONNECTION_SUCCESS" in last_output:
                connection_success = True
            last_output = ""
        elif event_type == "tool.execution_start":
            tool_name = getattr(event.data, "tool_name", "unknown")
            log_event("TOOL", f"▶ Executing: {tool_name}")
        elif event_type == "tool.execution_complete":
            result = getattr(event.data, "result", None)
            log_event("TOOL", "✓ Tool completed")
            if result:
                result_str = str(result)[:500]
                console.print(f"[dim]  Output: {result_str}[/dim]")
                # Check for connection test success in tool output
                if "Status" in result_str and "OK" in result_str:
                    connection_success = True
        elif event_type == "session.idle":
            log_event("SESSION", "Agent finished")
            done.set()
        elif event_type == "session.error":
            # Extract and display error details
            error_msg = getattr(event.data, "message", None) or getattr(event.data, "error", None)
            error_code = getattr(event.data, "code", None)
            error_details = getattr(event.data, "details", None)

            console.print("\n[bold red]━━━ SESSION ERROR ━━━[/bold red]")
            if error_code:
                console.print(f"[red]Code: {error_code}[/red]")
            if error_msg:
                console.print(f"[red]Message: {error_msg}[/red]")
            if error_details:
                console.print(f"[red]Details: {error_details}[/red]")

            # Try to get all attributes from the event data for debugging
            if hasattr(event, "data"):
                data_dict = (
                    {k: v for k, v in vars(event.data).items() if not k.startswith("_")}
                    if hasattr(event.data, "__dict__")
                    else str(event.data)
                )
                console.print(f"[dim red]Full error data: {data_dict}[/dim red]")
            console.print("[bold red]━━━━━━━━━━━━━━━━━━━━[/bold red]\n")
            done.set()
        elif event_type in ("assistant.reasoning_delta", "ping", "pong"):
            # Skip noisy events silently
            pass
        elif event_type == "session.usage_info":
            # Show usage info (token counts, model info)
            if hasattr(event, "data"):
                data_dict = (
                    {k: v for k, v in vars(event.data).items() if not k.startswith("_")}
                    if hasattr(event.data, "__dict__")
                    else str(event.data)
                )
                console.print(f"[dim cyan][USAGE] {data_dict}[/dim cyan]")
        elif event_type.startswith("session."):
            log_event("SESSION", event_type.replace("session.", ""))
        # Skip other events silently to reduce noise

    session.on(on_event)

    # Send the connection request
    initial_prompt = f"""\
Create and test the Snow CLI connection now.

Account: {account}
User: {user}
Token file: {token_file}

Execute the commands to:
1. First, add the connection using `snow connection add`
2. Then test it using `snow connection test`

After testing, tell me clearly if the connection was successful or not.
If successful, include "CONNECTION_SUCCESS" in your response.
If it failed, explain what went wrong and what the user needs to fix.
"""

    log_event("PROMPT", "Sending connection request to agent")
    await session.send({"prompt": initial_prompt})

    # Wait for completion
    await done.wait()

    # Cleanup session
    await session.destroy()

    return connection_success


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
    # Save PAT token to a temporary file for Snow CLI using NamedTemporaryFile
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".token", prefix="snowflake_pat_", delete=False
    ) as tmp:
        tmp.write(pat_token)
        token_file = Path(tmp.name)
    token_file.chmod(0o600)  # Secure permissions

    console.print(f"[dim]PAT token saved to: {token_file}[/dim]")

    try:
        # Create Copilot client
        log_event("CLIENT", "Starting Copilot client")
        client = CopilotClient()
        await client.start()
        log_event("CLIENT", "Copilot client started successfully")

        # Phase 1: Create and test connection
        connection_success = await run_connection_agent(client, account, user, token_file)

        if not connection_success:
            console.print(
                Panel(
                    "[bold red]❌ Connection setup failed![/bold red]\n\n"
                    "The Snow CLI connection could not be established.\n"
                    "Please check the error messages above and try again.\n\n"
                    "Common issues:\n"
                    "• Invalid account identifier format\n"
                    "• PAT token expired or invalid\n"
                    "• Network connectivity issues\n"
                    "• Snow CLI not installed",
                    title="Connection Failed",
                    border_style="red",
                )
            )
            await client.stop()
            return

        console.print(
            Panel(
                "[bold green]✓ Connection established successfully![/bold green]",
                title="Phase 1 Complete",
                border_style="green",
            )
        )

        # Ask user if they want to proceed with full setup
        proceed = Prompt.ask(
            "\n[bold cyan]Do you want to proceed with full Snowflake setup?[/bold cyan]",
            choices=["yes", "no"],
            default="yes",
        )

        if proceed != "yes":
            console.print("[dim]Setup cancelled by user.[/dim]")
            await client.stop()
            return

        # Phase 2: Full setup
        console.print(
            Panel(
                "[bold yellow]Phase 2: Full Snowflake Setup[/bold yellow]\n\n"
                "The agent will now set up:\n"
                "• STREAM role and STREAMEV user\n"
                "• INGESTION and CONTROL databases\n"
                "• External Volume for Iceberg\n"
                "• Events table and streaming pipe",
                title="🏗️ Infrastructure Setup",
            )
        )

        await run_full_setup_agent(client, account, user, str(token_file))

        await client.stop()

        console.print(
            Panel.fit(
                "[bold green]✓ Setup process completed![/bold green]\n\n"
                "Review the output above for any manual steps needed.",
                title="🎉 Complete",
            )
        )

    finally:
        # Clean up token file
        if token_file.exists():
            token_file.unlink()
            log_event("CLEANUP", "Removed temporary token file")


async def run_full_setup_agent(
    client: CopilotClient,
    account: str,
    user: str,
    token_file_path: str,
) -> None:
    """Run the agent for full Snowflake infrastructure setup."""
    system_prompt = get_setup_prompt(account, user, token_file_path)

    log_event("SESSION", "Creating full setup agent session")
    session = await client.create_session(
        {
            "system_message": {
                "mode": "append",
                "content": system_prompt,
            },
            "on_user_input_request": handle_user_input,
        }
    )

    done = asyncio.Event()
    reasoning_buffer = ""

    def on_event(event: Any) -> None:
        nonlocal reasoning_buffer
        event_type = event.type.value if hasattr(event.type, "value") else str(event.type)

        if event_type == "assistant.message_delta":
            delta = getattr(event.data, "delta_content", None) or ""
            console.print(delta, end="", markup=False)
        elif event_type == "assistant.reasoning_delta":
            # Show reasoning/thinking in a distinct way
            delta = (
                getattr(event.data, "delta_content", None)
                or getattr(event.data, "delta", None)
                or ""
            )
            if delta:
                reasoning_buffer += delta
                console.print(f"[dim yellow]{delta}[/dim yellow]", end="", markup=True)
        elif event_type == "assistant.reasoning":
            if reasoning_buffer:
                console.print()
                reasoning_buffer = ""
        elif event_type == "assistant.message":
            console.print()
            log_event("MESSAGE", "Assistant completed response")
        elif event_type == "tool.execution_start":
            tool_name = getattr(event.data, "tool_name", "unknown")
            log_event("TOOL", f"▶ Executing: {tool_name}")
        elif event_type == "tool.execution_complete":
            result = getattr(event.data, "result", None)
            log_event("TOOL", "✓ Tool completed")
            if result:
                result_str = str(result)[:500]
                console.print(f"[dim]  Output: {result_str}[/dim]")
        elif event_type == "session.idle":
            log_event("SESSION", "Agent finished")
            done.set()
        elif event_type == "session.error":
            # Extract and display error details
            error_msg = getattr(event.data, "message", None) or getattr(event.data, "error", None)
            error_code = getattr(event.data, "code", None)
            error_details = getattr(event.data, "details", None)

            console.print("\n[bold red]━━━ SESSION ERROR ━━━[/bold red]")
            if error_code:
                console.print(f"[red]Code: {error_code}[/red]")
            if error_msg:
                console.print(f"[red]Message: {error_msg}[/red]")
            if error_details:
                console.print(f"[red]Details: {error_details}[/red]")

            # Try to get all attributes from the event data for debugging
            if hasattr(event, "data"):
                data_dict = (
                    {k: v for k, v in vars(event.data).items() if not k.startswith("_")}
                    if hasattr(event.data, "__dict__")
                    else str(event.data)
                )
                console.print(f"[dim red]Full error data: {data_dict}[/dim red]")
            console.print("[bold red]━━━━━━━━━━━━━━━━━━━━[/bold red]\n")
            done.set()
        elif event_type in ("assistant.reasoning_delta", "ping", "pong"):
            pass  # Skip noisy events
        elif event_type == "session.usage_info":
            # Show usage info (token counts, model info)
            if hasattr(event, "data"):
                data_dict = (
                    {k: v for k, v in vars(event.data).items() if not k.startswith("_")}
                    if hasattr(event.data, "__dict__")
                    else str(event.data)
                )
                console.print(f"[dim cyan][USAGE] {data_dict}[/dim cyan]")
        elif event_type.startswith("session."):
            log_event("SESSION", event_type.replace("session.", ""))
        # Skip other events silently

    session.on(on_event)

    initial_prompt = """\
The connection is already established and working.

Now proceed with the full Snowflake setup. First, ask me for the required Azure information:
1. Azure Storage Account name
2. Azure Storage Container name
3. Azure Tenant ID

Then execute the setup steps from SNOWFLAKE_COMPLETE_SETUP.md.
"""

    log_event("PROMPT", "Sending setup request to agent")
    await session.send({"prompt": initial_prompt})
    await done.wait()
    await session.destroy()
