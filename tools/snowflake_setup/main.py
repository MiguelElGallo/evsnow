#!/usr/bin/env python3
"""
Snowflake Setup CLI - Automated Snowflake configuration with Copilot Agent.

This CLI guides users through:
1. Creating authentication policy for PAT support
2. Generating PAT (Programmatic Access Token) SQL
3. Creating Snow CLI connection
4. Setting up complete Snowflake infrastructure from SNOWFLAKE_COMPLETE_SETUP.md
"""

import asyncio
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.syntax import Syntax

# Handle imports - add parent to path for local module resolution
sys.path.insert(0, str(Path(__file__).parent))
from agent import run_setup_agent
from sql_templates import generate_pat_prerequisites_sql, generate_pat_sql

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
        help="Snowflake account identifier (e.g., VWBIVWS-SV93109)",
    ),
    user: str = typer.Option(
        None,
        "--user",
        "-u",
        help="Snowflake username",
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
    # Prompt for account if not provided
    if not account:
        account = Prompt.ask(
            "[bold cyan]Enter your Snowflake account identifier[/bold cyan]",
            default="",
        )
        if not account:
            console.print("[bold red]Error: Snowflake account is required.[/bold red]")
            raise typer.Exit(1)

    # Prompt for user if not provided
    if not user:
        user = Prompt.ask(
            "[bold cyan]Enter your Snowflake username[/bold cyan]",
            default="",
        )
        if not user:
            console.print("[bold red]Error: Snowflake username is required.[/bold red]")
            raise typer.Exit(1)

    asyncio.run(_run_setup(account, user, token_name, days_to_expiry))


async def _run_setup(
    account: str,
    user: str,
    token_name: str,
    days_to_expiry: int,
) -> None:
    """Main setup orchestration."""
    console.print(
        Panel.fit(
            "[bold blue]Snowflake Setup Wizard[/bold blue]\n"
            "This wizard will help you set up your complete Snowflake infrastructure.",
            title="🏔️ EvSnow Setup",
        )
    )

    # Step 1: Generate and display prerequisites SQL (authentication policy)
    prereq_sql = generate_pat_prerequisites_sql(user)

    console.print(
        "\n[bold yellow]Step 1: Create Authentication Policy (PAT Prerequisites)[/bold yellow]"
    )
    console.print(
        "\n[bold]PAT requires an authentication policy to bypass network policy requirements.[/bold]"
    )
    console.print("Run the following SQL in your Snowflake worksheet as ACCOUNTADMIN:")
    console.print("[yellow]─── Prerequisites SQL (Run First) ───[/yellow]\n")

    syntax = Syntax(prereq_sql, "sql", theme="monokai", line_numbers=False, word_wrap=True)
    console.print(syntax)
    console.print("[yellow]───────────────────────────────────────[/yellow]")

    # Wait for user to confirm they ran the prerequisites
    prereq_done = Confirm.ask(
        "\n[bold cyan]Have you run the prerequisites SQL above?[/bold cyan]",
        default=False,
    )
    if not prereq_done:
        console.print(
            "[bold red]Please run the prerequisites SQL first, then restart the setup.[/bold red]"
        )
        raise typer.Exit(1)

    # Step 2: Generate and display PAT SQL
    pat_sql = generate_pat_sql(user, token_name, days_to_expiry)

    console.print("\n[bold yellow]Step 2: Create Programmatic Access Token (PAT)[/bold yellow]")
    console.print("\nRun the following SQL in your Snowflake worksheet as ACCOUNTADMIN:")
    console.print("[green]─── PAT Creation SQL ───[/green]\n")

    syntax = Syntax(pat_sql, "sql", theme="monokai", line_numbers=False, word_wrap=True)
    console.print(syntax)
    console.print("[green]────────────────────────[/green]")

    console.print(
        "\n[bold]After running the SQL, copy the 'token_secret' value from the output.[/bold]"
    )
    console.print("[dim]The token appears ONLY ONCE - save it immediately![/dim]\n")

    # Step 3: Get the PAT from user
    pat_token = Prompt.ask(
        "[bold cyan]Paste the token_secret here[/bold cyan]",
        password=True,  # Hide input for security
    )

    if not pat_token or len(pat_token) < 10:
        console.print(
            "[bold red]Error: Invalid token provided. Please run the setup again.[/bold red]"
        )
        raise typer.Exit(1)

    console.print("\n[bold green]✓ Token received![/bold green]")

    # Step 4: Start the Copilot agent
    console.print("\n[bold yellow]Step 3: Starting AI Agent for automated setup...[/bold yellow]\n")

    await run_setup_agent(
        account=account,
        user=user,
        pat_token=pat_token,
    )


@app.command()
def validate() -> None:
    """Validate the current Snowflake setup."""
    console.print("[bold]Validating Snowflake setup...[/bold]")
    console.print("[dim]This feature is coming soon.[/dim]")


@app.command()
def version() -> None:
    """Show version information."""
    __version__ = "0.1.0"
    console.print(f"snowflake-setup-cli v{__version__}")


def cli_main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    cli_main()
