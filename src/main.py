"""
Main CLI application for EvSnow pipeline.

This module provides the main entry point for the pipeline that continuously:
1. Reads from Event Hub topics (configured via environment variables)
2. Sends messages to Snowflake tables (one table per topic)

Based on TYPER CLI framework with configuration management via pydantic settings.
"""

import logging

import logfire
import typer
from rich.console import Console
from rich.table import Table

from utils.cli_bootstrap import (
    configure_early_logfire,
    configure_logging,
    load_dotenv_if_present,
)
from utils.config import EvSnowConfig, load_config

# Load .env file by default if it exists
load_dotenv_if_present(caller_file=__file__)

# Early Logfire initialization to prevent "LogfireNotConfiguredWarning".
# Full configuration with user settings happens later during command execution.
configure_early_logfire()

# Initialize CLI app and console
app = typer.Typer(
    name="evsnow",
    help="EvSnow - Event Hub to Snowflake streaming pipeline",
    add_completion=False,
)
console = Console()

logger = configure_logging(console=console, level=logging.INFO, logger_name=__name__)


def _show_rbac_guidance() -> None:
    """Display Azure RBAC permission guidance for EventHub access."""
    console.print("\n[bold cyan]🔐 Required Azure RBAC Permissions[/bold cyan]")
    console.print("═" * 70)
    console.print()
    console.print("[bold]For EventHub Consumer Access:[/bold]")
    console.print("  ✓ [cyan]Azure Event Hubs Data Receiver[/cyan]")
    console.print("    Scope: Event Hub Namespace or Resource Group")
    console.print("    Purpose: Read messages from EventHub partitions")
    console.print()
    console.print("  i [cyan]Azure Event Hubs Data Sender[/cyan] is only needed for sender tools")
    console.print("    Checkpoints are stored in Snowflake/Postgres, not Event Hubs")
    console.print()
    console.print("[bold]How to Assign in Azure Portal:[/bold]")
    console.print("  1. Navigate to your Event Hub Namespace")
    console.print("  2. Click 'Access Control (IAM)'")
    console.print("  3. Click '+ Add' → 'Add role assignment'")
    console.print("  4. Select 'Azure Event Hubs Data Receiver'")
    console.print("  5. Click 'Next', select your user/service principal")
    console.print("  6. Click 'Review + assign'")
    console.print()
    console.print("[bold yellow]⚠️  Permission errors appear at runtime:[/bold yellow]")
    console.print("  • You'll see 'AuthenticationError' when trying to connect")
    console.print("  • Check logs for 'not authorized' or 'permission' errors")
    console.print("  • The pipeline will fail immediately if permissions are missing")
    console.print()
    console.print("[bold]Troubleshooting Resources:[/bold]")
    console.print(
        "  • [link=https://learn.microsoft.com/azure/event-hubs/troubleshoot-authentication-authorization]Azure EventHub Auth Troubleshooting[/link]"
    )
    console.print(
        "  • [link=https://learn.microsoft.com/azure/event-hubs/authenticate-managed-identity]Managed Identity Setup[/link]"
    )
    console.print()


def _initialize_logfire(logfire_config) -> None:
    """Initialize Logfire observability if enabled."""

    if not logfire_config.enabled:
        logger.info("Logfire observability disabled")
        return

    logger.info("Initializing Logfire observability for service: %s", logfire_config.service_name)

    try:
        logfire.configure(
            token=logfire_config.token if logfire_config.send_to_logfire else None,
            service_name=logfire_config.service_name,
            environment=logfire_config.environment,
            send_to_logfire=logfire_config.send_to_logfire,
            console=logfire.ConsoleOptions(
                verbose=logfire_config.console_logging,
                min_log_level=logfire_config.log_level.lower(),
            )
            if logfire_config.console_logging
            else False,
        )

        if logfire_config.send_to_logfire or logfire_config.console_logging:
            root_logger = logging.getLogger()
            logfire_handler = logfire.LogfireLoggingHandler()
            logfire_handler.setLevel(getattr(logging, logfire_config.log_level.upper()))
            root_logger.addHandler(logfire_handler)

        try:
            logfire.instrument_pydantic_ai()
            logger.info("✅ Pydantic AI instrumentation enabled")
        except Exception as pydantic_error:
            logger.warning("⚠️ Could not instrument Pydantic AI: %s", pydantic_error)

        logger.info(
            "✅ Logfire initialized - Cloud: %s, Console: %s, Level: %s",
            logfire_config.send_to_logfire,
            logfire_config.console_logging,
            logfire_config.log_level,
        )
    except Exception as exc:
        logger.warning("Failed to initialize Logfire: %s", exc)
        logger.warning("Pipeline will continue without Logfire observability")


@app.command()
def check_credentials() -> None:
    """Check which Azure credentials are available and will be used."""
    from azure.identity import (
        AzureCliCredential,
        EnvironmentCredential,
        ManagedIdentityCredential,
    )

    console.print("\n[bold blue]🔍 Checking Available Azure Credentials...[/bold blue]\n")

    console.print("[bold]Credential availability check:[/bold]")
    console.print("1. Environment variables")
    console.print("2. Managed Identity (if running in Azure)")
    console.print("3. Azure CLI")
    console.print("4. Azure PowerShell")
    console.print("5. Interactive browser\n")

    console.print(
        "[dim]Event Hub consumption currently uses Azure CLI credentials unless a per-hub "
        "connection string is configured.[/dim]\n"
    )

    # Check each credential type
    console.print("[bold]Available Credentials:[/bold]\n")

    # Environment credentials
    try:
        _env_cred = EnvironmentCredential()
        console.print("✅ [green]Environment variables[/green] - Available")
        console.print("   (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)\n")
    except Exception as e:
        console.print("❌ [dim]Environment variables - Not available[/dim]")
        console.print(f"   [dim]{e!s}[/dim]\n")

    # Managed Identity
    try:
        _msi_cred = ManagedIdentityCredential()
        console.print("✅ [yellow bold]Managed Identity[/yellow bold] - Available")
        console.print(
            "   [dim]Available for services that use DefaultAzureCredential, such as Postgres azure_token auth.[/dim]"
        )
        console.print(
            "   [yellow]Event Hub consumption does not use Managed Identity in the current runtime path.[/yellow]\n"
        )
    except Exception:
        console.print("❌ [dim]Managed Identity - Not available[/dim]")
        console.print("   [dim](Not running in Azure environment)[/dim]\n")

    # Azure CLI
    try:
        _cli_cred = AzureCliCredential()
        console.print("✅ [green]Azure CLI[/green] - Available")
        console.print("   [green](Will be used for Event Hub receiver authentication)[/green]\n")
    except Exception as e:
        console.print("❌ [dim]Azure CLI - Not available[/dim]")
        console.print(f"   [dim]{e!s}[/dim]\n")

    # Show conclusion
    console.print("\n[bold yellow]⚠️  CONCLUSION:[/bold yellow]")
    console.print(
        "[green]• Event Hub receiver uses AZURE CLI credentials unless EVENTHUBNAME_{N}_CONNECTION_STRING is set[/green]"
    )
    console.print("[green]• Ensure your CLI user has required RBAC roles[/green]")
    console.print("\n[bold]Next Steps:[/bold]")
    console.print("1. Go to Azure Portal → EventHub Namespace")
    console.print("2. Access Control (IAM) → Role Assignments")
    console.print("3. Verify your user has 'Azure Event Hubs Data Receiver' role")


@app.command()
def validate_config(
    env_file: str | None = typer.Option(
        None,
        "--env-file",
        "-e",
        help="Path to environment file (.env)",
    ),
    show_rbac: bool = typer.Option(
        False,
        "--show-rbac",
        help="Show Azure RBAC permission requirements",
    ),
) -> None:
    """Validate the configuration and display summary."""
    try:
        console.print("[bold blue]Loading configuration...[/bold blue]")

        config = load_config(env_file)
        validation_results = config.validate_configuration()

        if validation_results["valid"]:
            console.print("[bold green]✓ Configuration is valid![/bold green]")

            # Show RBAC guidance if requested
            if show_rbac:
                _show_rbac_guidance()

            # Check and create control table if needed
            try:
                import os

                target_db = os.getenv("TARGET_DB")
                target_schema = os.getenv("TARGET_SCHEMA")
                target_table = os.getenv("TARGET_TABLE")
                use_hybrid_table = config.use_hybrid_table
                control_backend = config.control_table_backend

                if target_db and target_schema and target_table:
                    if control_backend == "postgres":
                        console.print(
                            "\n[bold blue]Verifying control table (Postgres):[/bold blue] "
                            f"{target_db}.{target_schema}.{target_table}"
                        )
                        if not config.control_postgres:
                            console.print(
                                "[yellow]⚠ Warning: Postgres control table backend selected but CONTROL_PG_* settings are missing[/yellow]"
                            )
                        else:
                            from utils.postgres import create_control_table

                            try:
                                if create_control_table(
                                    target_db=target_db,
                                    target_schema=target_schema,
                                    target_table=target_table,
                                    config=config.control_postgres,
                                ):
                                    console.print(
                                        "[green]✓ Postgres control table verified/created successfully[/green]"
                                    )
                                else:
                                    console.print(
                                        "[yellow]⚠ Warning: Could not verify Postgres control table[/yellow]"
                                    )
                            except Exception as pg_error:
                                console.print(
                                    f"[yellow]⚠ Warning: Could not verify Postgres control table: {pg_error}[/yellow]"
                                )
                                logger.warning(
                                    f"Postgres control table verification failed: {pg_error}"
                                )
                    else:
                        table_type = "hybrid table" if use_hybrid_table else "table"
                        console.print(
                            f"\n[bold blue]Verifying control {table_type}:[/bold blue] {target_db}.{target_schema}.{target_table}"
                        )

                        # Use Snowflake control table
                        from utils.snowflake import create_control_table

                        try:
                            if create_control_table(
                                target_db=target_db,
                                target_schema=target_schema,
                                target_table=target_table,
                                config=config.snowflake_connection,
                                use_hybrid_table=use_hybrid_table,
                            ):
                                console.print(
                                    "[green]✓ Snowflake control table verified/created successfully[/green]"
                                )
                            else:
                                console.print(
                                    "[yellow]⚠ Warning: Could not verify Snowflake control table[/yellow]"
                                )
                        except Exception as sf_error:
                            console.print(
                                f"[yellow]⚠ Warning: Could not verify Snowflake control table: {sf_error}[/yellow]"
                            )
                            logger.warning(
                                f"Snowflake control table verification failed: {sf_error}"
                            )
                else:
                    console.print(
                        "[yellow]⚠ Control table settings not found in environment (TARGET_DB, TARGET_SCHEMA, TARGET_TABLE)[/yellow]"
                    )

            except Exception as e:
                console.print(f"[yellow]⚠ Warning: Could not verify control table: {e}[/yellow]")
                logger.warning(f"Control table verification failed: {e}")
        else:
            console.print("[bold red]✗ Configuration has errors![/bold red]")

        # Create summary table
        table = Table(title="Configuration Summary")
        table.add_column("Component", style="cyan")
        table.add_column("Count", justify="right", style="magenta")

        table.add_row("Event Hubs", str(validation_results["event_hubs_count"]))
        table.add_row(
            "Snowflake Configs", str(validation_results.get("snowflake_configs_count", 0))
        )
        table.add_row("Mappings", str(validation_results["mappings_count"]))

        console.print(table)

        # Display warnings
        if validation_results["warnings"]:
            console.print("\n[bold yellow]Warnings:[/bold yellow]")
            for warning in validation_results["warnings"]:
                console.print(f"  [yellow]⚠[/yellow] {warning}")

        # Display errors
        if validation_results["errors"]:
            console.print("\n[bold red]Errors:[/bold red]")
            for error in validation_results["errors"]:
                console.print(f"  [red]✗[/red] {error}")

        # Display detailed configurations
        if typer.confirm("\nShow detailed configuration?", default=False):
            _show_detailed_config(config)

    except Exception as e:
        console.print(f"[bold red]Configuration error:[/bold red] {e}")
        raise typer.Exit(1) from e


@app.command()
def run(
    env_file: str | None = typer.Option(
        None,
        "--env-file",
        "-e",
        help="Path to environment file (.env)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Validate configuration and show what would be processed without actually running",
    ),
    smart: bool = typer.Option(
        False,
        "--smart",
        help="Enable LLM-powered smart retry analysis for failures",
    ),
    capture: bool = typer.Option(
        False,
        "--capture",
        help="Capture each raw Event Hub message to messages/f_{timestamp}.json",
    ),
) -> None:
    """Run the ELT pipeline continuously."""
    try:
        console.print("[bold blue]Starting EvSnow Pipeline...[/bold blue]")

        # Load and validate configuration
        config = load_config(env_file)

        # Optional runtime-only toggles
        config.capture_messages = capture
        validation_results = config.validate_configuration()

        if not validation_results["valid"] and validation_results["errors"]:
            console.print("[bold red]Configuration has errors. Please fix them first.[/bold red]")
            for error in validation_results["errors"]:
                console.print(f"  [red]✗[/red] {error}")
            raise typer.Exit(1)

        # Initialize Logfire observability
        _initialize_logfire(config.logfire)

        if validation_results["warnings"]:
            console.print("[yellow]Configuration warnings:[/yellow]")
            for warning in validation_results["warnings"]:
                console.print(f"  [yellow]⚠[/yellow] {warning}")

        # Initialize retry manager
        retry_manager = None
        if smart:
            console.print("\n[bold cyan]🤖 Smart Retry Mode Enabled[/bold cyan]")
            console.print("   Using LLM to analyze exceptions and decide on retries")
            try:
                from utils.config import SmartRetryConfig
                from utils.smart_retry import RetryManager

                smart_config = SmartRetryConfig()

                # Validate API key is provided
                if not smart_config.llm_api_key:
                    console.print("[bold red]❌ Smart retry requires an LLM API key![/bold red]")
                    console.print("   Set SMART_RETRY_LLM_API_KEY in your .env file or environment")
                    raise typer.Exit(1)

                retry_manager = RetryManager(
                    smart_enabled=True,
                    max_attempts=smart_config.max_attempts,
                    llm_provider=smart_config.llm_provider,
                    llm_model=smart_config.llm_model,
                    llm_api_key=smart_config.llm_api_key,
                    llm_endpoint=smart_config.llm_endpoint,
                    timeout_seconds=smart_config.timeout_seconds,
                    enable_caching=smart_config.enable_caching,
                )
                console.print(f"   Provider: [cyan]{smart_config.llm_provider}[/cyan]")
                console.print(f"   Model: [cyan]{smart_config.llm_model}[/cyan]")
                if smart_config.llm_endpoint:
                    console.print(f"   Endpoint: [cyan]{smart_config.llm_endpoint[:50]}...[/cyan]")
                console.print(f"   Max attempts: [cyan]{smart_config.max_attempts}[/cyan]")
            except Exception as e:
                console.print(f"[red]❌ Failed to initialize smart retry: {e}[/red]")
                raise typer.Exit(1) from e
        else:
            console.print("\n[cyan]🔧 Standard Retry Mode Enabled[/cyan]")
            console.print("   Using fixed retry attempts with exponential backoff")
            from utils.smart_retry import RetryManager

            retry_manager = RetryManager(
                smart_enabled=False,
                max_attempts=3,
            )
            console.print("   Max attempts: [cyan]3[/cyan]")

        # Show RBAC permission reminder
        console.print(
            "\n[dim]� Tip: Run [bold]validate-config --show-rbac[/bold] to see required Azure permissions[/dim]"
        )

        if dry_run:
            console.print(
                "\n[bold yellow]DRY RUN MODE - No actual processing will occur[/bold yellow]"
            )
            _show_processing_plan(config)
            return

        # Start the pipeline
        console.print(f"\n[green]Starting pipeline with {len(config.mappings)} mappings...[/green]")

        # Import here to avoid circular imports
        import asyncio

        from pipeline.orchestrator import run_pipeline

        # Run the async pipeline with retry manager
        asyncio.run(run_pipeline(config, retry_manager=retry_manager))

    except KeyboardInterrupt:
        console.print("\n[yellow]Pipeline stopped by user[/yellow]")
        raise typer.Exit(0) from None
    except Exception as e:
        error_str = str(e).lower()

        # Check if it's an authentication/permission error
        if (
            "authenticationerror" in error_str
            or "not authorized" in error_str
            or "permission" in error_str
            or "unauthorized" in error_str
        ):
            console.print(f"\n[bold red]❌ Authentication/Permission Error:[/bold red] {e}")
            console.print()
            console.print("[bold yellow]� You lack required Azure RBAC permissions![/bold yellow]")
            console.print()
            console.print("[bold]Required Roles:[/bold]")
            console.print(
                "  • [cyan]Azure Event Hubs Data Receiver[/cyan] - to read EventHub messages"
            )
            console.print(
                "  • [cyan]Azure Event Hubs Data Sender[/cyan] - only if you use sender tooling"
            )
            console.print()
            console.print("[bold]How to Fix:[/bold]")
            console.print("  1. Go to Azure Portal → Your Event Hub Namespace")
            console.print("  2. Click 'Access Control (IAM)' → '+ Add' → 'Add role assignment'")
            console.print(
                "  3. Assign 'Azure Event Hubs Data Receiver' to your user/service principal"
            )
            console.print("  4. Do not add Sender unless this identity also publishes test events")
            console.print()
            console.print(
                "[dim]Run: [bold]uv run evsnow validate-config --show-rbac[/bold] for detailed guidance[/dim]"
            )
        else:
            console.print(f"\n[bold red]Pipeline error:[/bold red] {e}")
            logger.exception("Unexpected error in pipeline")
            console.print()
            console.print("[bold yellow]💡 TROUBLESHOOTING:[/bold yellow]")
            console.print("   • Check your .env file configuration")
            console.print("   • Verify EventHub namespace and connection settings")
            console.print("   • Ensure Snowflake token is valid")
            console.print("   • Run: [bold]uv run evsnow validate-config[/bold] to check configuration")
            console.print(
                "   • Run: [bold]uv run evsnow validate-config --show-rbac[/bold] for permission guidance"
            )

        raise typer.Exit(1) from e


def _show_detailed_config(config: EvSnowConfig) -> None:
    """Display detailed configuration information."""

    # Event Hubs
    if config.event_hubs:
        console.print("\n[bold cyan]Event Hub Configurations:[/bold cyan]")
        for key, eh_config in config.event_hubs.items():
            table = Table(title=f"Event Hub: {key}")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="white")

            table.add_row("Name", eh_config.name)
            table.add_row("Namespace", eh_config.namespace)
            table.add_row("Consumer Group", eh_config.consumer_group)
            table.add_row("Max Batch Size", str(eh_config.max_batch_size))
            table.add_row("Max Wait Time", f"{eh_config.max_wait_time}s")
            table.add_row("Prefetch Count", str(eh_config.prefetch_count))

            console.print(table)

    # Snowflake Configs
    if config.snowflake_configs:
        console.print("\n[bold cyan]Snowflake Configurations:[/bold cyan]")
        for key, sf_config in config.snowflake_configs.items():
            table = Table(title=f"Snowflake: {key}")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="white")

            table.add_row("Database", sf_config.database)
            table.add_row("Schema", sf_config.schema_name)
            table.add_row("Table", sf_config.table_name)
            table.add_row("Batch Size", str(sf_config.batch_size))

            console.print(table)

    # Mappings
    if config.mappings:
        console.print("\n[bold cyan]Event Hub ↔ Snowflake Mappings:[/bold cyan]")
        for i, mapping in enumerate(config.mappings, 1):
            table = Table(title=f"Mapping {i}")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="white")

            table.add_row("Event Hub", mapping.event_hub_key)
            table.add_row("Destination Type", "Snowflake")
            table.add_row("Snowflake", mapping.snowflake_key)
            table.add_row("Channel Pattern", mapping.channel_name_pattern)

            console.print(table)


def _show_processing_plan(config: EvSnowConfig) -> None:
    """Show what would be processed in dry-run mode."""
    console.print("\n[bold cyan]Processing Plan:[/bold cyan]")

    for mapping in config.mappings:
        eh_config = config.get_event_hub_config(mapping.event_hub_key)
        md_config = config.get_snowflake_config(mapping.snowflake_key)

        if eh_config and md_config:
            console.print(
                f"\n[green]Mapping:[/green] {mapping.event_hub_key} → {mapping.snowflake_key}"
            )
            console.print(
                f"  [cyan]Source:[/cyan] Event Hub '{eh_config.name}' in '{eh_config.namespace}'"
            )
            console.print(
                f"  [cyan]Target:[/cyan] Snowflake '{md_config.database}.{md_config.schema_name}.{md_config.table_name}'"
            )
            console.print(f"  [cyan]Batch Size:[/cyan] {eh_config.max_batch_size} messages")
            console.print(f"  [cyan]Max Wait:[/cyan] {eh_config.max_wait_time} seconds")


@app.command()
def status(
    env_file: str | None = typer.Option(
        None,
        "--env-file",
        "-e",
        help="Path to environment file (.env)",
    ),
) -> None:
    """Show pipeline status and health check."""
    try:
        console.print("[bold blue]Pipeline Status Check[/bold blue]")

        # Load configuration
        config = load_config(env_file)
        validation_results = config.validate_configuration()

        # Show configuration status
        if validation_results["valid"]:
            console.print("[green]✓ Configuration is valid[/green]")
        else:
            console.print("[red]✗ Configuration has errors[/red]")

        # Show mapping summary
        table = Table(title="Configured Mappings")
        table.add_column("EventHub", style="cyan")
        table.add_column("Snowflake", style="magenta")
        table.add_column("Status", style="green")

        for mapping in config.mappings:
            eh_config = config.get_event_hub_config(mapping.event_hub_key)
            md_config = config.get_snowflake_config(mapping.snowflake_key)

            if eh_config and md_config:
                table.add_row(
                    f"{eh_config.namespace}/{eh_config.name}",
                    f"{md_config.database}.{md_config.schema_name}.{md_config.table_name}",
                    "Ready" if validation_results["valid"] else "Config Error",
                )

        console.print(table)

        # Test connections if configuration is valid
        if validation_results["valid"]:
            console.print("\n[bold cyan]Testing Connections...[/bold cyan]")

            # Test Snowflake connection
            try:
                from utils.snowflake import check_connection

                if check_connection():
                    console.print("[green]✓ Snowflake connection successful[/green]")
                else:
                    console.print("[red]✗ Snowflake connection failed[/red]")
            except Exception as e:
                console.print(f"[red]✗ Snowflake connection error: {e}[/red]")

            console.print("\n[yellow]Note: EventHub connections are tested during runtime[/yellow]")

    except Exception as e:
        console.print(f"[bold red]Status check error:[/bold red] {e}")
        raise typer.Exit(1) from e


@app.command()
def monitor(
    log_file: str | None = typer.Option(
        None,
        "--log-file",
        "-l",
        help="Path to log file (default: pipeline_monitor.log in current directory)",
    ),
) -> None:
    """Launch interactive monitoring UI for the pipeline."""
    try:
        console.print("[red]Monitor UI not yet implemented[/red]")
        raise typer.Exit(1)

    except Exception as e:
        console.print(f"[bold red]Monitor UI error:[/bold red] {e}")
        logger.exception("Error starting monitor UI")
        raise typer.Exit(1) from e


@app.command()
def version() -> None:
    """Show version information."""
    console.print("EvSnow v0.1.0")
    console.print("EventHub to Snowflake streaming pipeline")
    console.print("\nComponents:")
    console.print("  • Azure EventHub async consumer with custom checkpointing")
    console.print("  • Snowflake high-performance ingestion")
    console.print("  • Pipeline orchestrator with concurrent mapping management")


@app.callback()
def main() -> None:
    """EvSnow - EventHub to Snowflake pipeline."""
    pass


def cli_main():
    """Entry point for the CLI when installed as a package."""
    app()


if __name__ == "__main__":
    app()
