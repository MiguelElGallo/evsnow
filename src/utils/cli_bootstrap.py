"""CLI bootstrap helpers.

This module centralizes CLI startup concerns:
- Optional `.env` loading
- Consistent Rich logging configuration
- Early Logfire initialization to avoid configuration warnings
- Final Logfire configuration based on user settings

Keeping these in one place reduces the size/complexity of the CLI entrypoint.
"""

from __future__ import annotations

import contextlib
import logging
from pathlib import Path
from typing import Any

import logfire
from logfire.exceptions import LogfireConfigError
from rich.console import Console
from rich.logging import RichHandler


def load_dotenv_if_present(*, caller_file: str) -> None:
    """Load a `.env` file if present.

    Searches in:
    1) Current working directory
    2) Project root inferred from the caller's file location

    This is intentionally best-effort and silent if python-dotenv is not installed.
    """

    try:
        from dotenv import load_dotenv

        env_path = Path.cwd() / ".env"
        if not env_path.exists():
            # Project root is the repository root (where pyproject.toml is expected).
            project_root = Path(caller_file).resolve().parent.parent.parent
            env_path = project_root / ".env"

        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        # python-dotenv not installed, environment variables should be set manually
        return


def configure_early_logfire() -> None:
    """Configure Logfire with a minimal no-op config.

    Some imported modules may start spans or log via Logfire early during import.
    This prevents LogfireNotConfiguredWarning while keeping sending disabled.
    """

    with contextlib.suppress(LogfireConfigError):
        logfire.configure(
            send_to_logfire=False,
            console=False,
        )


def configure_logging(
    *,
    console: Console,
    level: int = logging.INFO,
    logger_name: str = "__main__",
) -> logging.Logger:
    """Configure Rich logging and return a logger for the requested name."""

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    # Suppress verbose Azure SDK logs
    logging.getLogger("azure.eventhub").setLevel(logging.WARNING)
    logging.getLogger("azure.eventhub._pyamqp").setLevel(logging.WARNING)
    logging.getLogger("azure.identity").setLevel(logging.WARNING)
    logging.getLogger("azure.identity.aio").setLevel(logging.WARNING)

    return logging.getLogger(logger_name)


def initialize_logfire(*, logfire_config: Any, logger: logging.Logger) -> None:
    """Initialize Logfire observability if enabled."""

    if not getattr(logfire_config, "enabled", False):
        logger.info("Logfire observability disabled")
        return

    service_name = getattr(logfire_config, "service_name", "evsnow")
    logger.info("Initializing Logfire observability for service: %s", service_name)

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

        # Instrument Pydantic AI for automatic LLM call tracing (best-effort)
        try:
            logfire.instrument_pydantic_ai()
            logger.info("✅ Pydantic AI instrumentation enabled")
        except Exception as pydantic_error:  # pragma: no cover
            logger.warning("⚠️ Could not instrument Pydantic AI: %s", pydantic_error)

        logger.info(
            "✅ Logfire initialized - Cloud: %s, Console: %s, Level: %s",
            logfire_config.send_to_logfire,
            logfire_config.console_logging,
            logfire_config.log_level,
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to initialize Logfire: %s", exc)
        logger.warning("Pipeline will continue without Logfire observability")
