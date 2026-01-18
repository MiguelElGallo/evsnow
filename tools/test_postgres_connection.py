"""Quick Postgres connectivity check using .env settings."""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from utils.config_models import PostgresConnectionConfig
from utils.postgres import get_connection


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("azure.identity").setLevel(logging.INFO)
    logging.getLogger("azure.identity.aio").setLevel(logging.INFO)


def _load_env(env_file: Path | None) -> None:
    if env_file and env_file.exists():
        load_dotenv(env_file)
        logging.info("Loaded env file: %s", env_file)
        return

    default_env = Path.cwd() / ".env"
    if default_env.exists():
        load_dotenv(default_env)
        logging.info("Loaded env file: %s", default_env)
        return

    logging.warning("No .env file found; relying on existing environment variables")


def _normalize_postgres_identifier(value: str) -> str:
    cleaned = value.strip()
    if cleaned.startswith('"') and cleaned.endswith('"') and len(cleaned) > 1:
        return cleaned[1:-1]
    return cleaned.lower()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Postgres control-table connectivity using .env settings."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Path to .env file (defaults to ./ .env if present).",
    )
    parser.add_argument(
        "--query",
        default="SELECT current_user, current_database(), version()",
        help="Validation query to run after connecting.",
    )
    args = parser.parse_args()

    _configure_logging()
    _load_env(args.env_file)

    target_db = os.getenv("TARGET_DB")
    target_schema = os.getenv("TARGET_SCHEMA", "")
    target_table = os.getenv("TARGET_TABLE", "")
    backend = os.getenv("CONTROL_TABLE_BACKEND", "snowflake")

    if not target_db:
        logging.error("TARGET_DB is required for Postgres connection tests.")
        return 1

    target_db = _normalize_postgres_identifier(target_db)
    if target_schema:
        target_schema = _normalize_postgres_identifier(target_schema)
    if target_table:
        target_table = _normalize_postgres_identifier(target_table)

    logging.info("CONTROL_TABLE_BACKEND=%s", backend)
    logging.info(
        "Target control table: %s.%s.%s",
        target_db,
        target_schema or "<unset>",
        target_table or "<unset>",
    )

    try:
        config = PostgresConnectionConfig()
    except Exception as exc:
        logging.exception("Failed to load CONTROL_PG_* settings: %s", exc)
        return 1

    logging.info(
        "Postgres config: host=%s port=%s user=%s sslmode=%s auth_mode=%s",
        config.host,
        config.port,
        config.user,
        config.sslmode,
        config.auth_mode,
    )

    try:
        conn = get_connection(config, target_db, use_cache=False)
    except Exception as exc:
        logging.exception("Postgres connection failed: %s", exc)
        return 2

    try:
        with conn.cursor() as cursor:
            cursor.execute(args.query)
            rows = cursor.fetchall()
            logging.info("Query succeeded. Result: %s", rows)
    except Exception as exc:
        logging.exception("Query failed: %s", exc)
        return 3
    finally:
        with contextlib.suppress(Exception):
            conn.close()

    logging.info("Postgres connectivity test completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
