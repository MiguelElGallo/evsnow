"""Postgres utilities for control table storage."""

import contextlib
import json
import logging
from typing import Any

import psycopg
from azure.identity import DefaultAzureCredential
from psycopg import sql

from utils.config_models import PostgresConnectionConfig

logger = logging.getLogger(__name__)

_connection_cache: dict[str, psycopg.Connection] = {}
_azure_credential: DefaultAzureCredential | None = None

_AZURE_TOKEN_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"


def _get_cache_key(config: PostgresConnectionConfig, database: str) -> str:
    return f"{config.host}:{config.port}:{config.user}:{database}:{config.sslmode}:{config.auth_mode}"


def _get_azure_credential() -> DefaultAzureCredential:
    global _azure_credential
    if _azure_credential is None:
        _azure_credential = DefaultAzureCredential()
    return _azure_credential


def _get_password(config: PostgresConnectionConfig) -> str | None:
    if config.auth_mode == "password":
        return config.password
    logger.info("Requesting Azure token for Postgres user %s", config.user)
    credential = _get_azure_credential()
    try:
        token = credential.get_token(_AZURE_TOKEN_SCOPE)
    except Exception as exc:
        logger.error("Failed to obtain Azure token for Postgres: %s", exc, exc_info=True)
        raise
    logger.info("Obtained Azure token for Postgres (expires_on=%s)", token.expires_on)
    return token.token


def _is_connection_alive(conn: psycopg.Connection) -> bool:
    try:
        return not conn.closed
    except Exception:
        return False


def get_connection(
    config: PostgresConnectionConfig,
    database: str,
    use_cache: bool = True,
) -> psycopg.Connection:
    if use_cache:
        cache_key = _get_cache_key(config, database)
        cached_conn = _connection_cache.get(cache_key)
        if cached_conn and _is_connection_alive(cached_conn):
            return cached_conn
        if cached_conn:
            _connection_cache.pop(cache_key, None)

    logger.info(
        "Connecting to Postgres control DB %s at %s:%s as %s (auth_mode=%s)",
        database,
        config.host,
        config.port,
        config.user,
        config.auth_mode,
    )
    conn = psycopg.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=_get_password(config),
        dbname=database,
        sslmode=config.sslmode,
    )
    conn.autocommit = True

    if use_cache:
        cache_key = _get_cache_key(config, database)
        _connection_cache[cache_key] = conn

    return conn


def close_cached_connections() -> None:
    for cache_key, conn in list(_connection_cache.items()):
        try:
            conn.close()
        except Exception:
            logger.debug("Failed to close Postgres connection: %s", cache_key)
        finally:
            _connection_cache.pop(cache_key, None)


def create_control_table(
    target_db: str,
    target_schema: str,
    target_table: str,
    config: PostgresConnectionConfig,
) -> bool:
    conn: psycopg.Connection | None = None
    try:
        conn = get_connection(config, target_db, use_cache=False)
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(target_schema)
                )
            )
            cursor.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        ts_inserted TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        eventhub_namespace TEXT NOT NULL,
                        eventhub TEXT NOT NULL,
                        target_db TEXT NOT NULL,
                        target_schema TEXT NOT NULL,
                        target_table TEXT NOT NULL,
                        waterlevel BIGINT,
                        partition_id TEXT NOT NULL,
                        metadata JSONB,
                        PRIMARY KEY (
                            eventhub_namespace,
                            eventhub,
                            target_db,
                            target_schema,
                            target_table,
                            partition_id
                        )
                    )
                    """
                ).format(sql.Identifier(target_schema), sql.Identifier(target_table))
            )

        logger.info(
            "Control table verified/created: %s.%s.%s",
            target_db,
            target_schema,
            target_table,
        )
        return True
    finally:
        if conn is not None:
            with contextlib.suppress(Exception):
                conn.close()


def insert_partition_checkpoint(
    eventhub_namespace: str,
    eventhub: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    partition_id: str,
    waterlevel: int,
    metadata: dict[str, Any] | None = None,
    config: PostgresConnectionConfig | None = None,
    control_db: str | None = None,
    control_schema: str | None = None,
    control_table: str | None = None,
) -> None:
    if config is None:
        config = PostgresConnectionConfig()

    actual_control_db = control_db or target_db
    actual_control_schema = control_schema or target_schema
    actual_control_table = control_table or "INGESTION_STATUS"

    conn = get_connection(config, actual_control_db, use_cache=True)
    metadata_json = json.dumps(metadata) if metadata else None

    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {}.{} (
                    ts_inserted,
                    eventhub_namespace,
                    eventhub,
                    target_db,
                    target_schema,
                    target_table,
                    waterlevel,
                    partition_id,
                    metadata
                )
                VALUES (
                    CURRENT_TIMESTAMP,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s::jsonb
                )
                ON CONFLICT (
                    eventhub_namespace,
                    eventhub,
                    target_db,
                    target_schema,
                    target_table,
                    partition_id
                )
                DO UPDATE SET
                    waterlevel = EXCLUDED.waterlevel,
                    ts_inserted = EXCLUDED.ts_inserted,
                    metadata = EXCLUDED.metadata
                """
            ).format(
                sql.Identifier(actual_control_schema),
                sql.Identifier(actual_control_table),
            ),
            (
                eventhub_namespace,
                eventhub,
                target_db,
                target_schema,
                target_table,
                waterlevel,
                partition_id,
                metadata_json,
            ),
        )


def get_partition_checkpoints(
    eventhub_namespace: str,
    eventhub: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    config: PostgresConnectionConfig | None = None,
    control_db: str | None = None,
    control_schema: str | None = None,
    control_table: str | None = None,
) -> dict[str, int] | None:
    if config is None:
        config = PostgresConnectionConfig()

    actual_control_db = control_db or target_db
    actual_control_schema = control_schema or target_schema
    actual_control_table = control_table or "INGESTION_STATUS"

    conn = get_connection(config, actual_control_db, use_cache=True)
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                SELECT DISTINCT ON (partition_id) partition_id, waterlevel
                FROM {}.{}
                WHERE eventhub_namespace = %s
                  AND eventhub = %s
                  AND target_db = %s
                  AND target_schema = %s
                  AND target_table = %s
                  AND partition_id IS NOT NULL
                ORDER BY partition_id, ts_inserted DESC
                """
            ).format(
                sql.Identifier(actual_control_schema),
                sql.Identifier(actual_control_table),
            ),
            (
                eventhub_namespace,
                eventhub,
                target_db,
                target_schema,
                target_table,
            ),
        )
        results = cursor.fetchall()

    if not results:
        logger.info("No partition checkpoints found for %s/%s", eventhub_namespace, eventhub)
        return None

    partition_checkpoints = {row[0]: row[1] for row in results}
    logger.info(
        "Retrieved partition checkpoints: %s for %s/%s",
        partition_checkpoints,
        eventhub_namespace,
        eventhub,
    )
    return partition_checkpoints
