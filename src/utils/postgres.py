"""Postgres utilities for control table storage."""

import contextlib
import json
import logging
import time
import uuid
from typing import Any

import psycopg
from azure.identity import DefaultAzureCredential
from psycopg import sql

from utils.config_models import PostgresConnectionConfig

logger = logging.getLogger(__name__)

_connection_cache: dict[str, psycopg.Connection] = {}
_azure_credential: DefaultAzureCredential | None = None

_AZURE_TOKEN_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"


def _checkpoint_value(waterlevel: Any, metadata: Any) -> dict[str, Any]:
    decoded_metadata = metadata if isinstance(metadata, dict) else {}
    sequence_number = decoded_metadata.get("sequence_number", waterlevel)
    offset = decoded_metadata.get("offset_string", decoded_metadata.get("offset", waterlevel))
    return {"offset": str(offset), "sequence_number": int(sequence_number)}


def _ownership_table_name(control_table: str) -> str:
    return f"{control_table}_ownership"


def _get_cache_key(config: PostgresConnectionConfig, database: str) -> str:
    return (
        f"{config.host}:{config.port}:{config.user}:{database}:{config.sslmode}:{config.auth_mode}"
    )


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
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(target_schema))
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
                SELECT DISTINCT ON (partition_id) partition_id, waterlevel, metadata
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

    partition_checkpoints = {
        row[0]: _checkpoint_value(row[1], row[2] if len(row) > 2 else None)
        for row in results
    }
    logger.info(
        "Retrieved partition checkpoints: %s for %s/%s",
        partition_checkpoints,
        eventhub_namespace,
        eventhub,
    )
    return partition_checkpoints


def _ensure_postgres_ownership_table(cursor: Any, schema_name: str, table_name: str) -> None:
    cursor.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                fully_qualified_namespace TEXT NOT NULL,
                eventhub_name TEXT NOT NULL,
                consumer_group TEXT NOT NULL,
                target_db TEXT NOT NULL,
                target_schema TEXT NOT NULL,
                target_table TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                etag TEXT NOT NULL,
                last_modified_time DOUBLE PRECISION NOT NULL,
                ts_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (
                    fully_qualified_namespace,
                    eventhub_name,
                    consumer_group,
                    target_db,
                    target_schema,
                    target_table,
                    partition_id
                )
            )
            """
        ).format(sql.Identifier(schema_name), sql.Identifier(table_name))
    )


def list_partition_ownership(
    fully_qualified_namespace: str,
    eventhub_name: str,
    consumer_group: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    config: PostgresConnectionConfig | None = None,
    control_db: str | None = None,
    control_schema: str | None = None,
    control_table: str | None = None,
) -> list[dict[str, Any]]:
    if config is None:
        config = PostgresConnectionConfig()

    actual_control_db = control_db or target_db
    actual_control_schema = control_schema or target_schema
    ownership_table = _ownership_table_name(control_table or "INGESTION_STATUS")

    conn = get_connection(config, actual_control_db, use_cache=True)
    with conn.cursor() as cursor:
        _ensure_postgres_ownership_table(cursor, actual_control_schema, ownership_table)
        cursor.execute(
            sql.SQL(
                """
                SELECT partition_id, owner_id, etag, last_modified_time
                FROM {}.{}
                WHERE fully_qualified_namespace = %s
                  AND eventhub_name = %s
                  AND consumer_group = %s
                  AND target_db = %s
                  AND target_schema = %s
                  AND target_table = %s
                """
            ).format(
                sql.Identifier(actual_control_schema),
                sql.Identifier(ownership_table),
            ),
            (
                fully_qualified_namespace,
                eventhub_name,
                consumer_group,
                target_db,
                target_schema,
                target_table,
            ),
        )
        rows = cursor.fetchall()

    return [
        {
            "fully_qualified_namespace": fully_qualified_namespace,
            "eventhub_name": eventhub_name,
            "consumer_group": consumer_group,
            "partition_id": row[0],
            "owner_id": row[1],
            "etag": row[2],
            "last_modified_time": float(row[3]),
        }
        for row in rows
    ]


def claim_partition_ownership(
    ownership_list: list[dict[str, Any]],
    target_db: str,
    target_schema: str,
    target_table: str,
    config: PostgresConnectionConfig | None = None,
    control_db: str | None = None,
    control_schema: str | None = None,
    control_table: str | None = None,
) -> list[dict[str, Any]]:
    if not ownership_list:
        return []

    if config is None:
        config = PostgresConnectionConfig()

    actual_control_db = control_db or target_db
    actual_control_schema = control_schema or target_schema
    ownership_table = _ownership_table_name(control_table or "INGESTION_STATUS")

    conn = get_connection(config, actual_control_db, use_cache=True)
    claimed: list[dict[str, Any]] = []
    with conn.cursor() as cursor:
        _ensure_postgres_ownership_table(cursor, actual_control_schema, ownership_table)
        for ownership in ownership_list:
            now = time.time()
            new_etag = uuid.uuid4().hex
            if ownership.get("etag"):
                cursor.execute(
                    sql.SQL(
                        """
                        UPDATE {}.{}
                        SET owner_id = %s,
                            etag = %s,
                            last_modified_time = %s,
                            ts_modified = CURRENT_TIMESTAMP
                        WHERE fully_qualified_namespace = %s
                          AND eventhub_name = %s
                          AND consumer_group = %s
                          AND target_db = %s
                          AND target_schema = %s
                          AND target_table = %s
                          AND partition_id = %s
                          AND etag = %s
                        RETURNING partition_id
                        """
                    ).format(
                        sql.Identifier(actual_control_schema),
                        sql.Identifier(ownership_table),
                    ),
                    (
                        ownership["owner_id"],
                        new_etag,
                        now,
                        ownership["fully_qualified_namespace"],
                        ownership["eventhub_name"],
                        ownership["consumer_group"],
                        target_db,
                        target_schema,
                        target_table,
                        ownership["partition_id"],
                        ownership["etag"],
                    ),
                )
            else:
                cursor.execute(
                    sql.SQL(
                        """
                        INSERT INTO {}.{} (
                            fully_qualified_namespace, eventhub_name, consumer_group,
                            target_db, target_schema, target_table, partition_id,
                            owner_id, etag, last_modified_time, ts_modified
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT DO NOTHING
                        RETURNING partition_id
                        """
                    ).format(
                        sql.Identifier(actual_control_schema),
                        sql.Identifier(ownership_table),
                    ),
                    (
                        ownership["fully_qualified_namespace"],
                        ownership["eventhub_name"],
                        ownership["consumer_group"],
                        target_db,
                        target_schema,
                        target_table,
                        ownership["partition_id"],
                        ownership["owner_id"],
                        new_etag,
                        now,
                    ),
                )

            if cursor.fetchone():
                claimed.append({**ownership, "etag": new_etag, "last_modified_time": now})

    return claimed
