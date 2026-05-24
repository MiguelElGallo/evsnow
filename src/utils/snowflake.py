"""
Snowflake connection utilities for Snowflake pipeline.

This module provides utilities for:
1. Creating and managing Snowflake connections with private key authentication
2. Managing checkpoint/control tables for EventHub watermark tracking
3. Connection validation and testing
4. Helper functions for Snowflake operations
5. Connection pooling for high-performance streaming

Based on best practices from Snowflake documentation and the legacy implementation.
"""

import json
import logging
import os
import tempfile
import time
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import snowflake.connector as sc
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from utils.config import SnowflakeConnectionConfig

logger = logging.getLogger(__name__)

# Connection cache for reusing connections in high-performance streaming scenarios
# Key: (account, user, database, schema, warehouse, role)
_connection_cache: dict[tuple, sc.SnowflakeConnection] = {}


def _decode_checkpoint_metadata(metadata: Any) -> dict[str, Any]:
    if metadata is None:
        return {}
    if isinstance(metadata, dict):
        return metadata
    if isinstance(metadata, str):
        try:
            return json.loads(metadata)
        except json.JSONDecodeError:
            return {}
    if hasattr(metadata, "as_py"):
        value = metadata.as_py()
        return value if isinstance(value, dict) else {}
    return {}


def _checkpoint_value(waterlevel: Any, metadata: Any) -> dict[str, Any]:
    decoded_metadata = _decode_checkpoint_metadata(metadata)
    sequence_number = decoded_metadata.get("sequence_number", waterlevel)
    offset = decoded_metadata.get("offset_string", decoded_metadata.get("offset", waterlevel))
    return {"offset": str(offset), "sequence_number": int(sequence_number)}


def _ownership_table_name(control_table: str) -> str:
    return f"{control_table}_OWNERSHIP"


def _validate_snowflake_identifiers(identifiers: list[str]) -> None:
    import re

    for identifier in identifiers:
        if not re.match(r"^[A-Za-z0-9_$]+$", identifier):
            raise ValueError(f"Invalid Snowflake identifier: {identifier}")


@contextmanager
def temporary_private_key_file(private_key_pem: str) -> Generator[str]:
    """Write a private key to a temp file and clean it up."""
    fd, path = tempfile.mkstemp(suffix=".pem", prefix="snowflake_key_")
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(private_key_pem)
        yield path
    finally:
        try:
            os.unlink(path)
        except Exception:
            logger.warning("Failed to delete temporary private key file: %s", path)


@contextmanager
def temporary_profile_file(profile: dict[str, Any]) -> Generator[str]:
    """Write a streaming profile JSON to a temp file and clean it up."""
    fd, path = tempfile.mkstemp(suffix=".json", prefix="snowflake_profile_")
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(profile, handle)
        yield path
    finally:
        try:
            os.unlink(path)
        except Exception:
            logger.warning("Failed to delete temporary profile file: %s", path)


def _get_cache_key(config: SnowflakeConnectionConfig) -> tuple:
    """Generate a cache key for a connection configuration."""
    return (
        config.account,
        config.user,
        config.database,
        config.schema_name,
        config.warehouse,
        config.role or "",
    )


def _is_connection_alive(conn: sc.SnowflakeConnection) -> bool:
    """Check if a Snowflake connection is still alive."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return True
    except Exception:
        return False


def close_all_cached_connections() -> None:
    """
    Close all cached Snowflake connections.

    Call this during application shutdown to clean up resources.
    """
    global _connection_cache

    # Close all cached connections
    for key, conn in list(_connection_cache.items()):
        try:
            conn.close()
            logger.debug(f"Closed cached connection for {key[0]}")
        except Exception as e:
            logger.warning(f"Error closing cached connection: {e}")

    _connection_cache.clear()
    logger.info("All cached Snowflake connections closed")


def load_private_key(private_key_file: str, private_key_password: str | None = None) -> bytes:
    """
    Load private key from file for JWT authentication.

    Args:
        private_key_file: Path to the private key file
        private_key_password: Optional password for encrypted keys

    Returns:
        Private key bytes in DER format

    Raises:
        ValueError: If key file is invalid or cannot be read
    """
    try:
        key_path = Path(private_key_file).expanduser().resolve()

        with key_path.open("rb") as key:
            password = private_key_password.encode() if private_key_password else None

            private_key_obj = serialization.load_pem_private_key(
                key.read(), password=password, backend=default_backend()
            )

        # Convert to DER format for Snowflake
        private_key_der: bytes = private_key_obj.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return private_key_der

    except Exception as e:
        logger.error(f"Failed to load private key from {private_key_file}: {e}", exc_info=True)
        raise ValueError(f"Invalid private key file: {e}") from e


def get_connection(
    config: SnowflakeConnectionConfig | None = None,
    use_cache: bool = True,
) -> sc.SnowflakeConnection:
    """
    Create or retrieve a cached Snowflake connection using private key authentication.

    In high-performance streaming scenarios, connections are cached and reused
    to avoid the overhead of repeated authentication and connection setup.

    Args:
        config: Snowflake connection configuration. If not provided,
               will be loaded from environment variables.
        use_cache: If True, reuse cached connections. Set to False to force new connection.

    Returns:
        Active Snowflake connection

    Raises:
        Exception: If connection fails
    """
    if config is None:
        config = SnowflakeConnectionConfig()

    # Check cache first
    if use_cache:
        cache_key = _get_cache_key(config)
        cached_conn = _connection_cache.get(cache_key)

        if cached_conn and _is_connection_alive(cached_conn):
            logger.debug(f"Reusing cached Snowflake connection for account: {config.account}")
            return cached_conn
        elif cached_conn:
            # Connection died, remove from cache
            logger.debug("Cached connection is stale, creating new one")
            _connection_cache.pop(cache_key, None)

    logger.info(f"Connecting to Snowflake account: {config.account}")

    try:
        # Load private key
        private_key = load_private_key(config.private_key_file, config.private_key_password)

        # Build connection parameters
        conn_params = {
            "account": config.account,
            "user": config.user,
            "private_key": private_key,
            "warehouse": config.warehouse,
            "database": config.database,
            "schema": config.schema_name,
        }

        if config.role:
            conn_params["role"] = config.role

        # Create connection
        conn = sc.connect(**conn_params)

        logger.info(f"Successfully connected to Snowflake account: {config.account}")

        # Cache the connection for reuse
        if use_cache:
            cache_key = _get_cache_key(config)
            _connection_cache[cache_key] = conn
            logger.debug("Connection cached for future reuse")

        return conn

    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}", exc_info=True)
        raise


def check_connection(config: SnowflakeConnectionConfig | None = None) -> bool:
    """
    Test the Snowflake connection.

    Args:
        config: Optional Snowflake connection configuration

    Returns:
        True if connection is successful, False otherwise

    Raises:
        Exception: Re-raises connection errors for debugging
    """
    try:
        if config is None:
            config = SnowflakeConnectionConfig()

        logger.info(f"Testing connection to Snowflake account: {config.account}")

        conn = get_connection(config)

        try:
            # Test with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            result = cursor.fetchone()

            if result:
                logger.info(f"Successfully connected to Snowflake. Version: {result[0]}")

                # Verify context
                cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
                context = cursor.fetchone()

                if context:
                    db, schema, warehouse = context
                    logger.info(
                        f"Current context - Database: {db}, Schema: {schema}, Warehouse: {warehouse}"
                    )

                    # Verify expected context
                    if db != config.database.upper():
                        logger.warning(
                            f"Connected to different database: {db} (expected: {config.database})"
                        )
                    if schema != config.schema_name.upper():
                        logger.warning(
                            f"Connected to different schema: {schema} (expected: {config.schema_name})"
                        )
                    if warehouse != config.warehouse.upper():
                        logger.warning(
                            f"Connected to different warehouse: {warehouse} (expected: {config.warehouse})"
                        )

                return True

            logger.error("Connection test query returned no result")
            return False

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        raise


def create_control_table(
    target_db: str,
    target_schema: str,
    target_table: str,
    config: SnowflakeConnectionConfig | None = None,
    use_hybrid_table: bool = True,
) -> bool:
    """
    Create the INGESTION_STATUS control table for checkpoint management.

    By default, creates a Snowflake HYBRID TABLE which provides OLTP capabilities
    with row-level locking and primary key constraints, ideal for frequent checkpoint
    updates from multiple EventHub partitions.

    For trial accounts where Hybrid Tables are not available, set use_hybrid_table=False
    to create a regular table instead.

    This table tracks EventHub consumption progress per partition using a composite primary key
    to ensure uniqueness and efficient upsert operations.

    Args:
        target_db: Target database name
        target_schema: Target schema name
        target_table: Target table name (typically "INGESTION_STATUS")
        config: Optional Snowflake connection configuration
        use_hybrid_table: If True, create a Hybrid Table (default). If False, create a regular table.
                          Set to False for Snowflake trial accounts.

    Returns:
        True if table was created or already exists

    Raises:
        Exception: If table creation fails

    Reference:
        https://docs.snowflake.com/en/user-guide/tables-hybrid
    """
    try:
        if config is None:
            config = SnowflakeConnectionConfig()

        table_type = "HYBRID TABLE" if use_hybrid_table else "TABLE"
        logger.info(
            f"Creating control table ({table_type}): {target_db}.{target_schema}.{target_table}"
        )

        conn = get_connection(config)

        try:
            cursor = conn.cursor()

            # Use fully qualified identifiers - Snowflake validates identifier names
            # No SQL injection risk as identifiers are validated by Snowflake

            # Validate identifier format to prevent injection
            import re

            for identifier in [target_db, target_schema, target_table]:
                if not re.match(r"^[A-Za-z0-9_$]+$", identifier):
                    raise ValueError(f"Invalid Snowflake identifier: {identifier}")

            # Create schema if it doesn't exist
            schema_ddl = f"CREATE SCHEMA IF NOT EXISTS {target_db}.{target_schema}"
            cursor.execute(schema_ddl)

            if use_hybrid_table:
                # Create control table as HYBRID TABLE with improved schema for per-partition checkpoints
                # Hybrid tables provide OLTP capabilities with row-level locking, perfect for frequent checkpoint updates
                # Reference: https://docs.snowflake.com/en/user-guide/tables-hybrid
                table_ddl = f"""
                    CREATE HYBRID TABLE IF NOT EXISTS {target_db}.{target_schema}.{target_table} (
                        TS_INSERTED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
                        EVENTHUB_NAMESPACE VARCHAR(500),
                        EVENTHUB VARCHAR(200),
                        TARGET_DB VARCHAR(200),
                        TARGET_SCHEMA VARCHAR(200),
                        TARGET_TABLE VARCHAR(200),
                        WATERLEVEL NUMBER(38, 0),
                        PARTITION_ID VARCHAR(50) NOT NULL,
                        METADATA VARIANT,
                        PRIMARY KEY (EVENTHUB_NAMESPACE, EVENTHUB, TARGET_DB, TARGET_SCHEMA, TARGET_TABLE, PARTITION_ID)
                    )
                """
            else:
                # Create regular table for trial accounts or environments without Hybrid Table support
                # Note: Regular tables don't have row-level locking, but MERGE operations still work
                table_ddl = f"""
                    CREATE TABLE IF NOT EXISTS {target_db}.{target_schema}.{target_table} (
                        TS_INSERTED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
                        EVENTHUB_NAMESPACE VARCHAR(500),
                        EVENTHUB VARCHAR(200),
                        TARGET_DB VARCHAR(200),
                        TARGET_SCHEMA VARCHAR(200),
                        TARGET_TABLE VARCHAR(200),
                        WATERLEVEL NUMBER(38, 0),
                        PARTITION_ID VARCHAR(50) NOT NULL,
                        METADATA VARIANT
                    )
                """
            cursor.execute(table_ddl)

            logger.info(
                f"Control table verified/created: {target_db}.{target_schema}.{target_table}"
            )
            return True

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Failed to create control table: {e}", exc_info=True)
        logger.error(f"  Table: {target_db}.{target_schema}.{target_table}")
        raise


def insert_partition_checkpoint(
    eventhub_namespace: str,
    eventhub: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    partition_id: str,
    waterlevel: int,
    metadata: dict[str, Any] | None = None,
    config: SnowflakeConnectionConfig | None = None,
    control_db: str | None = None,
    control_schema: str | None = None,
    control_table: str | None = None,
) -> None:
    """
    Insert or update (MERGE) a checkpoint record for a specific partition in the hybrid table.

    Uses MERGE to ensure idempotent checkpoint updates. If a record exists for the same
    partition, it updates the waterlevel and timestamp. Otherwise, it inserts a new record.

    NOTE: This function uses cached connections for high-performance streaming.
    Connections are kept alive and reused across multiple checkpoint saves.

    Args:
        eventhub_namespace: EventHub namespace identifier
        eventhub: EventHub name
        target_db: Target DATA table database (where events are ingested)
        target_schema: Target DATA table schema (where events are ingested)
        target_table: Target DATA table name (where events are ingested)
        partition_id: EventHub partition ID
        waterlevel: Water level (sequence number) for this partition
        metadata: Optional metadata dictionary
        config: Optional Snowflake connection configuration
        control_db: Control table database (default: from config or target_db)
        control_schema: Control table schema (default: from config or target_schema)
        control_table: Control table name (default: INGESTION_STATUS)

    Raises:
        Exception: If merge operation fails
    """
    try:
        # Load config if not provided
        if config is None:
            config = SnowflakeConnectionConfig()

        # Get cached connection (don't close it!)
        conn = get_connection(config, use_cache=True)

        # Determine control table location (where to store the checkpoint)
        # Default to environment config or fall back to target table location
        actual_control_db = control_db or config.database
        actual_control_schema = control_schema or config.schema_name
        actual_control_table = control_table or "INGESTION_STATUS"

        # Build the fully qualified control table name
        control_table_fqn = f"{actual_control_db}.{actual_control_schema}.{actual_control_table}"

        logger.debug(
            f"Inserting checkpoint into control table: {control_table_fqn}, "
            f"for target table: {target_db}.{target_schema}.{target_table}"
        )

        # Validate identifiers
        import re

        for identifier in [
            actual_control_db,
            actual_control_schema,
            actual_control_table,
            target_db,
            target_schema,
            target_table,
        ]:
            if not re.match(r"^[A-Za-z0-9_$]+$", identifier):
                raise ValueError(f"Invalid Snowflake identifier: {identifier}")

        cursor = conn.cursor()

        # Ensure warehouse is active for DML operations
        if config.warehouse:
            cursor.execute(f"USE WAREHOUSE {config.warehouse}")
            logger.debug(f"Activated warehouse: {config.warehouse}")

        # Prepare metadata JSON
        import json

        metadata_json = json.dumps(metadata) if metadata else None

        # Use MERGE for upsert operation (ideal for hybrid tables with primary keys)
        # TARGET_DB, TARGET_SCHEMA, TARGET_TABLE columns identify the DATA table
        merge_sql = f"""
            MERGE INTO {control_table_fqn} AS target
            USING (
                SELECT
                    %s AS EVENTHUB_NAMESPACE,
                    %s AS EVENTHUB,
                    %s AS TARGET_DB,
                    %s AS TARGET_SCHEMA,
                    %s AS TARGET_TABLE,
                    %s AS PARTITION_ID,
                    %s AS WATERLEVEL,
                    PARSE_JSON(%s) AS METADATA,
                    CURRENT_TIMESTAMP() AS TS_INSERTED
            ) AS source
            ON target.EVENTHUB_NAMESPACE = source.EVENTHUB_NAMESPACE
               AND target.EVENTHUB = source.EVENTHUB
               AND target.TARGET_DB = source.TARGET_DB
               AND target.TARGET_SCHEMA = source.TARGET_SCHEMA
               AND target.TARGET_TABLE = source.TARGET_TABLE
               AND target.PARTITION_ID = source.PARTITION_ID
            WHEN MATCHED THEN
                UPDATE SET
                    target.WATERLEVEL = source.WATERLEVEL,
                    target.TS_INSERTED = source.TS_INSERTED,
                    target.METADATA = source.METADATA
            WHEN NOT MATCHED THEN
                INSERT (TS_INSERTED, EVENTHUB_NAMESPACE, EVENTHUB, TARGET_DB, TARGET_SCHEMA, TARGET_TABLE, WATERLEVEL, PARTITION_ID, METADATA)
                VALUES (source.TS_INSERTED, source.EVENTHUB_NAMESPACE, source.EVENTHUB, source.TARGET_DB, source.TARGET_SCHEMA, source.TARGET_TABLE, source.WATERLEVEL, source.PARTITION_ID, source.METADATA)
        """

        cursor.execute(
            merge_sql,
            (
                eventhub_namespace,
                eventhub,
                target_db,
                target_schema,
                target_table,
                partition_id,
                waterlevel,
                metadata_json,
            ),
        )

        cursor.close()

        logger.debug(
            f"Partition checkpoint merged: partition={partition_id}, waterlevel={waterlevel}"
        )

        # NOTE: Connection is NOT closed here - it's cached for reuse

    except Exception as e:
        logger.error(f"Failed to merge partition checkpoint: {e}", exc_info=True)
        logger.error(f"  EventHub: {eventhub_namespace}/{eventhub}")
        logger.error(f"  Target: {target_db}.{target_schema}.{target_table}")
        logger.error(f"  Partition: {partition_id}, Waterlevel: {waterlevel}")
        raise


def get_partition_checkpoints(
    eventhub_namespace: str,
    eventhub: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    config: SnowflakeConnectionConfig | None = None,
    control_db: str | None = None,
    control_schema: str | None = None,
    control_table: str | None = None,
) -> dict[str, int] | None:
    """
    Retrieve the latest checkpoint for each partition.

    Args:
        eventhub_namespace: EventHub namespace identifier
        eventhub: EventHub name
        target_db: Target DATA table database (where events are ingested)
        target_schema: Target DATA table schema (where events are ingested)
        target_table: Target DATA table name (where events are ingested)
        config: Optional Snowflake connection configuration
        control_db: Control table database (default: from config or target_db)
        control_schema: Control table schema (default: from config or target_schema)
        control_table: Control table name (default: INGESTION_STATUS)

    Returns:
        Dictionary mapping partition_id to waterlevel, or None if no checkpoints found

    Raises:
        Exception: If query fails
    """
    try:
        # Load config if not provided
        if config is None:
            config = SnowflakeConnectionConfig()

        # Determine control table location (where checkpoints are stored)
        actual_control_db = control_db or config.database
        actual_control_schema = control_schema or config.schema_name
        actual_control_table = control_table or "INGESTION_STATUS"

        # Validate identifiers
        import re

        for identifier in [
            actual_control_db,
            actual_control_schema,
            actual_control_table,
            target_db,
            target_schema,
            target_table,
        ]:
            if not re.match(r"^[A-Za-z0-9_$]+$", identifier):
                raise ValueError(f"Invalid Snowflake identifier: {identifier}")

        # Construct the fully qualified control table name
        control_table_fqn = f"{actual_control_db}.{actual_control_schema}.{actual_control_table}"

        logger.info(
            f"Querying control table: {control_table_fqn}, "
            f"for target: {target_db}.{target_schema}.{target_table}"
        )

        conn = get_connection(config, use_cache=True)
        cursor = conn.cursor()

        try:
            if config.warehouse:
                cursor.execute(f"USE WAREHOUSE {config.warehouse}")
                logger.debug(f"Activated warehouse: {config.warehouse}")

            sql = f"""
                SELECT PARTITION_ID, WATERLEVEL, METADATA
                FROM {control_table_fqn}
                WHERE EVENTHUB_NAMESPACE = %s
                  AND EVENTHUB = %s
                  AND TARGET_DB = %s
                  AND TARGET_SCHEMA = %s
                  AND TARGET_TABLE = %s
                  AND PARTITION_ID IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY PARTITION_ID
                    ORDER BY TS_INSERTED DESC
                ) = 1
            """
            cursor.execute(
                sql,
                (
                    eventhub_namespace,
                    eventhub,
                    target_db,
                    target_schema,
                    target_table,
                ),
            )

            results = cursor.fetchall()
        finally:
            cursor.close()

        if not results:
            logger.info(f"No partition checkpoints found for {eventhub_namespace}/{eventhub}")
            return None

        partition_checkpoints = {
            row[0]: _checkpoint_value(row[1], row[2] if len(row) > 2 else None)
            for row in results
        }

        logger.info(
            f"Retrieved partition checkpoints: {partition_checkpoints} for {eventhub_namespace}/{eventhub}"
        )
        return partition_checkpoints

    except Exception as e:
        logger.error(f"Failed to retrieve partition checkpoints: {e}", exc_info=True)
        logger.error(f"  EventHub: {eventhub_namespace}/{eventhub}")
        logger.error(f"  Control Table: {target_db}.{target_schema}.{target_table}")
        raise


def _ensure_snowflake_ownership_table(cursor: Any, ownership_table_fqn: str) -> None:
    database_name, schema_name, table_name = ownership_table_fqn.split(".", maxsplit=2)

    cursor.execute(f"SHOW HYBRID TABLES LIKE '{table_name}' IN SCHEMA {database_name}.{schema_name}")
    if cursor.fetchall():
        return

    cursor.execute(f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {database_name}.{schema_name}")
    if cursor.fetchall():
        raise RuntimeError(
            f"Existing Snowflake ownership table {ownership_table_fqn} is not a Hybrid Table. "
            "Migrate or drop it before enabling multi-consumer Event Hub ownership."
        )

    # Snowflake standard-table unique constraints are not enforced; ownership
    # claims need a Hybrid Table primary key for real compare-and-set behavior.
    # See: https://docs.snowflake.com/en/sql-reference/constraints-overview
    cursor.execute(
        f"""
        CREATE HYBRID TABLE IF NOT EXISTS {ownership_table_fqn} (
            FULLY_QUALIFIED_NAMESPACE VARCHAR(500) NOT NULL,
            EVENTHUB_NAME VARCHAR(500) NOT NULL,
            CONSUMER_GROUP VARCHAR(200) NOT NULL,
            TARGET_DB VARCHAR(200) NOT NULL,
            TARGET_SCHEMA VARCHAR(200) NOT NULL,
            TARGET_TABLE VARCHAR(200) NOT NULL,
            PARTITION_ID VARCHAR(50) NOT NULL,
            OWNER_ID VARCHAR(500) NOT NULL,
            ETAG VARCHAR(100) NOT NULL,
            LAST_MODIFIED_TIME FLOAT NOT NULL,
            TS_MODIFIED TIMESTAMP_NTZ NOT NULL,
            PRIMARY KEY (
                FULLY_QUALIFIED_NAMESPACE,
                EVENTHUB_NAME,
                CONSUMER_GROUP,
                TARGET_DB,
                TARGET_SCHEMA,
                TARGET_TABLE,
                PARTITION_ID
            )
        )
        """
    )


def list_partition_ownership(
    fully_qualified_namespace: str,
    eventhub_name: str,
    consumer_group: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    config: SnowflakeConnectionConfig | None = None,
    control_db: str | None = None,
    control_schema: str | None = None,
    control_table: str | None = None,
) -> list[dict[str, Any]]:
    if config is None:
        config = SnowflakeConnectionConfig()

    actual_control_db = control_db or config.database
    actual_control_schema = control_schema or config.schema_name
    actual_control_table = control_table or "INGESTION_STATUS"
    ownership_table = _ownership_table_name(actual_control_table)

    identifiers = [
        actual_control_db,
        actual_control_schema,
        ownership_table,
        target_db,
        target_schema,
        target_table,
    ]
    if config.warehouse:
        identifiers.append(config.warehouse)
    _validate_snowflake_identifiers(identifiers)

    ownership_table_fqn = f"{actual_control_db}.{actual_control_schema}.{ownership_table}"
    conn = get_connection(config, use_cache=True)
    cursor = conn.cursor()
    try:
        if config.warehouse:
            cursor.execute(f"USE WAREHOUSE {config.warehouse}")
        _ensure_snowflake_ownership_table(cursor, ownership_table_fqn)
        cursor.execute(
            f"""
            SELECT PARTITION_ID, OWNER_ID, ETAG, LAST_MODIFIED_TIME
            FROM {ownership_table_fqn}
            WHERE FULLY_QUALIFIED_NAMESPACE = %s
              AND EVENTHUB_NAME = %s
              AND CONSUMER_GROUP = %s
              AND TARGET_DB = %s
              AND TARGET_SCHEMA = %s
              AND TARGET_TABLE = %s
            """,
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
    finally:
        cursor.close()

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
    config: SnowflakeConnectionConfig | None = None,
    control_db: str | None = None,
    control_schema: str | None = None,
    control_table: str | None = None,
) -> list[dict[str, Any]]:
    if not ownership_list:
        return []

    if config is None:
        config = SnowflakeConnectionConfig()

    actual_control_db = control_db or config.database
    actual_control_schema = control_schema or config.schema_name
    actual_control_table = control_table or "INGESTION_STATUS"
    ownership_table = _ownership_table_name(actual_control_table)
    ownership_table_fqn = f"{actual_control_db}.{actual_control_schema}.{ownership_table}"

    identifiers = [
            actual_control_db,
            actual_control_schema,
            ownership_table,
            target_db,
            target_schema,
            target_table,
    ]
    if config.warehouse:
        identifiers.append(config.warehouse)
    _validate_snowflake_identifiers(identifiers)

    conn = get_connection(config, use_cache=True)
    cursor = conn.cursor()
    claimed: list[dict[str, Any]] = []
    try:
        if config.warehouse:
            cursor.execute(f"USE WAREHOUSE {config.warehouse}")
        _ensure_snowflake_ownership_table(cursor, ownership_table_fqn)

        for ownership in ownership_list:
            now = time.time()
            new_etag = uuid.uuid4().hex
            base_values = (
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
            )

            try:
                if ownership.get("etag"):
                    cursor.execute(
                        f"""
                        UPDATE {ownership_table_fqn}
                        SET OWNER_ID = %s,
                            ETAG = %s,
                            LAST_MODIFIED_TIME = %s,
                            TS_MODIFIED = CURRENT_TIMESTAMP()
                        WHERE FULLY_QUALIFIED_NAMESPACE = %s
                          AND EVENTHUB_NAME = %s
                          AND CONSUMER_GROUP = %s
                          AND TARGET_DB = %s
                          AND TARGET_SCHEMA = %s
                          AND TARGET_TABLE = %s
                          AND PARTITION_ID = %s
                          AND ETAG = %s
                        """,
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
                        f"""
                        INSERT INTO {ownership_table_fqn} (
                            FULLY_QUALIFIED_NAMESPACE, EVENTHUB_NAME, CONSUMER_GROUP,
                            TARGET_DB, TARGET_SCHEMA, TARGET_TABLE, PARTITION_ID,
                            OWNER_ID, ETAG, LAST_MODIFIED_TIME, TS_MODIFIED
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                        """,
                        base_values,
                    )
            except Exception as exc:
                logger.info(
                    "Ownership claim skipped for partition %s: %s",
                    ownership.get("partition_id"),
                    exc,
                )
                continue

            if getattr(cursor, "rowcount", 0) == 1:
                claimed.append(
                    {
                        **ownership,
                        "etag": new_etag,
                        "last_modified_time": now,
                    }
                )
    finally:
        cursor.close()

    return claimed


# Example usage
if __name__ == "__main__":
    import logging

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    print("Snowflake utilities module loaded successfully")
    print("Use functions like check_connection(), create_control_table(), etc.")
