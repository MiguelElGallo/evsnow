"""
Snowflake connection utilities for StreamDuck pipeline.

This module provides utilities for:
1. Creating and managing Snowflake connections with private key authentication
2. Managing checkpoint/control tables for EventHub watermark tracking
3. Connection validation and testing
4. Helper functions for Snowflake operations
5. Connection pooling for high-performance streaming

Based on best practices from Snowflake documentation and the legacy implementation.
"""

import logging
from pathlib import Path
from typing import Any

import snowflake.connector as sc
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Try to import Snowpark, but make it optional
try:
    from snowflake.snowpark import Session

    SNOWPARK_AVAILABLE = True
except ImportError:
    SNOWPARK_AVAILABLE = False
    Session = None  # type: ignore

from utils.config import SnowflakeConnectionConfig

logger = logging.getLogger(__name__)

# Connection cache for reusing connections in high-performance streaming scenarios
# Key: (account, user, database, schema, warehouse, role)
_connection_cache: dict[tuple, sc.SnowflakeConnection] = {}
_session_cache: dict[tuple, Any] = {}


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
    global _connection_cache, _session_cache

    # Close all cached connections
    for key, conn in list(_connection_cache.items()):
        try:
            conn.close()
            logger.debug(f"Closed cached connection for {key[0]}")
        except Exception as e:
            logger.warning(f"Error closing cached connection: {e}")

    # Close all cached sessions
    for key, session in list(_session_cache.items()):
        try:
            session.close()
            logger.debug(f"Closed cached session for {key[0]}")
        except Exception as e:
            logger.warning(f"Error closing cached session: {e}")

    _connection_cache.clear()
    _session_cache.clear()
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
        private_key_der = private_key_obj.private_bytes(
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
        config = SnowflakeConnectionConfig()  # type: ignore[call-arg]

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


def get_snowpark_session(
    config: SnowflakeConnectionConfig | None = None,
):  # type: ignore
    """
    Create a Snowpark Session using private key authentication.

    Args:
        config: Snowflake connection configuration. If not provided,
               will be loaded from environment variables.

    Returns:
        Active Snowpark Session

    Raises:
        ImportError: If snowflake-snowpark is not installed
        Exception: If session creation fails
    """
    if not SNOWPARK_AVAILABLE:
        raise ImportError(
            "snowflake-snowpark is not installed. Install it with: uv add snowflake-snowpark"
        )

    if config is None:
        config = SnowflakeConnectionConfig()  # type: ignore[call-arg]

    logger.info(f"Creating Snowpark session for account: {config.account}")

    try:
        # Load private key
        private_key = load_private_key(config.private_key_file, config.private_key_password)

        # Build connection parameters
        connection_parameters = {
            "account": config.account,
            "user": config.user,
            "private_key": private_key,
            "warehouse": config.warehouse,
            "database": config.database,
            "schema": config.schema_name,
        }

        if config.role:
            connection_parameters["role"] = config.role

        # Create Snowpark session
        session = Session.builder.configs(connection_parameters).create()  # type: ignore

        # Explicitly use the warehouse to ensure it's active
        if config.warehouse:
            session.sql(f"USE WAREHOUSE {config.warehouse}").collect()
            logger.info(f"Activated warehouse: {config.warehouse}")

        logger.info("Snowpark session created successfully")
        return session

    except Exception as e:
        logger.error(f"Failed to create Snowpark session: {e}", exc_info=True)
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
            config = SnowflakeConnectionConfig()  # type: ignore[call-arg]

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
) -> bool:
    """
    Create the INGESTION_STATUS control table as a Snowflake HYBRID TABLE for checkpoint management.

    Hybrid tables provide OLTP capabilities with row-level locking and primary key constraints,
    making them ideal for frequent checkpoint updates from multiple EventHub partitions.

    This table tracks EventHub consumption progress per partition using a composite primary key
    to ensure uniqueness and efficient upsert operations.

    Args:
        target_db: Target database name
        target_schema: Target schema name
        target_table: Target table name (typically "INGESTION_STATUS")
        config: Optional Snowflake connection configuration

    Returns:
        True if table was created or already exists

    Raises:
        Exception: If table creation fails

    Reference:
        https://docs.snowflake.com/en/user-guide/tables-hybrid
    """
    try:
        if config is None:
            config = SnowflakeConnectionConfig()  # type: ignore[call-arg]

        logger.info(f"Creating control table: {target_db}.{target_schema}.{target_table}")

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
            config = SnowflakeConnectionConfig()  # type: ignore[call-arg]

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
            config = SnowflakeConnectionConfig()  # type: ignore[call-arg]

        session = get_snowpark_session(config)

        try:
            # Determine control table location (where checkpoints are stored)
            actual_control_db = control_db or config.database
            actual_control_schema = control_schema or config.schema_name
            actual_control_table = control_table or "INGESTION_STATUS"

            # Construct the fully qualified control table name
            control_table_fqn = (
                f"{actual_control_db}.{actual_control_schema}.{actual_control_table}"
            )

            logger.info(
                f"Querying control table: {control_table_fqn}, "
                f"for target: {target_db}.{target_schema}.{target_table}"
            )

            # Get the table
            df = session.table(control_table_fqn)

            # Filter by EventHub AND target table (to get checkpoints for this specific data table)
            df = df.filter(
                (df["EVENTHUB_NAMESPACE"] == eventhub_namespace)
                & (df["EVENTHUB"] == eventhub)
                & (df["TARGET_DB"] == target_db)
                & (df["TARGET_SCHEMA"] == target_schema)
                & (df["TARGET_TABLE"] == target_table)
                & (df["PARTITION_ID"].is_not_null())
            )

            # Get the latest record for each partition using window function
            from snowflake.snowpark import Window
            from snowflake.snowpark.functions import desc, row_number

            window_spec = Window.partition_by("PARTITION_ID").order_by(desc("TS_INSERTED"))
            df = df.with_column("row_num", row_number().over(window_spec))
            df = df.filter(df["row_num"] == 1)

            # Select only partition_id and waterlevel
            df = df.select("PARTITION_ID", "WATERLEVEL")

            # Collect results
            results = df.collect()

            if not results:
                logger.info(f"No partition checkpoints found for {eventhub_namespace}/{eventhub}")
                return None

            # Convert to dictionary
            partition_checkpoints = {row["PARTITION_ID"]: row["WATERLEVEL"] for row in results}

            logger.info(
                f"Retrieved partition checkpoints: {partition_checkpoints} for {eventhub_namespace}/{eventhub}"
            )
            return partition_checkpoints

        finally:
            session.close()

    except Exception as e:
        logger.error(f"Failed to retrieve partition checkpoints: {e}", exc_info=True)
        logger.error(f"  EventHub: {eventhub_namespace}/{eventhub}")
        logger.error(f"  Control Table: {target_db}.{target_schema}.{target_table}")
        raise


# Example usage
if __name__ == "__main__":
    import logging

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    print("Snowflake utilities module loaded successfully")
    print("Use functions like check_connection(), create_control_table(), etc.")
