# Plan: Optional Postgres Checkpoint Table

## Goals
- Allow the checkpoint control table (INGESTION_STATUS) to live in Snowflake or Postgres.
- Keep data ingestion targets in Snowflake unchanged.
- Default behavior remains Snowflake for backwards compatibility.

## Proposed Configuration
- `CONTROL_TABLE_BACKEND={snowflake|postgres}` (default: `snowflake`).
- Keep `TARGET_DB`, `TARGET_SCHEMA`, `TARGET_TABLE` as the control table location for the chosen backend.
- Add Postgres connection options, e.g.:
  - `CONTROL_PG_HOST`
  - `CONTROL_PG_PORT`
  - `CONTROL_PG_USER`
  - `CONTROL_PG_PASSWORD`
  - `CONTROL_PG_SSLMODE`
- `CONTROL_PG_AUTH_MODE={password|azure_token}` (explicit mode selection).
- Use env vars only for Postgres connectivity (no DSN support).
- Azure Postgres (passwordless) option:
  - Support token-based auth via `DefaultAzureCredential` and `psycopg`.
  - Allow env inputs for the URI builder using the same `CONTROL_PG_*` variables.
  - Reference: https://learn.microsoft.com/en-us/azure/postgresql/connectivity/connect-python?tabs=bash%2Cpasswordless
- Connection pooling guidance (Azure official recommendation):
  - Prefer PgBouncer for pooling (built-in on Azure Database for PostgreSQL flexible server, or external).
  - No in-app pooling planned; document how to point `CONTROL_PG_HOST`/`CONTROL_PG_PORT` to PgBouncer.
  - Reference: https://learn.microsoft.com/en-us/azure/postgresql/connectivity/concepts-connection-pooling-best-practices

## Work Breakdown
1. Config modeling
   - Add `PostgresConnectionConfig` in `src/utils/config_models/`.
   - Add `control_table_backend` and `control_postgres` fields to `EvSnowConfig`.
   - Validate required Postgres settings when `control_table_backend=postgres`.
   - Normalize `TARGET_SCHEMA` for Postgres (e.g., force lower-case unless quoted).

2. Postgres control table utilities
   - Add `src/utils/postgres.py` with:
     - connection helper(s)
     - token-based auth helper for Azure Postgres (DefaultAzureCredential -> token as password)
     - `create_control_table(...)`
     - `insert_partition_checkpoint(...)` using `INSERT ... ON CONFLICT`
     - `get_partition_checkpoints(...)` with latest-per-partition query
   - Mirror the Snowflake schema and primary key:
     `(eventhub_namespace, eventhub, target_db, target_schema, target_table, partition_id)`.
   - Use `psycopg` (per Azure recommendation) for Postgres connectivity.

3. Checkpoint manager abstraction
   - Introduce a small interface (protocol/base class) for checkpoint managers.
   - Generalize the Azure `CheckpointStore` wrapper to accept any manager.
   - Implement `PostgresCheckpointManager` alongside existing `SnowflakeCheckpointManager`.

4. EventHub consumer integration
   - Update `EventHubAsyncConsumer` to accept `control_table_backend` and optional
     Postgres config.
   - Instantiate the correct manager/store based on the backend.
   - Update logs to reflect the chosen backend.

5. CLI validation and setup
   - Update `main.validate_config` to create/verify the control table in the selected backend.
   - Add a Postgres setup helper (SQL or CLI) similar to Snowflake setup docs.

6. Documentation and examples
   - Update `README.md` and `SNOWFLAKE_QUICKSTART.md` with the new option.
   - Add a short Postgres setup snippet and note on checkpoint migration when switching backends.

7. Tests
   - Unit tests for Postgres utilities and checkpoint manager.
   - Update EventHub/main tests to cover `control_table_backend=postgres`.
   - Config validation tests for missing/invalid Postgres fields.
   - Validate with `ruff` and `ty` after code changes; run full test suite; maintain coverage.

## Open Questions
- No open questions (auth mode is explicit via `CONTROL_PG_AUTH_MODE`).
