# Complete Snowflake Setup

This reference collects the Snowflake objects EvSnow needs. New users should
start with [First run](../tutorial/first-run.md) to understand the runtime flow.
If the Snowflake objects do not already exist, complete
[Snowflake quickstart](../getting-started/snowflake-quickstart.md) before
running the pipeline.

## Object Map

| Object | Default name | Purpose |
|--------|--------------|---------|
| Runtime role | `STREAM` | Least-privilege role used by EvSnow |
| Runtime user | `STREAMEV` | Key-pair authenticated service user |
| Warehouse | `COMPUTE_WH` | Runtime warehouse used for validation and setup |
| Ingestion database | `INGESTION` | Target database for streamed events |
| Ingestion schema | `PUBLIC` | Target schema for the first-run example |
| Control database | `CONTROL` | Checkpoint database |
| Control table | `CONTROL.PUBLIC.INGESTION_STATUS` | Partition checkpoint state |
| Target table | `INGESTION.PUBLIC.EVENTS_TABLE1` | Snowflake-managed Iceberg table |
| Pipe | `INGESTION.PUBLIC.EVENTS_TABLE_PIPE` | Required high-performance Snowpipe Streaming pipe |

## Tested Setup Scripts

Use the checked-in scripts as the source of truth:

- [`setup_snowflake.sql`](https://github.com/MiguelElGallo/evsnow/blob/main/setup_snowflake.sql) creates the role, user key assignment, warehouse, databases, schemas, control table, and grants.
- [`setup_snowpipe_streaming.sql`](https://github.com/MiguelElGallo/evsnow/blob/main/setup_snowpipe_streaming.sql) creates the Snowflake-managed Iceberg table and pipe with `DATA_SOURCE(TYPE => 'STREAMING')` if they do not already exist.

Run the scripts in Snowflake Worksheets or with the Snowflake CLI using a setup
role that can create users, roles, databases, schemas, tables, pipes, and
grants. Use `ACCOUNTADMIN` only for one-time bootstrap when your organization
does not provide a narrower setup role. The checked-in
`setup_snowflake.sql` starts with `USE ROLE ACCOUNTADMIN`; replace that line in
your rendered copy when using a delegated setup role.

If Snowflake returns `000666 ... account is suspended due to lack of payment
method`, the account can authenticate but cannot run setup DDL. Reactivate the
account and rerun the setup script.

The default setup keeps Iceberg storage Snowflake-managed:

```sql
CREATE ICEBERG TABLE IF NOT EXISTS INGESTION.PUBLIC.EVENTS_TABLE1
  CATALOG = SNOWFLAKE
  EXTERNAL_VOLUME = SNOWFLAKE_MANAGED
  ICEBERG_VERSION = 3;
```

The runtime role needs control-table, target-table, and pipe creation grants
before the setup scripts create the Snowflake objects:

```sql
GRANT CREATE SCHEMA ON DATABASE CONTROL TO ROLE STREAM;
GRANT CREATE TABLE ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM;
GRANT CREATE ICEBERG TABLE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT CREATE PIPE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
```

The control DDL grants are required because `evsnow validate-config` verifies
the control table by running idempotent `CREATE SCHEMA IF NOT EXISTS` and
`CREATE TABLE IF NOT EXISTS` statements as the runtime role.

## Runtime Configuration

Keep pipeline shape in `config/evsnow.toml`:

```toml
[control]
target_db = "CONTROL"
target_schema = "PUBLIC"
target_table = "INGESTION_STATUS"
backend = "snowflake"
ownership_mode = "local_single_consumer_smoke"
use_hybrid_table = false

[snowflake_configs.SNOWFLAKE_1]
database = "INGESTION"
schema_name = "PUBLIC"
table_name = "EVENTS_TABLE1"
batch_size = 100
```

Keep secrets and local credentials in `.env`:

```bash
SNOWFLAKE_ACCOUNT=<account_locator>.<region>
SNOWFLAKE_USER=STREAMEV
SNOWFLAKE_PRIVATE_KEY_FILE=snowflake/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=<key-password>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=STREAM
SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE
```

For one mapped target, EvSnow derives the session database and schema from the
target in `config/evsnow.toml`. Add `SNOWFLAKE_DATABASE` and
`SNOWFLAKE_SCHEMA_NAME` only for multi-target mappings or when you need to force
an explicit Snowflake session context.

See [Configuration](../configuration.md) for precedence and every supported
TOML/environment setting.

## Verify

```bash
uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env
```

The validation step checks the resolved configuration and can show RBAC
requirements before the pipeline starts. Treat warnings as setup failures for
the quickstart. `Warning: Could not verify Snowflake control table` means the
runtime role cannot prove or create the control schema/table.

## Query Iceberg Data

Use [Query Iceberg with DuckDB](../how-to/query-iceberg-with-duckdb.md) for the
optional DuckDB flow. That page keeps query-token guidance separate from the
runtime setup and requires a least-privilege query role.

## Troubleshooting

### RSA Key Authentication Fails

```sql
DESC USER STREAMEV;
```

Confirm `RSA_PUBLIC_KEY_FP` is populated, regenerate keys with
`./generate_snowflake_keys.sh` if needed, and verify `.env` points to the local
encrypted private key.

### Pipe Not Found

```sql
SHOW PIPES LIKE 'EVENTS_TABLE_PIPE' IN SCHEMA INGESTION.PUBLIC;
SHOW GRANTS ON PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE;
```

Confirm `SNOWFLAKE_PIPE_NAME` matches the created pipe and that the runtime role
has `OPERATE` and `MONITOR` grants.

### Checkpoint Writes Fail

```sql
DESC TABLE CONTROL.PUBLIC.INGESTION_STATUS;
SHOW GRANTS ON TABLE CONTROL.PUBLIC.INGESTION_STATUS;
```

The table schema must match `setup_snowflake.sql`, and the runtime role needs
`CREATE SCHEMA` on the control database, `CREATE TABLE` on the control schema,
and `SELECT`, `INSERT`, and `UPDATE` privileges on the control table.

### Account Suspended

```text
000666 (57014): Your account is suspended due to lack of payment method.
```

Fix the Snowflake account billing/reactivation state first. Connection tests can
still pass while create/drop/alter statements are blocked.
