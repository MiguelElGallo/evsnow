# Snowflake Quickstart

Use this page when the Snowflake objects for the first run do not exist yet.
It creates the runtime role, service user, control table, target table, and
Snowpipe Streaming pipe.

## Generate The RSA Key

```bash
./generate_snowflake_keys.sh
```

The script creates an encrypted private key and prints the public key value.
Keep the private key outside Git.

## Create The Runtime User

Run this in Snowflake with a setup role that can create roles, users,
databases, schemas, tables, and pipes:

```sql
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS STREAM;
CREATE USER IF NOT EXISTS STREAMEV
  DEFAULT_ROLE = STREAM
  DEFAULT_WAREHOUSE = COMPUTE_WH;

GRANT ROLE STREAM TO USER STREAMEV;
ALTER USER STREAMEV SET RSA_PUBLIC_KEY = '<PUBLIC_KEY_FROM_SCRIPT>';
```

Use `ACCOUNTADMIN` only for this one-time bootstrap if your organization does
not provide a narrower setup role. Runtime ingestion should use the `STREAM`
role or another least-privilege role.

## Create Databases And Schemas

```sql
CREATE DATABASE IF NOT EXISTS INGESTION;
CREATE SCHEMA IF NOT EXISTS INGESTION.PUBLIC;

CREATE DATABASE IF NOT EXISTS CONTROL;
CREATE SCHEMA IF NOT EXISTS CONTROL.PUBLIC;

GRANT USAGE ON DATABASE INGESTION TO ROLE STREAM;
GRANT USAGE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT USAGE ON DATABASE CONTROL TO ROLE STREAM;
GRANT USAGE ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM;
```

These objects match the defaults used in the first-run tutorial.

## Create The Control Table And Grants

```bash
# Open setup_snowflake.sql in Snowflake Worksheets or SnowSQL.
# Replace <PUBLIC_KEY_VALUE> with the value from generate_snowflake_keys.sh.
```

Run the checked-in
[`setup_snowflake.sql`](https://github.com/MiguelElGallo/evsnow/blob/main/setup_snowflake.sql).
It creates the exact `CONTROL.PUBLIC.INGESTION_STATUS` schema used by EvSnow:
`EVENTHUB_NAMESPACE`, `EVENTHUB`, target database/schema/table, `WATERLEVEL`,
`PARTITION_ID`, and `METADATA`.

## Create The Target Table And Pipe

```bash
# Open setup_snowpipe_streaming.sql in Snowflake Worksheets or SnowSQL.
```

Run the checked-in
[`setup_snowpipe_streaming.sql`](https://github.com/MiguelElGallo/evsnow/blob/main/setup_snowpipe_streaming.sql).
It creates the Snowflake-managed Iceberg table and the required high-performance
Snowpipe Streaming pipe with `DATA_SOURCE(TYPE => 'STREAMING')`.

The table definition uses Snowflake-managed Iceberg storage by default:

```sql
CREATE OR REPLACE ICEBERG TABLE INGESTION.PUBLIC.EVENTS_TABLE1
  CATALOG = SNOWFLAKE
  EXTERNAL_VOLUME = SNOWFLAKE_MANAGED
  ICEBERG_VERSION = 3;
```

Use [Complete Snowflake setup](../snowflake/complete-setup.md) if you need the
long-form object reference or troubleshooting notes.

## Configure EvSnow

```bash
cp config/evsnow.example.toml config/evsnow.toml
```

Set the Snowflake target in `config/evsnow.toml`:

```toml
[snowflake_configs.SNOWFLAKE_1]
database = "INGESTION"
schema_name = "PUBLIC"
table_name = "EVENTS_TABLE1"
batch_size = 100
```

Create `.env` with only secrets and local credentials:

```bash
SNOWFLAKE_ACCOUNT=<account_locator>.<region>
SNOWFLAKE_USER=STREAMEV
SNOWFLAKE_PRIVATE_KEY_FILE=snowflake/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=<key-password>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=STREAM
SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE
```

The full environment template remains in
[`.env.example`](https://github.com/MiguelElGallo/evsnow/blob/main/.env.example).

## Verify

```bash
uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env
```

After validation passes, continue with [First run](../tutorial/first-run.md).
