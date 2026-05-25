# Snowflake Quickstart

Use this page when the Snowflake objects for the first run do not exist yet.
It creates the runtime role, service user, warehouse, control table, target
table, and Snowpipe Streaming pipe.

## Before You Start

You need an active Snowflake account and a setup role that can create users,
roles, warehouses, databases, schemas, tables, pipes, and grants. `ACCOUNTADMIN`
is acceptable for a one-time bootstrap when your organization does not provide a
narrower setup role.

If `snow connection test` succeeds but object creation fails with
`000666 ... account is suspended due to lack of payment method`, reactivate the
Snowflake account before retrying. A successful login does not prove the account
can run setup DDL.

The commands below use the Snowflake CLI:

```bash
snow connection test --connection <setup-connection>
```

You can also run the same SQL in Snowflake Worksheets. Replace
`<setup-connection>` with the connection name for your setup role.

## Generate The RSA Key

For an interactive local setup, run:

```bash
./generate_snowflake_keys.sh
```

The script creates:

| File | Purpose |
|------|---------|
| `snowflake/rsa_key_encrypted.p8` | Encrypted private key used by EvSnow |
| `snowflake/rsa_key_pub.pem` | Public key file |
| `snowflake/rsa_key_pub_value.txt` | Public key value to paste into Snowflake SQL |

For headless validation or CI, provide the password through
`EVSNOW_KEY_PASSWORD`:

```bash
EVSNOW_KEY_PASSWORD="replace-with-key-password" ./generate_snowflake_keys.sh
```

Do not pipe the password into the script. Keep the encrypted private key and the
password outside Git. The script does not write an unencrypted private key unless
you explicitly set `EVSNOW_WRITE_UNENCRYPTED_KEY=yes`.

## Run The Snowflake Bootstrap

Render `setup_snowflake.sql` with the generated public key, then run it with the
setup connection:

```bash
mkdir -p .quickstart
PUBLIC_KEY_VALUE="$(tr -d '\n' < snowflake/rsa_key_pub_value.txt)"
sed "s|<PUBLIC_KEY_VALUE>|${PUBLIC_KEY_VALUE}|g" \
  setup_snowflake.sql > .quickstart/setup_snowflake.sql

snow sql \
  --connection <setup-connection> \
  --filename .quickstart/setup_snowflake.sql
```

The script creates or verifies:

| Object | Default |
|--------|---------|
| Runtime role | `STREAM` |
| Runtime user | `STREAMEV` |
| Warehouse | `COMPUTE_WH` |
| Ingestion database/schema | `INGESTION.PUBLIC` |
| Control database/schema/table | `CONTROL.PUBLIC.INGESTION_STATUS` |

It also grants the `STREAM` role the privileges needed by the runtime,
including control-table setup and validation.

## Create The Target Table And Pipe

Run the Snowpipe Streaming setup script:

```bash
snow sql \
  --connection <setup-connection> \
  --filename setup_snowpipe_streaming.sql
```

The script creates the Snowflake-managed Iceberg target table and the
high-performance Snowpipe Streaming pipe if they do not already exist:

| Object | Default |
|--------|---------|
| Target table | `INGESTION.PUBLIC.EVENTS_TABLE1` |
| Streaming pipe | `INGESTION.PUBLIC.EVENTS_TABLE_PIPE` |

The table uses Snowflake-managed Iceberg storage:

```sql
CREATE ICEBERG TABLE IF NOT EXISTS INGESTION.PUBLIC.EVENTS_TABLE1
  CATALOG = SNOWFLAKE
  EXTERNAL_VOLUME = SNOWFLAKE_MANAGED
  ICEBERG_VERSION = 3;
```

Use [Complete Snowflake setup](../snowflake/complete-setup.md) for the long-form
object reference.

## Verify Snowflake Objects

Run these checks with the setup connection:

```bash
snow sql --connection <setup-connection> --query "
DESC USER STREAMEV;
SHOW GRANTS TO ROLE STREAM;
DESC TABLE CONTROL.PUBLIC.INGESTION_STATUS;
DESC TABLE INGESTION.PUBLIC.EVENTS_TABLE1;
SHOW PIPES LIKE 'EVENTS_TABLE_PIPE' IN SCHEMA INGESTION.PUBLIC;
SHOW GRANTS ON PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE;
"
```

The pipe check must return `EVENTS_TABLE_PIPE`, and the grant checks must show
the runtime role has table access plus `OPERATE` and `MONITOR` on the pipe.
`DESC USER STREAMEV` should show `TYPE = SERVICE`.

Keep this checklist as the known-good proof before moving on:

| Check | Expected result |
|-------|-----------------|
| `DESC USER STREAMEV` | `TYPE` is `SERVICE` |
| `SHOW GRANTS TO ROLE STREAM` | `CREATE SCHEMA` on `CONTROL`, `CREATE TABLE` on `CONTROL.PUBLIC`, and DML on `CONTROL.PUBLIC.INGESTION_STATUS` |
| `SHOW PIPES LIKE 'EVENTS_TABLE_PIPE'` | One pipe named `EVENTS_TABLE_PIPE` in `INGESTION.PUBLIC` |
| `SHOW GRANTS ON PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE` | `STREAM` has `OPERATE` and `MONITOR` |
| `DESC TABLE INGESTION.PUBLIC.EVENTS_TABLE1` | The target table exists before EvSnow starts |

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
SNOWFLAKE_ACCOUNT=<account_locator>
SNOWFLAKE_USER=STREAMEV
SNOWFLAKE_PRIVATE_KEY_FILE=snowflake/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=<key-password>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=STREAM
SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE
```

For this one-target quickstart, EvSnow derives the Snowflake session
database/schema from `config/evsnow.toml`. Set `SNOWFLAKE_DATABASE` and
`SNOWFLAKE_SCHEMA_NAME` only when you map to multiple database/schema pairs or
need an explicit session context.

The full environment template remains in
[`.env.example`](https://github.com/MiguelElGallo/evsnow/blob/main/.env.example).

## Verify EvSnow Configuration

```bash
uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env
```

This validates the resolved EvSnow configuration and control-table access. Keep
the Snowflake object checks above as the proof that the Iceberg table, pipe, and
pipe grants exist.

Treat any warning as a setup failure even if the command exits `0`. In
particular, `Warning: Could not verify Snowflake control table` means the
runtime role still lacks the control-table privileges needed by validation.
The expected success marker is
`Snowflake control table verified/created successfully` with no warnings.

After validation passes without warnings, continue with
[First run](../tutorial/first-run.md).
