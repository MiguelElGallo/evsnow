# Configuration

EvSnow can read configuration from two places.

Use TOML for the pipeline shape.

Use `.env` for secrets and machine-specific values.

This keeps the file you review small, typed, and easy to validate.

## Start with TOML

Create a structured config file:

```bash
cp config/evsnow.example.toml config/evsnow.toml
```

Then edit `config/evsnow.toml`.

For one Event Hub and one Snowflake table, the important parts look like this:

```toml
eventhub_namespace = "eventhub1.servicebus.windows.net"
environment = "development"
region = "local"

[event_hubs.EVENTHUBNAME_1]
name = "topic1"
namespace = "eventhub1.servicebus.windows.net"
consumer_group = "$Default"

[snowflake_configs.SNOWFLAKE_1]
database = "INGESTION"
schema_name = "PUBLIC"
table_name = "EVENTS_TABLE1"
batch_size = 100

[[mappings]]
event_hub_key = "EVENTHUBNAME_1"
snowflake_key = "SNOWFLAKE_1"
```

The names `EVENTHUBNAME_1` and `SNOWFLAKE_1` are still the runtime keys.

That keeps TOML compatible with the existing pipeline mapping model.

## Keep secrets in `.env`

Do not put passwords, tokens, or shared access keys in TOML.

Put them in `.env` or in the real process environment:

```bash
SNOWFLAKE_ACCOUNT=aaaaaa-bbbbbbb
SNOWFLAKE_USER=STREAMEV
SNOWFLAKE_PRIVATE_KEY_FILE=/path/to/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=your-key-password
SNOWFLAKE_WAREHOUSE=compute_wh
SNOWFLAKE_ROLE=STREAM
SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE
SMART_RETRY_LLM_API_KEY=your-api-key
LOGFIRE_TOKEN=your-logfire-token
CONTROL_PG_PASSWORD=checkpoint-password
```

This is useful because `config/evsnow.toml` can be reviewed and copied without exposing secrets.

For one mapped Snowflake target, EvSnow derives the Snowflake connection
session database/schema from `snowflake_configs` in TOML. If you map to
multiple database/schema pairs, set `SNOWFLAKE_DATABASE` and
`SNOWFLAKE_SCHEMA_NAME` explicitly as the session context.

## Validate first

Run validation before starting the pipeline:

```bash
uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env
```

EvSnow loads the TOML file, then applies values from `.env`, then applies real environment variables.

So the order is:

```text
model defaults < TOML < .env / --env-file < process environment < CLI runtime flags
```

This lets TOML describe the default pipeline, while local shells and deployment systems can still override values.

## Run with TOML

Run a dry run:

```bash
uv run evsnow run --config-file config/evsnow.toml --env-file .env --dry-run
```

Start the pipeline:

```bash
uv run evsnow run --config-file config/evsnow.toml --env-file .env
```

If `config/evsnow.toml` exists, EvSnow will use it by default.

Passing `--config-file` makes the source explicit.

## Use schema validation

The generated JSON Schema lives at `schemas/evsnow.schema.json`.

Many editors can use that schema to autocomplete fields and flag invalid values.

EvSnow uses the same Pydantic models at runtime, so editor feedback and CLI validation come from the same structure.

Use [Parameter reference](reference/parameters.md) when you need the full list
of supported TOML keys, `.env` variables, defaults, and allowed values.

## Multiple mappings

TOML makes repeated mappings easier to read than a long `.env`.

```toml
[event_hubs.EVENTHUBNAME_1]
name = "orders"
namespace = "eventhub1.servicebus.windows.net"
consumer_group = "$Default"

[event_hubs.EVENTHUBNAME_2]
name = "payments"
namespace = "eventhub1.servicebus.windows.net"
consumer_group = "$Default"

[snowflake_configs.SNOWFLAKE_1]
database = "INGESTION"
schema_name = "PUBLIC"
table_name = "ORDERS"

[snowflake_configs.SNOWFLAKE_2]
database = "INGESTION"
schema_name = "PUBLIC"
table_name = "PAYMENTS"

[[mappings]]
event_hub_key = "EVENTHUBNAME_1"
snowflake_key = "SNOWFLAKE_1"

[[mappings]]
event_hub_key = "EVENTHUBNAME_2"
snowflake_key = "SNOWFLAKE_2"
```

Each mapping connects one Event Hub key to one Snowflake target key.

## Control tables

For a local Snowflake trial smoke test:

```toml
[control]
target_db = "CONTROL"
target_schema = "PUBLIC"
target_table = "INGESTION_STATUS"
backend = "snowflake"
ownership_mode = "local_single_consumer_smoke"
use_hybrid_table = false
```

This persists checkpoints in a standard Snowflake table.

It keeps Event Hub partition ownership in memory, so use it for one local consumer only.

For production durable ownership, use Snowflake Hybrid Tables or Postgres.

For Postgres:

```toml
[control]
target_db = "control"
target_schema = "public"
target_table = "ingestion_status"
backend = "postgres"

[control.postgres]
host = "localhost"
port = 5432
user = "checkpoint_user"
sslmode = "require"
auth_mode = "password"
```

Then put the password in `.env`:

```bash
CONTROL_PG_PASSWORD=checkpoint-password
```

## Common validation errors

If the namespace is missing the Event Hubs suffix:

```toml
eventhub_namespace = "eventhub1"
```

Validation fails before connecting to Azure:

```text
Event Hub namespace must end with .servicebus.windows.net
```

If a mapping points to a key that does not exist:

```toml
[[mappings]]
event_hub_key = "EVENTHUBNAME_2"
snowflake_key = "SNOWFLAKE_1"
```

EvSnow reports the missing mapping target.

That is the main benefit of moving pipeline shape out of `.env`: the structure is visible, typed, and validated before the run begins.
