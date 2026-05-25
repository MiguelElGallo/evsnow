# evsnow

EvSnow streams events from Azure Event Hubs into Snowflake with checkpointing
and observability.

This README walks through the smallest local setup first: one Event Hub, one
Snowflake target, one checkpoint table, validation, and a pipeline run.

EvSnow can write to regular Snowflake tables or Snowflake-managed Apache
Iceberg tables through Snowpipe Streaming.

[![Tests](https://github.com/MiguelElGallo/evsnow/actions/workflows/tests.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/tests.yml)
[![CI/CD Pipeline](https://github.com/MiguelElGallo/evsnow/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/MiguelElGallo/evsnow/branch/main/graph/badge.svg)](https://codecov.io/gh/MiguelElGallo/evsnow)

[![Watch the EvSnow walkthrough](media/videoevsnow.png)](https://www.youtube.com/watch?v=zX3K-rfNZIU)

Video: Click the image above for a walkthrough of this repo.

![EvSnow Event Hubs to Snowflake overview](<media/ChatGPT Image Nov 9, 2025, 01_36_42 PM.png>)

## Prerequisites

For the first local run, you need:

- Python 3.13+ and `uv`
- Azure CLI (`az`) for the local `azure_cli` auth example
- One Azure Event Hub you can read from and send to
- One Snowflake table and pipe you can write to
- A Snowflake RSA private key for key-pair authentication
- OpenSSL if you need to generate the Snowflake RSA key locally

For reset/testing workflows, `snowsql` or the Snowflake UI is useful.

If you still need to create the Snowflake table, Snowpipe Streaming pipe, or
role grants, follow the [Snowflake setup guide](./SNOWFLAKE_COMPLETE_SETUP.md)
before validation.

## Install

```bash
# Clone the repository
git clone https://github.com/MiguelElGallo/evsnow.git
cd evsnow

# Install dependencies
uv sync
```

Then create the TOML and `.env` files in the next section before running
validation or the pipeline.

## Configure

Create two local files:

- `config/evsnow.toml` describes the pipeline: Event Hub input, Snowflake
  target, mappings, and checkpoint table.
- `.env` stores secrets and local credentials.

Environment variables override TOML. Keep pipeline shape in TOML unless you
intentionally need a local override.

See [Configuration](./docs/configuration.md) when you need the full reference.

### 1. Copy the structured config file

```bash
cp config/evsnow.example.toml config/evsnow.toml
```

The local `config/evsnow.toml` should stay out of Git. It contains deployment
shape that is often environment-specific, even when it has no passwords.

Now edit `config/evsnow.toml`. For one Event Hub to one Snowflake table, the
core config looks like this:

```toml
eventhub_namespace = "eventhub1.servicebus.windows.net"
environment = "development"
region = "local"

[control]
target_db = "CONTROL"
target_schema = "PUBLIC"
target_table = "INGESTION_STATUS"
backend = "snowflake"
ownership_mode = "local_single_consumer_smoke"
use_hybrid_table = false

[eventhub_defaults]
credential_mode = "azure_cli"
starting_position_on_no_checkpoint = "-1"

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

`EVENTHUBNAME_1` and `SNOWFLAKE_1` are local mapping keys. The real Event Hub
name is `name = "topic1"`.

For your first run, change only these values:

- `eventhub_namespace`
- `[event_hubs.EVENTHUBNAME_1].namespace`
- `[event_hubs.EVENTHUBNAME_1].name`
- `[snowflake_configs.SNOWFLAKE_1].database`
- `[snowflake_configs.SNOWFLAKE_1].schema_name`
- `[snowflake_configs.SNOWFLAKE_1].table_name`

Leave `environment = "development"` and `region = "local"` for a local smoke
test.

For the first setup, use the same namespace value in both namespace fields.

### 2. Create the environment file

Create `.env` with secrets and local-only runtime values:

If you do not already have a Snowflake RSA key, create it first:

```bash
./generate_snowflake_keys.sh
```

The script prints the public key value and the `ALTER USER` SQL. Run that SQL
in Snowflake with a role allowed to alter the target user, such as
`ACCOUNTADMIN`, before validation; otherwise Snowflake key-pair authentication
will fail. Then set `SNOWFLAKE_PRIVATE_KEY_FILE` to the generated private key
path and use the same password for `SNOWFLAKE_PRIVATE_KEY_PASSWORD`.

```bash
cat > .env <<'EOF'
SNOWFLAKE_ACCOUNT=aaaaaa-bbbbbbb
SNOWFLAKE_USER=STREAMEV
SNOWFLAKE_PRIVATE_KEY_FILE=snowflake/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=compute_wh
SNOWFLAKE_ROLE=STREAM
SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE
EOF
```

Run these commands from the repo root so the relative key path resolves
correctly. If your pipe has a different name, change `SNOWFLAKE_PIPE_NAME` in
`.env`.

Do not copy pipeline shape keys such as `EVENTHUB_NAMESPACE`, `EVENTHUBNAME_1`,
`TARGET_DB`, or `SNOWFLAKE_1_DATABASE` into `.env` when using TOML. `.env`
overrides TOML.

For the single-target setup above, no Snowflake database or schema variables are
needed in `.env`.

Keep local config files out of Git:

```bash
grep -qxF 'config/evsnow.toml' .git/info/exclude || \
  printf '\n# Local EvSnow runtime config\nconfig/evsnow.toml\n' >> .git/info/exclude
git check-ignore -v config/evsnow.toml .env
git status --short --ignored -- config/evsnow.toml .env
```

### 3. Validate and run

The example uses Azure CLI authentication. Log in before running the pipeline:

```bash
az login
```

For Azure RBAC, the pipeline identity needs `Azure Event Hubs Data Receiver`.
To use the sender below, it also needs `Azure Event Hubs Data Sender`.

First validate the config:

```bash
uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env
```

Then test startup without ingesting:

```bash
uv run evsnow run --config-file config/evsnow.toml --env-file .env --dry-run
```

Then run the pipeline in terminal 1:

```bash
uv run evsnow run --config-file config/evsnow.toml --env-file .env
```

Leave terminal 1 running. Then open terminal 2 and send test messages. The
sender uses Azure `DefaultAzureCredential`; after `az login`, it can use your
Azure CLI identity. Use the same namespace and Event Hub name that you set in
`config/evsnow.toml`:

```bash
uv run python tools/eventhub_sender/main.py \
  --namespace eventhub1.servicebus.windows.net \
  --eventhub topic1 \
  --count 10000 \
  --batch-size 1000
```

The sender uses explicit Event Hub flags here because `.env` is reserved for
secrets and local credentials.

`validate-config` validates the resolved settings and can prompt to verify or
create the configured control table. If `config/evsnow.toml` exists in the
current working directory, EvSnow also discovers it by default. Passing
`--config-file` keeps the source explicit for local testing and runbooks.

When the pipeline starts successfully, logs show the Event Hub name, Snowflake
target, and `Starting to receive messages`. If the batch has `0 messages`, the
consumer is connected but has not received new events yet.

The example above is a local single-consumer smoke-test setup. For production,
use durable ownership with a Snowflake Hybrid Table, or choose the Postgres
control-table backend. See [Runtime options](#runtime-options) for those
options.

### Snowflake authentication reference

Generate RSA key pair for authentication:

```bash
./generate_snowflake_keys.sh
```

See [SNOWFLAKE_QUICKSTART.md](./SNOWFLAKE_QUICKSTART.md) for detailed
instructions. The SQL looks like this:

```sql
ALTER USER STREAMEV SET RSA_PUBLIC_KEY='MIIBIjANBgkqhki...';
```

Keep private keys outside Git and point `SNOWFLAKE_PRIVATE_KEY_FILE` at that
local path. Before committing, verify the key path, `.env`, and local TOML are
not staged:

```bash
git check-ignore -v snowflake/ .env config/evsnow.toml
git status --short --ignored -- snowflake/ .env config/evsnow.toml
```

### Azure authentication

The first-run path already uses `az login`. If you skipped that path, run it
before using `credential_mode = "azure_cli"`:

```bash
az login
```

For production-capable `DefaultAzureCredential` behavior, use
`credential_mode = "default"` in TOML. That supports service-principal
environment variables and managed identity in Azure-hosted runtimes.

For a user-assigned managed identity, set the client ID on each Event Hub config
and use `credential_mode = "default"`:

```toml
[event_hubs.EVENTHUBNAME_1]
name = "topic1"
namespace = "eventhub1.servicebus.windows.net"
consumer_group = "$Default"
credential_mode = "default"
managed_identity_client_id = "00000000-0000-0000-0000-000000000000"
```

To use Azure CLI-only auth for local testing:

```toml
[eventhub_defaults]
credential_mode = "azure_cli"
```

For the pipeline, you can also provide a per-Event Hub connection string in
`.env`:

```bash
EVENTHUBNAME_1_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKey=..."
```

Reference:
- [DefaultAzureCredential for Python](https://learn.microsoft.com/python/api/azure-identity/azure.identity.aio.defaultazurecredential)
- [EventHubConsumerClient options](https://learn.microsoft.com/python/api/azure-eventhub/azure.eventhub.eventhubconsumerclient)

## Command reference

```bash
# Validate configuration
uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env

# Run the pipeline
uv run evsnow run --config-file config/evsnow.toml --env-file .env

# Check status
uv run evsnow status --config-file config/evsnow.toml --env-file .env

# Dry run (test without ingesting)
uv run evsnow run --config-file config/evsnow.toml --env-file .env --dry-run
```

### Starting Fresh

Choose where EvSnow starts when there is no saved checkpoint.

Only truncate destination or checkpoint tables in a development/test account
after confirming the active Snowflake role, database, schema, and table names.

```bash
# Development/test only: starting fresh after truncating tables
snowsql -q "truncate table ingestion.public.events_table1;"
snowsql -q "truncate table control.public.ingestion_status;"

# EvSnow will use starting_position_on_no_checkpoint on the next run.
uv run evsnow run --config-file config/evsnow.toml --env-file .env
```

```toml
# Read all existing messages first. This is the default.
starting_position_on_no_checkpoint = "-1"

# Read only messages that arrive after the consumer connects.
starting_position_on_no_checkpoint = "@latest"

```

After EvSnow saves checkpoints, it resumes from the last checkpoint and ignores
this setting. If you truncate the checkpoint table, the next run behaves like a
first run again.

Use `-1` when you do not want to miss existing messages. Use `@latest` for
development tests that only need new messages.

Reference:
- [Event Hubs Event Position](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers)
- [Starting Position Options](https://learn.microsoft.com/en-us/python/api/azure-eventhub/azure.eventhub.eventhubconsumerclient#azure-eventhub-eventhubconsumerclient-receive)

## Runtime options

### Control table backends

The local example uses Snowflake for checkpoints and in-memory partition
ownership:

```toml
[control]
backend = "snowflake"
ownership_mode = "local_single_consumer_smoke"
use_hybrid_table = false
```

This mode is only for one local consumer. It persists checkpoints to a standard
Snowflake table, but it does not validate multi-consumer ownership or failover.

For production with Snowflake as the control backend, use durable ownership with
a Snowflake Hybrid Table:

```toml
[control]
backend = "snowflake"
ownership_mode = "durable"
use_hybrid_table = true
```

Hybrid Tables are required for durable Snowflake ownership because standard
Snowflake table primary and unique constraints are not enforced.

For Postgres checkpoints, set `[control].backend = "postgres"` and keep the
Postgres password or Azure token settings in `.env`:

```bash
CONTROL_PG_HOST=localhost
CONTROL_PG_PORT=5432
CONTROL_PG_USER=checkpoint_user
CONTROL_PG_PASSWORD=checkpoint_password
CONTROL_PG_SSLMODE=require
CONTROL_PG_AUTH_MODE=password
```

When `CONTROL_PG_AUTH_MODE=azure_token`, EvSnow uses `DefaultAzureCredential`
and passes the access token as the password. In that mode,
`CONTROL_PG_PASSWORD` is ignored.

For one mapped Snowflake target, EvSnow derives the Snowflake connection
session database/schema from `[snowflake_configs.SNOWFLAKE_1]` in TOML. If you
map to multiple database/schema pairs, set `SNOWFLAKE_DATABASE` and
`SNOWFLAKE_SCHEMA_NAME` explicitly as the session context.

### Smart Retry

Smart Retry is optional. It uses an LLM to classify failures before retrying:

```bash
# Add to .env
SMART_RETRY_ENABLED=true
SMART_RETRY_LLM_PROVIDER=azure
SMART_RETRY_LLM_MODEL=gpt-4o-mini
SMART_RETRY_LLM_API_KEY=your-key
SMART_RETRY_LLM_ENDPOINT=https://your-deployment.cognitiveservices.azure.com/...
SMART_RETRY_MAX_ATTEMPTS=3
SMART_RETRY_TIMEOUT_SECONDS=10
SMART_RETRY_ENABLE_CACHING=true
```

Run with `--smart` flag:

```bash
uv run evsnow run --config-file config/evsnow.toml --env-file .env --smart
```

### Logfire Observability

Send structured traces/logs for observability:

```bash
# Add to .env
LOGFIRE_ENABLED=true
LOGFIRE_TOKEN=your_logfire_token
LOGFIRE_SERVICE_NAME=evsnow
LOGFIRE_ENVIRONMENT=production
LOGFIRE_SEND_TO_LOGFIRE=true
LOGFIRE_CONSOLE_LOGGING=true
LOGFIRE_LOG_LEVEL=INFO
```

Get your token at [logfire.pydantic.dev](https://logfire.pydantic.dev)

### Snowpipe Streaming

The pipeline uses Snowflake's high-performance Snowpipe Streaming SDK and
requires a Snowflake PIPE object.

The main `.env` example already includes `SNOWFLAKE_PIPE_NAME`. Create the pipe
with the [Snowflake setup guide](./SNOWFLAKE_COMPLETE_SETUP.md) or
`setup_snowpipe_streaming.sql` before running the pipeline.

## Configuration reference

See [Configuration](./docs/configuration.md), [`config/evsnow.example.toml`](./config/evsnow.example.toml), and [`.env.example`](./.env.example) for the full environment-variable reference.

## Docs

- [Configuration](./docs/configuration.md) - TOML config, `.env` secrets, validation, and examples
- [Python Pipeline Hardening Plan](./docs/python-pipeline-hardening-plan.md) - Migration findings, milestones, and remaining hardening work
- [Snowflake setup guide](./SNOWFLAKE_COMPLETE_SETUP.md) - Snowflake table, pipe, and role setup

## License

See [LICENSE](./LICENSE) for details.
