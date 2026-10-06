# First Run

This tutorial runs one Event Hub into one Snowflake target with one checkpoint
table. It keeps pipeline shape in TOML and keeps secrets in `.env`.

## Before You Start

You need:

- Python `3.13` or newer and `uv`.
- Azure CLI and access to your Azure subscription.
- Snowflake CLI and access to an active Snowflake account.

The setup pages below create any missing cloud objects and generate the
encrypted private key. If those objects already exist, have their names and the
runtime user's private key available.

## Install

```bash
git clone https://github.com/MiguelElGallo/evsnow.git
cd evsnow
uv sync
```

You now have the `evsnow` CLI available through `uv run`.

## Choose Your Starting Point

Complete the missing setup steps in this order:

1. If the namespace or Event Hub does not exist, follow
   [Event Hub quickstart](../getting-started/event-hub-quickstart.md).
2. If the Snowflake objects do not exist, follow
   [Snowflake quickstart](../getting-started/snowflake-quickstart.md). It creates
   the `STREAM` role, `STREAMEV` user, `CONTROL` and `INGESTION` databases,
   target Iceberg table, streaming pipe, and encrypted private key.
3. Return here to configure and run the pipeline. If both services are already
   ready, continue below using their existing names and credentials.

## Create The Runtime Files

```bash
[ -f config/evsnow.toml ] || cp config/evsnow.example.toml config/evsnow.toml
```

The command preserves an existing configuration from setup. Edit
`config/evsnow.toml` for your first Event Hub and Snowflake table:

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
starting_position_on_no_checkpoint = "@latest"

[event_hubs.EVENTHUBNAME_1]
name = "topic1"
namespace = "eventhub1.servicebus.windows.net"
consumer_group = "$Default"

[snowflake_configs.SNOWFLAKE_1]
database = "INGESTION"
schema_name = "PUBLIC"
table_name = "EVENTS_TABLE1"
batch_size = 3

[[mappings]]
event_hub_key = "EVENTHUBNAME_1"
snowflake_key = "SNOWFLAKE_1"
```

Keep your actual namespace, Event Hub name, and Snowflake target values.
Set `batch_size = 3` and `starting_position_on_no_checkpoint = "@latest"` as shown,
including when reusing a file from setup. Start the pipeline before sending the
three example messages below. The complete batch then flushes without waiting
for the default batch timeout.

After the first run, raise `batch_size` for normal throughput. Change
`starting_position_on_no_checkpoint` to `-1` only when you intentionally want to
backfill retained Event Hub messages. `EVENTHUBNAME_1` and `SNOWFLAKE_1` are
local mapping keys.

## Create `.env`

Use the encrypted key created during Snowflake setup. You can start from the
local template:

```bash
[ -f .env ] || cp .env.example .env
```

The command preserves any credentials you already configured. Keep only the
local credentials needed by the run:

```bash
SNOWFLAKE_ACCOUNT=aaaaaa-bbbbbbb
SNOWFLAKE_USER=STREAMEV
SNOWFLAKE_PRIVATE_KEY_FILE=snowflake/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=STREAM
SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE
```

If local Azure CLI auth is not the path you want to use, add the Event Hub
connection string to `.env`:

```bash
AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKey=..."
```

When a connection string is present, EvSnow uses it for the receiver and the
sender utility uses it automatically. `credential_mode = "azure_cli"` only
applies when no connection string is configured.

Do not put pipeline shape keys such as `EVENTHUB_NAMESPACE`, `TARGET_DB`, or
`SNOWFLAKE_1_DATABASE` in `.env` for this path. An explicit `--env-file`
overrides TOML, so keeping shape in TOML makes the run easier to inspect.
For one mapped Snowflake target, EvSnow derives the Snowflake session
database/schema from the target in `config/evsnow.toml`.

## Validate

```bash
az login
uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env
```

Continue only when validation reports
`Snowflake control table verified/created successfully` with no warnings.
A warning can accompany exit status `0`; resolve it before starting the pipeline.

The Azure identity needs `Azure Event Hubs Data Receiver`. If you use the
included sender utility, it also needs `Azure Event Hubs Data Sender`. Use
[Event Hub quickstart](../getting-started/event-hub-quickstart.md) when the
namespace, Event Hub, or local RBAC grants do not exist yet.

## Dry Run

```bash
uv run evsnow run --config-file config/evsnow.toml --env-file .env --dry-run
```

The dry run validates startup without ingesting events.

## Run The Pipeline

Open terminal 1:

```bash
uv run evsnow run --config-file config/evsnow.toml --env-file .env
```

When startup succeeds, logs show the Event Hub name, Snowflake target, and
`Starting to receive messages`.

If the receiver fails with `Failed to invoke Azure CLI`, first confirm `az login`
and `az account get-access-token --resource https://eventhubs.azure.net/` work
in the same shell. For a quick local run, use
`AZURE_EVENTHUB_CONNECTION_STRING` in `.env`; for production, prefer
`credential_mode = "default"` with a service principal or managed identity.

Open terminal 2 and send example messages:

```bash
RUN_ID="evsnow-first-run-$(date -u +%Y%m%dT%H%M%SZ)"
START_ID=$(date -u +%s)

uv run python tools/eventhub_sender/main.py \
  --namespace eventhub1.servicebus.windows.net \
  --eventhub topic1 \
  --count 3 \
  --start-id "$START_ID" \
  --batch-size 3 \
  --credential-mode azure_cli \
  --partition-key "$RUN_ID" \
  --payload "{\"run_id\":\"$RUN_ID\",\"purpose\":\"first-run\"}"
```

Use the namespace and Event Hub name from `config/evsnow.toml`.

Then prove those messages reached Snowflake:

```bash
set -a
source .env
set +a

TARGET_DATABASE=INGESTION
TARGET_SCHEMA=PUBLIC
TARGET_TABLE=EVENTS_TABLE1

PRIVATE_KEY_PASSPHRASE="$SNOWFLAKE_PRIVATE_KEY_PASSWORD" \
snow sql -x \
  --account "$SNOWFLAKE_ACCOUNT" \
  --user "$SNOWFLAKE_USER" \
  --authenticator SNOWFLAKE_JWT \
  --private-key-file "$SNOWFLAKE_PRIVATE_KEY_FILE" \
  --role "$SNOWFLAKE_ROLE" \
  --warehouse "$SNOWFLAKE_WAREHOUSE" \
  --database "$TARGET_DATABASE" \
  --schema "$TARGET_SCHEMA" \
  --format JSON \
  -q "WITH proof AS (
          SELECT TRY_PARSE_JSON(EVENT_BODY):sequence_id::NUMBER AS sequence_id
          FROM ${TARGET_DATABASE}.${TARGET_SCHEMA}.${TARGET_TABLE}
          WHERE TRY_PARSE_JSON(EVENT_BODY):payload:run_id::STRING = '$RUN_ID'
      )
      SELECT COUNT(*) AS rows_arrived,
             LISTAGG(sequence_id::STRING, ',')
               WITHIN GROUP (ORDER BY sequence_id)
             AS sequence_ids,
             IFF(COUNT(*) = 0, NULL, MAX(sequence_id) - MIN(sequence_id) + 1 - COUNT(*))
             AS missing_sequence_count
      FROM proof;"
```

!!! note "Arrival timing"

    Snowpipe Streaming flush and consumer checkpoint timing are asynchronous.
    If the first query returns `rows_arrived = 0`, wait 15 seconds and rerun
    the same query while the pipeline is still running. The required result is
    `rows_arrived = 3` and `missing_sequence_count = 0`.

Use [Event Hub sender](../tools/eventhub-sender.md) for the longer sender
reference and repeatable arrival checks.

## What Happened

``` { .mermaid data-search-exclude }
sequenceDiagram
    participant Sender as Event Hub sender
    participant EventHub as Azure Event Hubs
    participant EvSnow as EvSnow
    participant Control as Control table
    participant Snowflake as Snowflake target

    Sender->>EventHub: Publish example events
    EvSnow->>EventHub: Receive batches
    EvSnow->>Snowflake: Append through Snowpipe Streaming
    EvSnow->>Control: Save checkpoints
```

If a batch has `0 messages`, the consumer is connected but no new events have
arrived. After EvSnow saves checkpoints, later runs resume from the saved
offsets and ignore `starting_position_on_no_checkpoint`.

## Next Steps

- Use [Configuration](../configuration.md) for the full TOML and `.env` reference.
- Use [Snowflake key-pair auth](../snowflake/key-pair-auth.md) to troubleshoot RSA authentication.
- Use [Snowflake quickstart](../getting-started/snowflake-quickstart.md) when the runtime user, target database, pipe, or grants do not exist yet.
- Use [Event Hub sender](../tools/eventhub-sender.md) for repeatable local message sends.
