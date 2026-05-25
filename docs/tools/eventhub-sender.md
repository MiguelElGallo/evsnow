# Event Hub Sender Utility

Small Typer CLI to push sequentially numbered JSON messages to an Event Hub so you can verify downstream ingestion gaps.

## Usage

```bash
uv run python tools/eventhub_sender/main.py \
  --eventhub my-hub \
  --namespace my-namespace.servicebus.windows.net \
  --count 100 \
  --start-id 1 \
  --batch-size 50 \
  --payload '{"kind":"test"}'
```

Or use a connection string (env var supported):

```bash
export AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKey=..."
uv run python tools/eventhub_sender/main.py --eventhub my-hub --count 20

# If your .env already has EVENTHUB_NAMESPACE, EVENTHUBNAME_1, and AZURE_EVENTHUB_CONNECTION_STRING,
# you can omit those flags and the CLI will pick them up automatically:
uv run python tools/eventhub_sender/main.py --count 20
```

This Typer app is mounted as a single command. Pass options directly to
`main.py`; do not add a `send` subcommand.

## Message shape

Each message is a JSON object:

```json
{
  "sequence_id": 42,
  "sent_at": "2025-12-04T12:34:56.789Z",
  "source": "evsnow-cli-sender",
  "trace_id": "<uuid>",
  "payload": { "kind": "test" }
}
```

`sequence_id` increments monotonically from `--start-id` so you can detect missing ids after ingestion.

## End-to-End Arrival Check

Use a unique `run_id` when you want to verify that a small test batch reached
Snowflake.

```bash
RUN_ID="evsnow-smoke-$(date -u +%Y%m%dT%H%M%SZ)"
START_ID=$(date -u +%s)

uv run python tools/eventhub_sender/main.py \
  --count 3 \
  --start-id "$START_ID" \
  --batch-size 3 \
  --partition-key "$RUN_ID" \
  --payload "{\"run_id\":\"$RUN_ID\",\"purpose\":\"arrival-check\"}"
```

Then query the target table:

```bash
set -a
source .env
set +a

snow sql -x \
  --account "$SNOWFLAKE_ACCOUNT" \
  --user "$SNOWFLAKE_USER" \
  --authenticator SNOWFLAKE_JWT \
  --private-key-file "$SNOWFLAKE_PRIVATE_KEY_FILE" \
  --role "$SNOWFLAKE_ROLE" \
  --warehouse "$SNOWFLAKE_WAREHOUSE" \
  --database "$SNOWFLAKE_1_DATABASE" \
  --schema "$SNOWFLAKE_1_SCHEMA" \
  --format JSON \
  -q "SELECT COUNT(*) AS rows_arrived,
             LISTAGG(TRY_PARSE_JSON(EVENT_BODY):sequence_id::STRING, ',')
               WITHIN GROUP (ORDER BY TRY_PARSE_JSON(EVENT_BODY):sequence_id::NUMBER)
             AS sequence_ids
      FROM ${SNOWFLAKE_1_DATABASE}.${SNOWFLAKE_1_SCHEMA}.${SNOWFLAKE_1_TABLE}
      WHERE TRY_PARSE_JSON(EVENT_BODY):payload:run_id::STRING = '$RUN_ID';"
```

For a 3-message smoke test, `rows_arrived` should be `3` and the
`sequence_ids` should be consecutive.
