# Event Hub Sender Utility

Small Typer CLI to push sequentially numbered JSON messages to an Event Hub so you can verify downstream ingestion gaps.

## Usage

```bash
uv run python tools/eventhub_sender/main.py send \
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
uv run python tools/eventhub_sender/main.py send --eventhub my-hub --count 20

# If your .env already has EVENTHUB_NAMESPACE, EVENTHUBNAME_1, and AZURE_EVENTHUB_CONNECTION_STRING,
# you can omit those flags and the CLI will pick them up automatically:
uv run python tools/eventhub_sender/main.py send --count 20
```

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
