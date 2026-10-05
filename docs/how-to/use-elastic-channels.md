# Enable Elastic Channels

Use Elastic mode to stream through Snowflake's implicit channel while EvSnow
keeps Event Hubs checkpoints in its existing control table. Named Channels remain
the default. Elastic mode provides at-least-once delivery, so keep a stable event
ID in every producer payload and plan for replay duplicates.

For the delivery model and the difference between IDs, source offsets, and append
tokens, see [Elastic acknowledgements and replay](../explanation/elastic-channels.md).

## Prepare the target and pipe

Start with a working [first run](../tutorial/first-run.md). You need the runtime
service user, checkpoint table, target table, and custom streaming pipe. The
quickstart uses `INGESTION.PUBLIC.EVENTS_TABLE1` and
`INGESTION.PUBLIC.EVENTS_TABLE_PIPE`; use your configured names below.

Run `uv sync --locked` from the repository root. Elastic mode requires
`snowpipe-streaming` 1.8.0 or later, which the project dependency and lockfile
provide. EvSnow uses the custom pipe named by `SNOWFLAKE_PIPE_NAME`; it does not
switch to Snowflake's automatic table pipe. The runtime role needs database and
schema `USAGE`, target-table `INSERT`, and custom-pipe `OPERATE`.
[Snowflake SDK tutorial](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-getting-started),
[access control](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-access-control).

Enable processing-error capture using a setup role. Choose the statement that
matches the target's table type:

```sql
-- Native Snowflake table:
ALTER TABLE INGESTION.PUBLIC.EVENTS_TABLE1 SET ERROR_LOGGING = TRUE;

-- Snowflake-managed Iceberg table, as created by the quickstart:
ALTER ICEBERG TABLE INGESTION.PUBLIC.EVENTS_TABLE1 SET ERROR_LOGGING = TRUE;
```

An append acknowledgement confirms durable buffering. Rows can still fail type
conversion or pipe transformations afterwards; error logging preserves those
rows for diagnosis and recovery.
[Snowflake error logging](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-error-tables).

## Select Elastic mode

Add these non-secret settings to the `.env` used for this run:

```dotenv
SNOWFLAKE_CHANNEL_MODE=elastic
SNOWFLAKE_ACK_TIMEOUT_SECONDS=60
SNOWFLAKE_CLOSE_TIMEOUT_SECONDS=60
```

Both timeouts must be positive integers. The acknowledgement timeout bounds one
wait for a submitted batch. The close timeout bounds the Elastic adapter's wait
for pending acknowledgements and SDK client close, not the entire consumer or
pipeline shutdown. If an
acknowledgement wait times out, EvSnow retains the original Future and observes
it on the next attempt for that batch; it does not immediately append the rows
again. Until a processing attempt reports success, the source checkpoint does
not advance.

These settings also exist as `channel_mode`, `ack_timeout_seconds`, and
`close_timeout_seconds` in a complete `[snowflake_connection]` TOML table. A
table containing only those three keys is invalid: the current configuration
model requires all connection fields before applying environment overrides.
Use `.env` for the standard example that already keeps connection credentials
there. See [Snowflake connection settings](../reference/parameters.md#snowflake-connection-settings).

Validate and inspect the resolved run:

```bash
uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env
uv run evsnow run --config-file config/evsnow.toml --env-file .env --dry-run
```

Continue when validation has no warnings, then start the pipeline:

```bash
uv run evsnow run --config-file config/evsnow.toml --env-file .env
```

Use one local consumer when `control.ownership_mode` is
`local_single_consumer_smoke`. That mode persists source checkpoints in a
standard Snowflake table and holds partition ownership in memory. Elastic mode
does not change that ownership contract or require Azure Blob checkpoint storage.

## Verify rows and processing errors

Wait for target-table visibility after the durable acknowledgement. Query the
configured target with a role that has `SELECT` access:

Reading processing errors also requires `SELECT ERROR TABLE` on the target.
Grant it to the operational role during setup:

```sql
GRANT SELECT ERROR TABLE ON TABLE INGESTION.PUBLIC.EVENTS_TABLE1 TO ROLE STREAM;
```

```sql
SELECT COUNT(*) AS raw_rows
FROM INGESTION.PUBLIC.EVENTS_TABLE1;

SELECT timestamp, error_code,
       error_metadata:error_message::STRING AS error_message,
       error_metadata:details:pipe_name::STRING AS pipe_name,
       error_data:"$1"::STRING AS raw_payload
FROM ERROR_TABLE(INGESTION.PUBLIC.EVENTS_TABLE1)
WHERE error_metadata:service = 'snowpipe_streaming'
ORDER BY timestamp DESC;
```

Use your producer's stable event ID to check distinct events, missing IDs, and
payload values. A raw row count alone can hide missing events behind duplicates.
If rows remain missing, inspect the error table and the custom pipe definition
and grants. Buffer acknowledgement is not a target-row validation result.
[Snowflake Elastic operations](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-operations).

## Run the 200000 event proof

Prepare disposable Azure and Snowflake resources and a configuration pointing
only to those resources. The harness uses the configured resources; it does not
provision them. Keep private keys and the run's `.env` outside tracked files.
The test target must expose `EVENT_BODY`, `EVENTHUB_NAMESPACE`, `EVENTHUB_NAME`,
`PARTITION_ID`, and `SEQUENCE_NUMBER` for its SQL checks. In the custom pipe, map
the last four from `$1:eventhub_namespace`, `$1:eventhub_name`,
`$1:partition_id`, and `$1:sequence_number`. The quickstart pipe already maps the
partition and sequence fields; add namespace and hub columns to the isolated
test table and pipe when adapting that schema. Enable `ERROR_LOGGING` on that
target and use a fresh control-table scope.

```bash
uv run python tools/elastic_live_test.py \
  --config-file config/evsnow.toml \
  --env-file .env \
  --run-id elastic-proof-001 \
  --events 200000 \
  --evidence-dir .local/elastic-proof-001
```

Choose a fresh run ID for each proof and a fresh target/control-table scope.
Keep raw run artifacts under the ignored `.local/` directory, then copy only
sanitized evidence into documentation. The producer sends deterministic JSON
with root fields `run_id`, `event_id`, `value`,
and `message`. For event IDs `0` through `199999`, `value = event_id * 17 + 3`,
`message = "elastic-"` followed by the six-digit ID, and partition = `event_id % 2`.

The harness sends two halves with a consumer restart between them, records
checkpoints and resumed messages, and polls SQL visibility. It then replays the
last 20 source events from each partition. Report the raw row count separately
from the 200,000 distinct IDs and the deduplicated result; this replay is intended
to show duplicate behavior. Inspect
[live test results](../development/live-test-results.md) for the recorded run and
cleanup evidence.

### Use Azure Basic for a disposable test

Basic with one throughput unit and two partitions is sufficient for this proof.
Use `$Default`, its single consumer group. Basic retains events for one day and
limits each publish request, including a batch, to 256 KB. The harness defaults
to 800 events per second and batches of at most 240 KiB, below the one-TU ingress
limits of 1,000 events per second and 1 MB per second for these small payloads.
[Azure tier limits](https://learn.microsoft.com/en-us/azure/event-hubs/compare-tiers).

For an isolated namespace, select your test subscription, then create only this
run's resource group:

```bash
az account set --subscription <test-subscription-id-or-name>
RUN_SUFFIX="$(date -u +%Y%m%d%H%M%S)"
RESOURCE_GROUP="rg-evsnow-elastic-${RUN_SUFFIX}"
EVENTHUB_NAMESPACE="evsnowelastic${RUN_SUFFIX}"
EVENTHUB_NAME=elastic-events
LOCATION=swedencentral

az group create --name "$RESOURCE_GROUP" --location "$LOCATION" \
  --tags project=evsnow purpose=elastic-test
az eventhubs namespace create --resource-group "$RESOURCE_GROUP" \
  --name "$EVENTHUB_NAMESPACE" --location "$LOCATION" --sku Basic --capacity 1
az eventhubs eventhub create --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$EVENTHUB_NAMESPACE" --name "$EVENTHUB_NAME" \
  --partition-count 2 --retention-time 24
```

Grant the test identity scoped sender and receiver roles using the
[Event Hub quickstart](../getting-started/event-hub-quickstart.md#grant-local-smoke-test-access),
then put the new namespace and hub names in the run's TOML. Use an isolated
Snowflake native table, custom pipe, checkpoint table, service user, and X-Small
warehouse with auto-suspend. Avoid a separate Azure compute service for this local
test. Streaming uses Snowflake's ingestion service; checkpoint and verification
SQL use the warehouse.

After collecting the SQL and replay evidence, delete this run's resource group
and verify deletion. Apply the same cleanup if the test fails:

```bash
az group delete --name "$RESOURCE_GROUP" --yes --no-wait
az group wait --name "$RESOURCE_GROUP" --deleted
az group exists --name "$RESOURCE_GROUP"
```

The last command must return `false`. Suspend the isolated Snowflake warehouse
and disable the test service user when the proof is complete. Keep the result
table and sanitized evidence available for review.

## Return to Named mode

Set `SNOWFLAKE_CHANNEL_MODE=named`, or remove the mode setting to use the default.
Run validation and the dry run again. Named mode retains the existing partition
channel names and offset-token behavior. Source checkpoints continue to use the
configured control table.
