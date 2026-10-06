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
`snowpipe-streaming` 1.8.1 or later, which the project dependency and lockfile
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

A terminal batch failure, failed final drain, or client shutdown error makes
`evsnow run` exit with status `1` after cleanup, so a supervisor can detect and
restart a failed pipeline. A late acknowledgement can make the final drain
succeed and save its checkpoint, but an earlier terminal failure still reports
status `1`. A successful graceful shutdown reports status `0`.

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

## Return to Named mode

Set `SNOWFLAKE_CHANNEL_MODE=named`, or remove the mode setting to use the default.
Run validation and the dry run again. Named mode retains the existing partition
channel names and offset-token behavior. Source checkpoints continue to use the
configured control table.
