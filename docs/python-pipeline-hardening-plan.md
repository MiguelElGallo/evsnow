# Python Pipeline Hardening Plan

This plan tracks the Python fixes chosen instead of a Rust migration. The gates below are intentionally scoped so each one can be implemented, peer reviewed, and verified before moving on.

## Gate 1 - Snowpipe Commit Safety And Partition-Safe Routing

Status: Accepted

Scope:
- Added Snowpipe Streaming SDK import compatibility for current and older `ChannelStatus` layouts.
- Prefer `append_rows(...)` with `start_offset_token` and `end_offset_token`, with `append_row(...)` fallback.
- Require Event Hub `sequence_number` for source-stable Snowflake offset tokens.
- Wait for Snowflake `wait_for_commit(...)` before reporting ingest success.
- Force channel-status validation and fail closed on row errors or fatal channel status.
- Group orchestrator batches by Event Hub partition.
- Route each Event Hub partition to a stable Snowflake channel suffix.
- Fail the batch if any partition ingest fails.

Verification:
- `uv run pytest -q tests/unit/test_snowflake_client.py tests/unit/test_streaming.py tests/unit/test_orchestrator.py`
- Result: passed during gate review.

## Gate 2 - Event Hub Fail-Closed Batch Contract

Status: Accepted

Scope:
- `_process_batch(...)` returns a boolean success/failure contract.
- Sync processors run via `asyncio.to_thread(...)`.
- Async processor functions and async callable instances are awaited.
- Batch mutation is serialized with batch and processing locks.
- Ready and timed-out batches are detached before processing.
- Detached batches are tracked in an explicit FIFO queue.
- Failed-batch restore rebuilds the current batch from detached backlog first, then newer partial batch messages.
- Processor failure, checkpoint retry exhaustion, or missing `partition_context` fails the batch.
- Failed batches do not checkpoint and do not advance success stats.
- Batch failure shuts down the receive loop/client.
- Snowflake committed replay still validates channel status before reporting success.

Verification:
- `uv run pytest -q tests/unit/test_snowflake_client.py tests/unit/test_eventhub.py tests/unit/test_orchestrator.py tests/unit/test_streaming.py`
- Result: `168 passed`, with existing warnings under review.
- `uv run ruff check --select F811 ...`
- Result: passed.

## Gate 3 - Durable Checkpoint Ownership And Metadata

Status: Accepted

Scope:
- Replaced in-memory partition ownership with durable backend delegation.
- Added Snowflake and Postgres ownership list/claim helpers.
- Snowflake ownership requires a Hybrid Table with an enforced primary key for safe compare-and-set claims.
- If an existing Snowflake ownership table is a standard table, setup fails closed with migration guidance.
- Postgres ownership uses primary-key-backed upsert/update semantics.
- Checkpoint backend calls are offloaded with `asyncio.to_thread(...)` behind manager-level I/O locks.
- Checkpoint loads fail closed instead of silently starting from an unsafe position.
- Checkpoint save failure propagates through the SDK checkpoint path.
- Event Hub offset and sequence number are preserved separately.
- Startup logging handles both legacy integer checkpoints and metadata-shaped checkpoints.
- Snowflake ownership helper paths validate warehouse identifiers before issuing `USE WAREHOUSE`.

Verification:
- `uv run pytest -q tests/unit/test_snowflake_client.py tests/unit/test_eventhub.py tests/unit/test_orchestrator.py tests/unit/test_streaming.py tests/unit/test_snowflake_utils.py`
- Result: `210 passed`, with existing warnings under review.
- Targeted `ruff` `F401` / `F811` checks passed for changed source files.

## Gate 4 - Azure Identity And SDK Options

Status: Accepted

Scope:
- Aligned code and docs around production identity behavior.
- Prefer `DefaultAzureCredential` for production-capable authentication.
- Keep `AzureCliCredential` as an explicit local-development opt-in.
- Added user-assigned managed identity client ID support.
- Close credentials on token validation failure and shutdown.
- Pass configured Event Hubs retry and load-balancing options into `EventHubConsumerClient`.
- Pass configured prefetch, max-wait, last-enqueued tracking, and error callback options into `receive(...)`.
- Preserve fail-closed behavior when the SDK calls `on_error`, even if the SDK swallows callback exceptions.
- Fail closed on receive-time credential errors instead of leaving the consumer running.
- Validate partition ownership expiration against the load-balancing interval.
- Allow Azure's documented `max_wait_time=0` receive mode.
- Reject unsafe fresh-start `starting_position=0`; use `-1` for beginning or `@latest` for new events.
- Updated README and `.env.example` for identity mode, SDK options, and connection-string override behavior.

Verification:
- `uv run pytest -q tests/unit`
- Result: `392 passed`, with 2 existing warnings under review.
- `uv run ruff check`
- Result: passed.
- `uv run python -m py_compile src/utils/azure_identity.py src/utils/config_models/eventhub.py src/utils/config.py src/consumers/eventhub.py src/main.py`
- Result: passed.

## Open Gates

Gate 5 - Verification And Handoff:
- Run a live two-consumer Snowflake/Event Hub ownership and load-balancing smoke test if credentials and test infrastructure are available.
- Update operator docs for identity, ownership, checkpoint behavior, and remaining warnings.
- Verify that code comments explain the chosen safety/SDK decisions and include vendor documentation links where those decisions depend on external SDK behavior.
- Document that Snowflake ownership uses a Hybrid Table for enforced primary-key claims; operators without Hybrid Table support/privileges should use Postgres control tables or migrate/drop an existing standard ownership table before enabling Snowflake ownership.

Latest live-smoke status:
- Full mocked test suite passes: `405 passed`, with 2 existing warnings.
- Live Event Hub sender succeeds against `evsnowqa20260428230344.servicebus.windows.net/topic1` when using a namespace SAS connection string retrieved from Azure CLI.
- Live pipeline with Snowflake control-table ownership fails closed before receiving messages because Snowflake Hybrid Tables are not available on the current trial account.
- Live pipeline with Docker-backed Postgres control tables succeeds end to end: the sender wrote 5 marker messages, Event Hub receive processed them, Snowpipe Streaming flushed partition channels, and Snowflake query found 5 marker rows.
- The configured Azure Postgres fallback is still not currently usable from this machine: the configured host is stale, and the discovered Azure Postgres hosts reject the configured credentials.
