# Elastic acknowledgements and replay

EvSnow offers two Snowpipe Streaming channel modes. Named Channels keep the
existing partition-channel and offset-token path. Elastic Channels use a single
implicit ingestion path per custom pipe, while Snowflake manages its capacity
and lifecycle. Named mode remains the compatibility default; enable Elastic
explicitly when at-least-once delivery fits your data model.
[Snowflake Elastic overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-overview).

## What a successful acknowledgement means

Elastic ingestion has two separate milestones. First, Snowflake durably buffers
the append and completes its acknowledgement Future. Then the pipe processes
the rows and commits successful rows to the target table. Query visibility and
row-level processing errors follow the first milestone.

``` { .mermaid data-search-exclude }
sequenceDiagram
    participant Source as Azure Event Hubs
    participant Consumer as EvSnow consumer
    participant Adapter as Elastic adapter
    participant Buffer as Snowflake durable buffer
    participant Control as Control table
    participant Table as Target table
    Source->>Consumer: Receive source batch
    Consumer->>Adapter: Submit partition batch
    Adapter->>Buffer: Append rows
    Buffer-->>Adapter: Durable acknowledgement
    par Source handoff
        Adapter-->>Consumer: Processing attempt succeeds
        Consumer->>Control: Save source checkpoint
    and Table processing
        Buffer->>Table: Validate and commit rows
    end
```

The checkpoint says the source data was handed durably to Snowflake. It does
not assert that every row passed a transformation. Enable target-table
`ERROR_LOGGING` and query `ERROR_TABLE(...)` to identify rejected rows; verify
expected IDs and payloads in the target separately.
[Snowflake acknowledgement semantics](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-error-handling),
[processing error capture](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-error-tables).

## How checkpoints survive a restart

EvSnow continues to use its existing Snowflake or Postgres control backend.
Source checkpoints are scoped to the Event Hubs namespace, hub, consumer group,
and partition. A batch must return success before the consumer can save its
source position. An append failure or unresolved acknowledgement leaves that
position unchanged.

Elastic acknowledgements can arrive out of order. EvSnow waits for the relevant
submitted batch before reporting success; a later acknowledgement does not
authorize skipping earlier unresolved work. A wait timeout retains the original
Future. Another processing attempt for the same source batch observes that
Future instead of submitting a second append. The Elastic adapter's close method
waits for pending acknowledgements and closes the SDK client within its close
budget; it never writes source checkpoints. The consumer can save checkpoints
during shutdown if its final drain processing attempt succeeds. The adapter's
close timeout does not bound the entire consumer or pipeline shutdown.
[Snowflake checkpoint guidance](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices).

The SDK's memory buffer and EvSnow's pending-Future state do not survive process
loss. A restart reads from the durable source checkpoint, so a previously
accepted append may be replayed if its acknowledgement or checkpoint was lost.
Event Hubs retention must cover the outage and replay period. Choose an Event Hubs tier and retention period that cover your recovery
requirement.
[Elastic buffer limitations](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-limitations),
[Azure retention limits](https://learn.microsoft.com/en-us/azure/event-hubs/compare-tiers).

## Three identifiers with different jobs

| Identifier | Example | What it identifies |
|---|---|---|
| Producer event ID | `run_id` plus `event_id` | A logical event. Preserve it when republishing the same event. |
| Source identity | Namespace, hub, partition, sequence number | One publication in one Event Hubs stream. Add consumer group when describing checkpoint scope. |
| Elastic append token | A compact batch identifier | Local correlation of a submitted append with its acknowledgement. |

Re-reading an Event Hubs record preserves its source identity. Republishing the
same logical event creates a new source sequence number, so the producer ID is
needed to recognize that duplicate across publications. EvSnow's row envelope
provides `eventhub_namespace`, `eventhub_name`, and `consumer_group` to the pipe.
Map namespace and hub into target columns when a table combines sources;
partition and sequence number alone are not globally unique.

An append token is held in SDK memory and is not a Snowflake deduplication key
or a saved source position. Neither it nor a REST request ID prevents duplicate
rows. Elastic has no ordered-ingestion guarantee, committed-offset recovery
query, or offset-token commit wait.
[Snowflake Elastic limitations](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-limitations).

## Deduplicate according to event meaning

Choose a deduplication key before relying on Elastic output. For a
producer that includes a run ID and event ID, `(run_id, event_id)` identifies an immutable logical event. The
following query produces one representative row per event from the quickstart
target shape:

```sql
WITH events AS (
    SELECT *, TRY_PARSE_JSON(EVENT_BODY) AS body
    FROM INGESTION.PUBLIC.EVENTS_TABLE1
)
SELECT * EXCLUDE body
FROM events
WHERE body:run_id::STRING = 'example-run-001'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY body:run_id::STRING, body:event_id::NUMBER
    ORDER BY ENQUEUED_TIME, PARTITION_ID, SEQUENCE_NUMBER
) = 1;
```

For mutable events, define which version wins and how conflicting payloads are
handled. Counting distinct IDs alone cannot establish payload correctness.
Keep raw rows for investigating replay, and verify missing IDs, unexpected IDs,
and payload values alongside the deduplicated result.

## Choose a channel mode

| Requirement | Named mode | Elastic mode |
|---|---|---|
| Existing EvSnow configuration | Default | Explicit opt-in |
| Channel lifecycle | Named partition channels | Implicit channel tied to the client |
| Source position in Snowflake | Named offset tokens | No offset-token support |
| Durable delivery handoff | Named channel commit path | Append acknowledgement |
| Duplicate handling | Named recovery relies on correct offset tracking and source replay | At-least-once output requires reconciliation when duplicates matter |
| Ordering | Named channels provide ordered ingestion | No ordering guarantee |

Snowflake recommends Named Channels when ordered ingestion or exactly-once
recovery is required. That recovery still depends on retained source records and
correct application offset handling. Elastic simplifies channel management but
does not remove source retention or downstream data-quality checks.
[Snowflake channel choice](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-overview).

Use [Enable Elastic Channels](../how-to/use-elastic-channels.md) for the settings,
run commands, and operational checks.
