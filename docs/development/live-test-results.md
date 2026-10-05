# Elastic Channels live test

The Elastic implementation passed a real Azure Event Hubs-to-Snowflake test on
5 October 2026. All 200,000 expected events arrived with correct contents and
source metadata. A persisted consumer restart introduced no duplicates. A
separate replay of 40 retained source events produced exactly 40 duplicate rows.
The disposable Azure resource group was deleted and its absence was verified.

This proof belongs to the fresh `MiguelElGallo/evsnow` checkout on branch
`mpz/elastic-channels`. The run ID is `elastic-20261005-1931`. It does not reuse
results from the earlier checkout.

## Results

| Check | Before explicit replay | After explicit replay |
|---|---:|---:|
| Source events published | 200,000 | 200,000 |
| Raw target rows | 200,000 | 200,040 |
| Distinct producer event IDs | 200,000 | 200,000 |
| Distinct source identities | 200,000 | 200,000 |
| Missing or invalid IDs | 0 | 0 |
| Invalid payloads | 0 | 0 |
| Invalid namespace, hub, partition or sequence metadata | 0 | 0 |
| Duplicate rows | 0 | 40 |
| Snowflake processing errors | 0 | 0 |

The test published IDs `0` through `199999`, each with `value = event_id * 17 + 3`
and a deterministic message. Each ID was sent to partition `event_id % 2`; the
expected source sequence was `floor(event_id / 2)`. SQL checked these values for
every row, including the replay rows. Every ID was present: 200,000 distinct
valid IDs in a range containing exactly 200,000 possible values.

The source identity is the namespace, hub, partition and sequence number.
It is distinct from the producer's event ID and from the SDK's local append token.

## Restart and replay

The production `PipelineMapping` and Event Hubs consumer ingested two halves:

| Stage | Published | First source sequence on both partitions | Last saved sequence on both partitions | Duration |
|---|---:|---:|---:|---:|
| Initial consumer | 100,000 | 0 | 49,999 | 137.153 s |
| Fresh consumer using persisted checkpoints | 100,000 | 50,000 | 99,999 | 139.187 s |

The second consumer's first event IDs were `100000` and `100001`. Checkpoint
read-backs and observed first messages agree. Each stage durably acknowledged
100,000 rows, with zero acknowledgement failures, wait timeouts or close failures.

The explicit replay read sequences `99980` through `99999` from each Azure
partition and appended them through the production mapping and Elastic adapter.
It left the primary consumer checkpoints at `99999`. The target gained 40 rows
while preserving exactly 200,000 distinct IDs.

A retained SQL view, `EVSNOW_ELASTIC_261005_1931.PUBLIC.EVENTS_BY_EVENT_ID`, selects
one row per run/event ID. Its independent read-back returned 200,000 rows,
IDs `0` through `199999`, and zero invalid deterministic values. This demonstrates
downstream reconciliation; Elastic itself does not deduplicate source replay.

The full proof ran from `2026-10-05T19:38:30Z` to `2026-10-05T19:43:44Z`, including
restart, query visibility polling and replay. The producer spent approximately
125 seconds per half at a configured ceiling of 800 events/second. These are
observations from this test, not a maximum-throughput benchmark.

## Environment and cost controls

| Component | Verified configuration |
|---|---|
| Azure subscription | `OpenAI demos` |
| Azure region | Sweden Central |
| Event Hubs namespace | `evsnow-elastic-20261005-1931` |
| Event Hubs SKU | Basic, one throughput unit |
| Hub / partitions / retention | `events`, two partitions, 24 hours |
| Consumer group | `$Default` |
| Producer batching | At most 240 KiB; 800 events/second ceiling |
| Source authentication | Azure CLI identity scoped to the disposable hub |
| Checkpoint storage | Existing Snowflake control table; local single-consumer ownership |
| Snowflake account / region | `VAYNIMM-KP67615` / `AZURE_SWEDENCENTRAL` |
| Target | Native table `EVSNOW_ELASTIC_261005_1931.PUBLIC.EVENTS` |
| Streaming pipe | `EVSNOW_ELASTIC_261005_1931.PUBLIC.EVENTS_PIPE`, implicit Elastic Channel |
| Runtime authentication | Dedicated key-pair service user and scoped role |
| Query/checkpoint warehouse | X-Small, 60-second auto-suspend, query acceleration disabled |
| SDKs | `snowpipe-streaming` 1.8.1; `azure-eventhub` 5.15.1 |

Basic is the lowest Event Hubs tier and supports this test's default consumer
group and small messages. The Azure Retail Prices API returned USD 0.015 per
Basic throughput-unit hour and USD 0.028 per million ingress events in Sweden
Central. The 200,000 small source events represent USD 0.0056 of ingress at that
retail rate, plus billed throughput-unit hours. This is a rate calculation, not
an invoice; Snowflake ingestion and warehouse charges are separate.
[Azure tier limits](https://learn.microsoft.com/en-us/azure/event-hubs/compare-tiers),
[Azure pricing API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices).

The live JSON records package metadata `0.2.1` because the user requested the
version bump after ingestion began. The tested Elastic code is included in the
prepared `0.3.0` package; its package, installed metadata and CLI versions agree,
and the full local suite was rerun after the metadata/CLI change.

## Cleanup and retained evidence

The cleanup handler checked the unique run tag before deleting
`rg-evsnow-elastic-20261005-1931`. `az group exists` returned `false`, including a
second independent read-back. Its Event Hubs namespace, hub and scoped role
assignment were contained in that group. No resources from earlier runs were
included in deletion.

The isolated Snowflake warehouse read back `SUSPENDED`, and the test service user
read back `DISABLED=true`. The result database, raw table, control checkpoints
and deduplicated view remain available for review. Private keys and runtime
credentials remain in the ignored `.local/` directory.

The sanitized evidence directory is `docs/development/evidence/elastic-20261005/`:

- `live-test.json`: every stage's counts, acknowledgements, checkpoints, first
  messages, replay and final acceptance result.
- `azure-group.json`, `azure-namespace.json`, `azure-eventhub.json`: resource scope,
  Basic SKU, capacity, partitions and retention.
- `azure-retail-prices.json`: primary-source rate read-back.
- `verification.sql` and `snowflake-final-verification.json`: independent raw,
  deduplicated, processing-error and checkpoint queries.
- `azure-group-exists-after.txt`, `cleanup-final.json`, `snowflake-cleanup.json`:
  deletion, suspension and disabled-user read-backs.
- `local-validation.json` and `peer-review.md`: tests, static checks and review.

## Reproduce

Use [the Elastic how-to](../how-to/use-elastic-channels.md) to create fresh,
disposable resources and prepare an isolated target/control-table configuration.
Enable table error logging and grant the verifying role `SELECT ERROR TABLE`.
Run `tools/elastic_live_test.py` with a fresh run ID and the new config/env paths.
The harness operates the configured resources; arrange guarded Azure deletion
on both success and failure. Its test requires an empty target/control table and
a new two-partition Event Hub.

Local checks passed: **501 tests**, Ruff lint and formatting, ty, and the strict
Zensical documentation build. The 35 new Elastic tests include original-Future
reuse, late acknowledgement, partial partition success and checkpoint-write
failure. [Release notes](../release-notes/0.3.0.md) explain how to opt in and what
delivery guarantees apply.
