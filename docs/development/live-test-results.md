# Elastic Channels live test

EvSnow 0.3.0 with refreshed dependencies passed a real Azure Event Hubs-to-Snowflake test on
5 October 2026. All 200,000 expected events arrived with correct contents and
source metadata. A persisted consumer restart introduced no duplicates. A
separate replay of 40 retained source events produced exactly 40 duplicate rows.
The disposable Azure resource group was deleted and its absence was verified.

This proof belongs to the fresh `MiguelElGallo/evsnow` checkout on branch
`mpz/elastic-channels`. The run ID is `elastic-20261005-2008`. It does not reuse
results from the earlier checkout.

## Retest after the PR review fixes

The corrected code also passed a fresh 200,000-event run, `elastic-fixes-20261005-2043`,
on Azure Event Hubs Basic with one throughput unit and an isolated Snowflake
X-Small warehouse. The first half ingested 100,000 events in 137.377 seconds;
the restarted consumer ingested the next 100,000 in 135.131 seconds, resuming at
sequence `50000` on both partitions. Both halves recorded zero acknowledgement
failures, wait timeouts, and close failures.

Before explicit replay, the target contained 200,000 raw rows and 200,000 distinct
IDs. Replaying sequences `99980` through `99999` on each partition produced
200,040 raw rows, 200,000 distinct IDs, and exactly 40 duplicate rows. Missing or
invalid IDs, payload errors, source metadata errors, and Snowflake processing
errors all remained zero. The run completed from `2026-10-05T20:58:34Z` through
`2026-10-05T21:03:41Z`.

[Full run evidence](evidence/elastic-fixes-20261005/live-test.json) and
[independent SQL read-back](evidence/elastic-fixes-20261005/snowflake-final-verification.json)
record these results. The source hashes match before and after both cloud runs.

The new disposable Azure group was deleted and its absence verified repeatedly.
The isolated Snowflake warehouse is suspended and the test service user is
disabled. Both raw tables and event-ID views remain available for review.
[Cleanup read-backs](evidence/elastic-fixes-20261005/cleanup-final.json) and
[local validation](evidence/elastic-fixes-20261005/local-validation.json) record
these checks.

A fresh four-event test passed on the corrected consumer, shutdown, and replay
paths. The two consumer stages saved sequence `0`, then resumed at sequence `1`
on both partitions. Before replay, Snowflake held four raw rows and four distinct
IDs. Replaying all four retained source events produced eight raw rows and four
distinct IDs, with zero missing events, invalid payloads, invalid source metadata,
or processing errors. The replay started after integer sequence `-1` on each
partition, confirming the small-count boundary against the real Azure service.

This run used the larger target batch size and therefore exercised the existing
300-second partial-batch timeout in each stage. The configured Event Hub batch
timeout is not forwarded by the mapping; it did not shorten that wait. Set the
target `batch_size` to `2` for a quick four-event smoke run.

The small run's mapping counters include a final drain that reused acknowledged
receipts, so they count processing attempts rather than distinct ingested rows.
The SDK and independent SQL checks confirm two ingested events per stage and no
duplicates before the deliberate replay.

[Four-event evidence](evidence/elastic-fixes-20261005/small-smoke/live-test.json),
[effective configuration](evidence/elastic-fixes-20261005/test-configuration.json),
and [source hashes](evidence/elastic-fixes-20261005/source-provenance.json) record
the tested inputs and code. Failure exit statuses are covered by deterministic
CLI regressions, including unresolved acknowledgements and signal-driven cleanup.

## Dependency-refresh run results

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
| Initial consumer | 100,000 | 0 | 49,999 | 135.098 s |
| Fresh consumer using persisted checkpoints | 100,000 | 50,000 | 99,999 | 135.109 s |

The second consumer's first event IDs were `100000` and `100001`. Checkpoint
read-backs and observed first messages agree. Each stage durably acknowledged
100,000 rows, with zero acknowledgement failures, wait timeouts or close failures.

The explicit replay read sequences `99980` through `99999` from each Azure
partition and appended them through the production mapping and Elastic adapter.
It left the primary consumer checkpoints at `99999`. The target gained 40 rows
while preserving exactly 200,000 distinct IDs.

A retained SQL view, `EVSNOW_ELASTIC_261005_2008.PUBLIC.EVENTS_BY_EVENT_ID`, selects
one row per run/event ID. Its independent read-back returned 200,000 rows,
IDs `0` through `199999`, and zero invalid deterministic values. This demonstrates
downstream reconciliation; Elastic itself does not deduplicate source replay.

The full proof ran from `2026-10-05T20:11:33Z` to `2026-10-05T20:16:31Z`, including
restart, query visibility polling and replay. The producer spent approximately
125 seconds per half at a configured ceiling of 800 events/second. These are
observations from this test, not a maximum-throughput benchmark.

## Environment and cost controls

| Component | Verified configuration |
|---|---|
| Azure subscription | `OpenAI demos` |
| Azure region | Sweden Central |
| Event Hubs namespace | `evsnow-elastic-20261005-2008` |
| Event Hubs SKU | Basic, one throughput unit |
| Hub / partitions / retention | `events`, two partitions, 24 hours |
| Consumer group | `$Default` |
| Producer batching | At most 240 KiB; 800 events/second ceiling |
| Source authentication | Azure CLI identity scoped to the disposable hub |
| Checkpoint storage | Existing Snowflake control table; local single-consumer ownership |
| Snowflake account / region | `VAYNIMM-KP67615` / `AZURE_SWEDENCENTRAL` |
| Target | Native table `EVSNOW_ELASTIC_261005_2008.PUBLIC.EVENTS` |
| Streaming pipe | `EVSNOW_ELASTIC_261005_2008.PUBLIC.EVENTS_PIPE`, implicit Elastic Channel |
| Runtime authentication | Dedicated key-pair service user and scoped role |
| Query/checkpoint warehouse | X-Small, 60-second auto-suspend, query acceleration disabled |
| Runtime | EvSnow 0.3.0; Python 3.13.16 |
| SDKs | `snowpipe-streaming` 1.8.1; `azure-eventhub` 5.15.1; connector 4.8.0 |
| Other refreshed libraries | Cryptography 50.0.2; Logfire 5.1.1; Pydantic AI 2.54.0; aiohttp 3.14.4; Typer 0.27.2 |

Basic is the lowest Event Hubs tier and supports this test's default consumer
group and small messages. The Azure Retail Prices API returned USD 0.015 per
Basic throughput-unit hour and USD 0.028 per million ingress events in Sweden
Central. The 200,000 small source events represent USD 0.0056 of ingress at that
retail rate, plus billed throughput-unit hours. This is a rate calculation, not
an invoice; Snowflake ingestion and warehouse charges are separate.
[Azure tier limits](https://learn.microsoft.com/en-us/azure/event-hubs/compare-tiers),
[Azure pricing API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices).

This retest uses the final dependency lock and records package metadata `0.3.0`.
The original implementation proof, run `elastic-20261005-1931`, remains in
`docs/development/evidence/elastic-20261005/`. That earlier run recorded version
`0.2.1` before the requested version bump and dependency refresh. Its evidence is
historical; the measurements above come from the fresh dependency retest.

## Cleanup and retained evidence

The cleanup handler checked the unique run tag before deleting
`rg-evsnow-elastic-20261005-2008`. `az group exists` returned `false`, including a
second independent read-back. Its Event Hubs namespace, hub and scoped role
assignment were contained in that group. No resources from earlier runs were
included in deletion.

The isolated Snowflake warehouse read back `SUSPENDED`, and the test service user
read back `DISABLED=true`. The result database, raw table, control checkpoints
and deduplicated view remain available for review. Private keys and runtime
credentials remain in the ignored `.local/` directory.

The dependency-refresh evidence directory is
`docs/development/evidence/elastic-deps-20261005/`:

- `live-test.json`: every stage's counts, acknowledgements, checkpoints, first
  messages, replay and final acceptance result.
- `azure-group.json`, `azure-namespace.json`, `azure-eventhub.json`: resource scope,
  Basic SKU, capacity, partitions and retention.
- `azure-retail-prices.json`: primary-source rate read-back.
- `verification.sql` and `snowflake-final-verification.json`: independent raw,
  deduplicated, processing-error and checkpoint queries.
- `azure-group-exists-after.txt`, `cleanup-final.json`, `snowflake-cleanup.json`:
  deletion, suspension and disabled-user read-backs.
- `dependency-update.json`: direct library upgrades and upstream constraints.
- `local-validation.json` and `peer-review.md`: tests, static checks and review.

## Reproduce

Use [the Elastic how-to](../how-to/use-elastic-channels.md) to create fresh,
disposable resources and prepare an isolated target/control-table configuration.
Enable table error logging and grant the verifying role `SELECT ERROR TABLE`.
Run `tools/elastic_live_test.py` with a fresh run ID and the new config/env paths.
The harness operates the configured resources; arrange guarded Azure deletion
on both success and failure. Its test requires an empty target/control table and
a new two-partition Event Hub.

Local checks passed after the PR review fixes: **530 tests**, Ruff lint and formatting, ty, and the strict
Zensical documentation build. The 57 focused Elastic tests include original-Future reuse, late acknowledgement,
partial partition success, checkpoint-write failure, CLI failure status, concurrent
shutdown, and replay boundaries from four through 44 events. Seven additional offline SDK tests exercise AI providers and Logfire;
16 helper tests check the separate Copilot integration. An installed wheel was
checked outside the source checkout. [Release notes](../release-notes/0.3.0.md) explain how to opt in and what
delivery guarantees apply.
