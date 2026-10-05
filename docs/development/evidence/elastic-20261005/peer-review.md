# Peer review record

An independent review approved `plan.md` before core implementation. It required
retaining original append Futures on wait timeouts, bounded adapter shutdown,
explicit at-least-once replay semantics, Basic-tier request/rate limits, and
verification of target rows and processing errors independently of acknowledgements.
Those amendments were incorporated.

The implementation review checked the dedicated adapter, configuration, source
checkpoint contract, live harness and documentation. Resolved findings:

- Normalize Snowflake `COUNT_IF` NULL results to zero in acceptance queries.
- Check exact partition and sequence metadata, including first messages after restart.
- Preserve adapter and consumer statistics after shutdown for evidence.
- Bound producer, connector network and SQL statement waits.
- Add production-boundary tests for partial partition acknowledgement, late
  acknowledgement, and checkpoint-write failure after acknowledgement.
- Raise on unresolved/failed adapter close instead of reporting successful flush.
- Distinguish adapter close from consumer final-drain checkpoint writes in docs.
- Document `SELECT ERROR TABLE` privileges confirmed by the live account.

The final local suite passed 501 tests, including 35 new Elastic tests. Ruff,
formatting, ty and a strict Zensical build passed. Live evidence is recorded
separately in `live-test.json` and the Azure/Snowflake cleanup read-backs.

The final release/evidence review confirmed the 200,000-event coverage, persisted
restart, 40-row replay, deduplicated SQL view and scoped cleanup. It clarified
that incomplete shutdown leaves unacknowledged batches uncheckpointed; a close
error does not imply that an earlier successful checkpoint was rolled back.
The post-bump package/runtime/CLI proof and refreshed validation timestamp are
recorded in `local-validation.json`.
