# EvSnow changelog

## 0.3.0

EvSnow 0.3.0 adds optional Snowflake Elastic Channels while keeping Named
Channels as the default. Elastic ingestion waits for durable acknowledgements,
reuses pending acknowledgements after a timeout, and bounds Elastic adapter close waits.
Terminal batch and shutdown failures reach supervisors through CLI exit status
`1` after cleanup.
It also refreshes dependencies and adapts the optional AI integrations to their
current SDK APIs.

Read the [0.3.0 release notes](docs/release-notes/0.3.0.md) for configuration,
delivery behavior and upgrade steps.
