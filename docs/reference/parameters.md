# Parameter Reference

This page lists the configuration inputs EvSnow accepts through structured TOML
and `.env` or process environment variables.

Prefer TOML for pipeline shape. Prefer `.env` or deployment secrets for
credentials, tokens, passwords, and machine-specific paths.

EvSnow loads values in this order:

```text
model defaults < TOML < default .env < process environment < explicit --env-file < CLI runtime flags
```

The default `.env` file is loaded when the CLI starts, but it does not replace
variables already present in the shell. An explicit `--env-file` is loaded with
override semantics, so it can replace shell values and TOML-derived values for
that command. Prefer keeping pipeline shape in TOML and using `.env` for
secrets, local paths, and connection credentials.

Find the right section:

- Pipeline shape: [Top-Level Pipeline Settings](#top-level-pipeline-settings)
- Checkpoints and ownership: [Control Table Settings](#control-table-settings)
- Event Hub sources: [Event Hub Settings](#event-hub-settings)
- Snowflake auth and targets: [Snowflake Connection Settings](#snowflake-connection-settings)
  and [Snowflake Target Settings](#snowflake-target-settings)
- Observability and retry behavior: [Logfire Observability](#logfire-observability)
  and [Smart Retry Settings](#smart-retry-settings)

The tables use these conventions:

- `{N}` is a numeric mapping suffix such as `1`, `2`, or `3`.
- `<KEY>` is a TOML table key such as `EVENTHUBNAME_1` or `SNOWFLAKE_1`.
- `not supported` means that source is not part of EvSnow's public
  configuration surface for that setting.
- Blank defaults mean the value is required for that source or conditional on a
  selected backend/mode.

## Top-Level Pipeline Settings

| Setting | TOML key | `.env` variable | Default / allowed values | Notes |
|---------|----------|-----------------|--------------------------|-------|
| Event Hubs namespace | `eventhub_namespace` | `EVENTHUB_NAMESPACE` | required; must end with `.servicebus.windows.net` | Shared namespace for Event Hub definitions. |
| Environment label | `environment` | `ENVIRONMENT` | `development` | Used in generated Snowflake channel names. |
| Region label | `region` | `REGION` | `default` | Used in generated Snowflake channel names. |
| Client identifier | `client_id` | `EVSNOW_CLIENT_ID` | local hostname | Normalized to letters, numbers, `_`, and `-`. |
| Maximum concurrent channels | `max_concurrent_channels` | `MAX_CONCURRENT_CHANNELS` | `50` | Pipeline concurrency guard. |
| Ingestion timeout | `ingestion_timeout_seconds` | `INGESTION_TIMEOUT_SECONDS` | `300` | Seconds. |
| Maximum concurrent mappings | `max_concurrent_mappings` | `MAX_CONCURRENT_MAPPINGS` | `10` | Mapping processors. |
| Health-check interval | `health_check_interval_seconds` | `HEALTH_CHECK_INTERVAL_SECONDS` | `60` | Seconds. |
| Pipeline restart attempts | `max_pipeline_restart_attempts` | `MAX_PIPELINE_RESTART_ATTEMPTS` | `3` | Restart attempts after failure. |
| Pipeline restart delay | `pipeline_restart_delay_seconds` | `PIPELINE_RESTART_DELAY_SECONDS` | `30` | Seconds between restart attempts. |
| Detailed logging | `enable_detailed_logging` | `ENABLE_DETAILED_LOGGING` | `false` | Boolean. |
| Log message samples | `log_message_samples` | `LOG_MESSAGE_SAMPLES` | `false` | Boolean. |
| Metrics collection | `metrics_collection_enabled` | `METRICS_COLLECTION_ENABLED` | `true` | Boolean. |

## Control Table Settings

| Setting | TOML key | `.env` variable | Default / allowed values | Notes |
|---------|----------|-----------------|--------------------------|-------|
| Control database | `control.target_db` | `TARGET_DB` | `CONTROL` | Normalized to lowercase when backend is Postgres unless quoted in TOML. |
| Control schema | `control.target_schema` | `TARGET_SCHEMA` | `PUBLIC` | Normalized to lowercase when backend is Postgres unless quoted in TOML. |
| Control table | `control.target_table` | `TARGET_TABLE` | `INGESTION_STATUS` | Normalized to lowercase when backend is Postgres unless quoted in TOML. |
| Control backend | `control.backend` | `CONTROL_TABLE_BACKEND` | `snowflake`; `snowflake`, `postgres` | Selects checkpoint/control storage backend. |
| Ownership mode | `control.ownership_mode` | `CONTROL_OWNERSHIP_MODE` | `durable`; `durable`, `local_single_consumer_smoke` | `local_single_consumer_smoke` is valid only with `snowflake` backend. |
| Hybrid control table | `control.use_hybrid_table` | `USE_HYBRID_TABLE` | `false` | Boolean; requires Snowflake Hybrid Table support. |

## Postgres Control Backend

Use these only when `control.backend` / `CONTROL_TABLE_BACKEND` is `postgres`.

| Setting | TOML key | `.env` variable | Default / allowed values | Notes |
|---------|----------|-----------------|--------------------------|-------|
| Host | `control.postgres.host` | `CONTROL_PG_HOST` | required | Required for Postgres backend. |
| Port | `control.postgres.port` | `CONTROL_PG_PORT` | `5432`; `1`-`65535` | Integer TCP port. |
| User | `control.postgres.user` | `CONTROL_PG_USER` | required | Required for Postgres backend. |
| Password | `control.postgres.password` | `CONTROL_PG_PASSWORD` | required when `password` auth | Keep in `.env` or a secret store. Ignored for `azure_token` auth. |
| SSL mode | `control.postgres.sslmode` | `CONTROL_PG_SSLMODE` | `require` | Any non-empty libpq SSL mode value, for example `require` or `verify-full`. |
| Auth mode | `control.postgres.auth_mode` | `CONTROL_PG_AUTH_MODE` | `password`; `password`, `azure_token` | `azure_token` uses `DefaultAzureCredential`. |

## Event Hub Settings

Use one `[event_hubs.<KEY>]` TOML table or one `EVENTHUBNAME_{N}` family per
Event Hub. Per-hub settings override shared defaults.

| Setting | TOML key | `.env` variable | Default / allowed values | Notes |
|---------|----------|-----------------|--------------------------|-------|
| Event Hub name | `event_hubs.<KEY>.name` | `EVENTHUBNAME_{N}` | required | Event Hub entity name. |
| Namespace | `event_hubs.<KEY>.namespace` | use `EVENTHUB_NAMESPACE` | required in TOML table | Keep this aligned with top-level `eventhub_namespace`. |
| Consumer group | `event_hubs.<KEY>.consumer_group` | `EVENTHUBNAME_{N}_CONSUMER_GROUP` | required | Usually `$Default` for local smoke tests. |
| Connection string | `event_hubs.<KEY>.connection_string` | `EVENTHUBNAME_{N}_CONNECTION_STRING` | unset | Secret; use only when bypassing Entra ID auth. |
| Maximum SDK batch size | `event_hubs.<KEY>.max_batch_size` | `EVENTHUBNAME_{N}_MAX_BATCH_SIZE` | `1000`; `> 0` | Azure Event Hubs receive batch size. |
| Maximum wait time | `event_hubs.<KEY>.max_wait_time` | `EVENTHUBNAME_{N}_MAX_WAIT_TIME`; global `EVENTHUB_MAX_WAIT_TIME` | `60`; `>= 0` | Seconds; `0` lets the SDK wait until an event is received. |
| Prefetch count | `event_hubs.<KEY>.prefetch_count` | `EVENTHUBNAME_{N}_PREFETCH_COUNT`; global `EVENTHUB_PREFETCH_COUNT` | `300`; `> 0` | Azure Event Hubs prefetch count. |
| Retry total | `event_hubs.<KEY>.retry_total` | `EVENTHUBNAME_{N}_RETRY_TOTAL`; global `EVENTHUB_RETRY_TOTAL` | `3`; `> 0` | SDK retry attempts. |
| Retry backoff factor | `event_hubs.<KEY>.retry_backoff_factor` | `EVENTHUBNAME_{N}_RETRY_BACKOFF_FACTOR`; global `EVENTHUB_RETRY_BACKOFF_FACTOR` | `0.8`; `> 0` | SDK retry backoff factor. |
| Retry backoff max | `event_hubs.<KEY>.retry_backoff_max` | `EVENTHUBNAME_{N}_RETRY_BACKOFF_MAX`; global `EVENTHUB_RETRY_BACKOFF_MAX` | `120`; `> 0` | Seconds. |
| Retry mode | `event_hubs.<KEY>.retry_mode` | `EVENTHUBNAME_{N}_RETRY_MODE`; global `EVENTHUB_RETRY_MODE` | `exponential`; `exponential`, `fixed` | Case-normalized. |
| Load-balancing interval | `event_hubs.<KEY>.load_balancing_interval` | `EVENTHUBNAME_{N}_LOAD_BALANCING_INTERVAL`; global `EVENTHUB_LOAD_BALANCING_INTERVAL` | `30`; `> 0` | Seconds. |
| Ownership expiration interval | `event_hubs.<KEY>.partition_ownership_expiration_interval` | `EVENTHUBNAME_{N}_PARTITION_OWNERSHIP_EXPIRATION_INTERVAL`; global `EVENTHUB_PARTITION_OWNERSHIP_EXPIRATION_INTERVAL` | unset; `> 0` and at least `6 * load_balancing_interval` | Leave unset for SDK default unless tuning competing consumers. |
| Load-balancing strategy | `event_hubs.<KEY>.load_balancing_strategy` | `EVENTHUBNAME_{N}_LOAD_BALANCING_STRATEGY`; global `EVENTHUB_LOAD_BALANCING_STRATEGY` | `greedy`; `greedy`, `balanced` | Case-normalized. |
| Track last-enqueued properties | `event_hubs.<KEY>.track_last_enqueued_event_properties` | `EVENTHUBNAME_{N}_TRACK_LAST_ENQUEUED_EVENT_PROPERTIES`; global `EVENTHUB_TRACK_LAST_ENQUEUED_EVENT_PROPERTIES` | `false` | Boolean. |
| Credential mode | `event_hubs.<KEY>.credential_mode` | `EVENTHUBNAME_{N}_CREDENTIAL_MODE`; global `EVENTHUB_CREDENTIAL_MODE` | `default`; `default`, `azure_cli` | `default` uses production-capable `DefaultAzureCredential`. |
| Managed identity client ID | `event_hubs.<KEY>.managed_identity_client_id` | `EVENTHUBNAME_{N}_MANAGED_IDENTITY_CLIENT_ID`; global `EVENTHUB_MANAGED_IDENTITY_CLIENT_ID` | unset | User-assigned managed identity client ID. |
| Connection-string compatibility flag | `event_hubs.<KEY>.use_connection_string` | `EVENTHUBNAME_{N}_USE_CONNECTION_STRING` | `false` | Compatibility setting accepted by the config parser. Runtime auth follows whether a connection string is set. |
| Checkpoint interval | `event_hubs.<KEY>.checkpoint_interval_seconds` | `EVENTHUBNAME_{N}_CHECKPOINT_INTERVAL_SECONDS`; global `EVENTHUB_CHECKPOINT_INTERVAL_SECONDS` | `300`; `> 0` | Seconds. |
| Maximum message batch size | `event_hubs.<KEY>.max_message_batch_size` | `EVENTHUBNAME_{N}_MAX_MESSAGE_BATCH_SIZE`; global `EVENTHUB_MAX_MESSAGE_BATCH_SIZE` | `1000`; `> 0` | Processing batch size. |
| Batch timeout | `event_hubs.<KEY>.batch_timeout_seconds` | `EVENTHUBNAME_{N}_BATCH_TIMEOUT_SECONDS`; global `EVENTHUB_BATCH_TIMEOUT_SECONDS` | `300`; `> 0` | Seconds. |
| Starting position without checkpoint | `event_hubs.<KEY>.starting_position_on_no_checkpoint` | `EVENTHUBNAME_{N}_STARTING_POSITION_ON_NO_CHECKPOINT` | `-1`; `-1`, `@latest` | `-1` starts from the beginning; `@latest` reads only new events. |

## Shared Event Hub Defaults

Use `[eventhub_defaults]` in TOML or the global `EVENTHUB_*` variables when the
same SDK tuning applies to every Event Hub. Per-hub TOML keys and
`EVENTHUBNAME_{N}_*` variables override these defaults.

| Setting | TOML key | `.env` variable | Default / allowed values | Notes |
|---------|----------|-----------------|--------------------------|-------|
| Connection string | `eventhub_defaults.connection_string` | `AZURE_EVENTHUB_CONNECTION_STRING` | unset | Secret; applies to hubs without a per-hub connection string. |
| Maximum wait time | `eventhub_defaults.max_wait_time` | `EVENTHUB_MAX_WAIT_TIME` | unset; `>= 0` | Falls back to per-model default `60` when unset. |
| Prefetch count | `eventhub_defaults.prefetch_count` | `EVENTHUB_PREFETCH_COUNT` | unset; `> 0` | Falls back to per-model default `300` when unset. |
| Retry total | `eventhub_defaults.retry_total` | `EVENTHUB_RETRY_TOTAL` | unset; `> 0` | Falls back to per-model default `3` when unset. |
| Retry backoff factor | `eventhub_defaults.retry_backoff_factor` | `EVENTHUB_RETRY_BACKOFF_FACTOR` | unset; `> 0` | Falls back to per-model default `0.8` when unset. |
| Retry backoff max | `eventhub_defaults.retry_backoff_max` | `EVENTHUB_RETRY_BACKOFF_MAX` | unset; `> 0` | Falls back to per-model default `120` when unset. |
| Retry mode | `eventhub_defaults.retry_mode` | `EVENTHUB_RETRY_MODE` | unset; `exponential`, `fixed` | Falls back to per-model default `exponential` when unset. |
| Load-balancing interval | `eventhub_defaults.load_balancing_interval` | `EVENTHUB_LOAD_BALANCING_INTERVAL` | unset; `> 0` | Falls back to per-model default `30` when unset. |
| Ownership expiration interval | `eventhub_defaults.partition_ownership_expiration_interval` | `EVENTHUB_PARTITION_OWNERSHIP_EXPIRATION_INTERVAL` | unset; `> 0` and at least `6 * load_balancing_interval` | Leave unset for SDK default unless tuning competing consumers. |
| Load-balancing strategy | `eventhub_defaults.load_balancing_strategy` | `EVENTHUB_LOAD_BALANCING_STRATEGY` | unset; `greedy`, `balanced` | Falls back to per-model default `greedy` when unset. |
| Track last-enqueued properties | `eventhub_defaults.track_last_enqueued_event_properties` | `EVENTHUB_TRACK_LAST_ENQUEUED_EVENT_PROPERTIES` | unset; boolean | Falls back to per-model default `false` when unset. |
| Credential mode | `eventhub_defaults.credential_mode` | `EVENTHUB_CREDENTIAL_MODE` | unset; `default`, `azure_cli` | Falls back to per-model default `default` when unset. |
| Managed identity client ID | `eventhub_defaults.managed_identity_client_id` | `EVENTHUB_MANAGED_IDENTITY_CLIENT_ID` | unset | User-assigned managed identity client ID. |
| Connection-string compatibility flag | `eventhub_defaults.use_connection_string` | no global variable | unset; boolean | Compatibility setting accepted by TOML. Runtime auth follows whether a connection string is set. |
| Checkpoint interval | `eventhub_defaults.checkpoint_interval_seconds` | `EVENTHUB_CHECKPOINT_INTERVAL_SECONDS` | unset; `> 0` | Falls back to per-model default `300` when unset. |
| Maximum message batch size | `eventhub_defaults.max_message_batch_size` | `EVENTHUB_MAX_MESSAGE_BATCH_SIZE` | unset; `> 0` | Falls back to per-model default `1000` when unset. |
| Batch timeout | `eventhub_defaults.batch_timeout_seconds` | `EVENTHUB_BATCH_TIMEOUT_SECONDS` | unset; `> 0` | Falls back to per-model default `300` when unset. |
| Starting position without checkpoint | `eventhub_defaults.starting_position_on_no_checkpoint` | no global variable | unset; `-1`, `@latest` | Falls back to per-model default `-1` when unset. |

## Snowflake Connection Settings

These settings define the shared Snowflake session and Snowpipe Streaming pipe.
Keep private-key paths and passwords in `.env` or a secret store. Do not commit
TOML files that contain private key paths or passwords.

When Snowflake connection values come from `.env`, EvSnow can derive
`SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA_NAME` from the mapped target when
there is exactly one target database/schema pair. If you use a literal
`[snowflake_connection]` TOML table, include `database` and `schema_name`
because that table is validated as a complete Snowflake connection model.

| Setting | TOML key | `.env` variable | Default / allowed values | Notes |
|---------|----------|-----------------|--------------------------|-------|
| Account | `snowflake_connection.account` | `SNOWFLAKE_ACCOUNT` | required | Account identifier; letters, numbers, `.`, `_`, and `-`. |
| User | `snowflake_connection.user` | `SNOWFLAKE_USER` | required | Non-empty Snowflake user. |
| Private key file | `snowflake_connection.private_key_file` | `SNOWFLAKE_PRIVATE_KEY_FILE` | required | Must point to a readable file. |
| Private key password | `snowflake_connection.private_key_password` | `SNOWFLAKE_PRIVATE_KEY_PASSWORD` | unset | Required only for encrypted private keys. |
| Warehouse | `snowflake_connection.warehouse` | `SNOWFLAKE_WAREHOUSE` | required | Non-empty Snowflake identifier. |
| Session database | `snowflake_connection.database` | `SNOWFLAKE_DATABASE` | derived when one mapped target has one database/schema | Set explicitly when mappings span multiple database/schema pairs. |
| Session schema | `snowflake_connection.schema_name` | `SNOWFLAKE_SCHEMA_NAME` | derived when one mapped target has one database/schema | `.env` uses `SCHEMA_NAME`, not `SCHEMA`. |
| Role | `snowflake_connection.role` | `SNOWFLAKE_ROLE` | unset | Optional runtime role. |
| Pipe name | `snowflake_connection.pipe_name` | `SNOWFLAKE_PIPE_NAME` | required | Snowpipe Streaming pipe object name. |

## Snowflake Target Settings

Use one `[snowflake_configs.<KEY>]` TOML table or one `SNOWFLAKE_{N}` `.env`
family per target table.

| Setting | TOML key | `.env` variable | Default / allowed values | Notes |
|---------|----------|-----------------|--------------------------|-------|
| Target database | `snowflake_configs.<KEY>.database` | `SNOWFLAKE_{N}_DATABASE` | required | Snowflake identifier. |
| Target schema | `snowflake_configs.<KEY>.schema_name` | `SNOWFLAKE_{N}_SCHEMA` | required | `.env` target key uses `SCHEMA`, unlike connection `SNOWFLAKE_SCHEMA_NAME`. |
| Target table | `snowflake_configs.<KEY>.table_name` | `SNOWFLAKE_{N}_TABLE` | required | Snowflake identifier. |
| Batch size | `snowflake_configs.<KEY>.batch_size` | `SNOWFLAKE_{N}_BATCH` | `1000` | Records per batch. |
| Retry attempts | `snowflake_configs.<KEY>.max_retry_attempts` | `SNOWFLAKE_{N}_MAX_RETRY_ATTEMPTS` | `3` | Integer. |
| Retry delay | `snowflake_configs.<KEY>.retry_delay_seconds` | `SNOWFLAKE_{N}_RETRY_DELAY_SECONDS` | `5` | Seconds. |
| Connection timeout | `snowflake_configs.<KEY>.connection_timeout_seconds` | `SNOWFLAKE_{N}_CONNECTION_TIMEOUT_SECONDS` | `30` | Seconds. |
| Channel status interval | `snowflake_configs.<KEY>.channel_status_interval_seconds` | `SNOWFLAKE_{N}_CHANNEL_STATUS_INTERVAL_SECONDS` | `60`; `0` disables | Seconds between channel status checks. |
| Client refresh interval | `snowflake_configs.<KEY>.client_refresh_interval_seconds` | `SNOWFLAKE_{N}_CLIENT_REFRESH_INTERVAL_SECONDS` | `0`; `0` disables | Seconds between proactive client recreation. |

## Event Hub To Snowflake Mappings

TOML supports explicit mappings. `.env` uses automatic numeric mappings:
`EVENTHUBNAME_1` maps to `SNOWFLAKE_1`, `EVENTHUBNAME_2` maps to
`SNOWFLAKE_2`, and so on.

| Setting | TOML key | `.env` variable | Default / allowed values | Notes |
|---------|----------|-----------------|--------------------------|-------|
| Event Hub key | `mappings[].event_hub_key` | automatic by `{N}` | required in TOML | Must reference a defined `EVENTHUBNAME_{N}` key. |
| Snowflake key | `mappings[].snowflake_key` | automatic by `{N}` | required in TOML | Must reference a defined `SNOWFLAKE_{N}` key. |
| Channel name pattern | `mappings[].channel_name_pattern` | automatic default | `{event_hub}-{env}-{region}-{client_id}` | Allowed placeholders are `{event_hub}`, `{env}`, `{region}`, and `{client_id}`. |

## Logfire Observability

Logfire can be configured in TOML or `.env`. Keep tokens in `.env` or a secret
manager.

| Setting | TOML key | `.env` variable | Default / allowed values | Notes |
|---------|----------|-----------------|--------------------------|-------|
| Enabled | `logfire.enabled` | `LOGFIRE_ENABLED` | `false` | Boolean. |
| Token | `logfire.token` | `LOGFIRE_TOKEN` | unset | Required when enabled and sending to Logfire cloud. |
| Service name | `logfire.service_name` | `LOGFIRE_SERVICE_NAME` | `evsnow` | Service label in Logfire. |
| Environment | `logfire.environment` | `LOGFIRE_ENVIRONMENT` | `development` | Environment tag. |
| Send to Logfire | `logfire.send_to_logfire` | `LOGFIRE_SEND_TO_LOGFIRE` | `true` | Boolean. |
| Console logging | `logfire.console_logging` | `LOGFIRE_CONSOLE_LOGGING` | `true` | Boolean. |
| Log level | `logfire.log_level` | `LOGFIRE_LOG_LEVEL` | `INFO`; `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | Case-normalized to uppercase. |

## Smart Retry Settings

Smart retry settings are `.env` or process environment settings. They are read
only when the run command uses `--smart`.

| Setting | TOML key | `.env` variable | Default / allowed values | Notes |
|---------|----------|-----------------|--------------------------|-------|
| Enabled flag | not supported | `SMART_RETRY_ENABLED` | `false` | Parsed by the settings model, but `uv run evsnow run --smart` is the runtime switch. |
| LLM provider | not supported | `SMART_RETRY_LLM_PROVIDER` | `openai`; `openai`, `azure`, `anthropic`, `gemini`, `groq`, `cohere` | Case-normalized. |
| LLM model | not supported | `SMART_RETRY_LLM_MODEL` | `gpt-4o-mini` | For Azure, use your deployment name. |
| API key | not supported | `SMART_RETRY_LLM_API_KEY` | required with `--smart` | Keep in `.env` or a secret store. |
| Endpoint | not supported | `SMART_RETRY_LLM_ENDPOINT` | unset | Required for Azure OpenAI-style custom endpoints. |
| Maximum attempts | not supported | `SMART_RETRY_MAX_ATTEMPTS` | `3`; `1`-`10` | Integer. |
| Timeout | not supported | `SMART_RETRY_TIMEOUT_SECONDS` | `10`; `1`-`60` | Seconds. |
| Enable caching | not supported | `SMART_RETRY_ENABLE_CACHING` | `true` | Boolean. |

## Runtime-Only CLI Flags

These are not persisted in TOML or `.env`.

| Command | Flag | Accepted values | Notes |
|---------|------|-----------------|-------|
| `validate-config`, `run`, `status` | `--config-file`, `-c` | path | Load a structured TOML config file. Defaults to `config/evsnow.toml` when present. |
| `validate-config`, `run`, `status` | `--env-file`, `-e` | path | Load an environment file with override semantics. |
| `validate-config` | `--show-rbac` | flag | Show Azure Event Hubs RBAC guidance after config validation. |
| `run` | `--dry-run` | flag | Validate and show the processing plan without starting the pipeline. |
| `run` | `--smart` | flag | Enable smart retry mode and read `SMART_RETRY_*` settings. |
| `run` | `--capture` | flag | Capture raw Event Hub messages to `messages/` for the run. |
| `monitor` | `--log-file`, `-l` | path | Accepted by the CLI, but the monitor UI is not implemented yet. |
| `check-credentials` | none | none | Checks available Azure credential types. |
| `version` | none | none | Prints EvSnow version and component summary. |
