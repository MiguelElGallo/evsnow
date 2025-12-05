# evsnow

[![Tests](https://github.com/MiguelElGallo/evsnow/actions/workflows/tests.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/tests.yml)
[![CI/CD Pipeline](https://github.com/MiguelElGallo/evsnow/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/MiguelElGallo/evsnow/branch/main/graph/badge.svg)](https://codecov.io/gh/MiguelElGallo/evsnow)
Stream data from Azure Event Hubs to Snowflake in real-time with built-in checkpointing and observability.

![alt text](<media/ChatGPT Image Nov 9, 2025, 01_36_42 PM.png>)

See a [video](https://youtu.be/zX3K-rfNZIU) for a general overview.

## Prerequisites

- Python 3.13+ and `uv` installed
- Snowflake account/role with permission to create/use the target database, schema, and pipe
- Azure Event Hub namespace with read access (consumer group) and either `az login` or a connection string
- Ability to run OpenSSL to generate RSA keys for Snowflake key-pair auth
- `snowsql` (or Snowflake UI) to truncate tables/checkpoints when testing from scratch

## Install

```bash
# Clone the repository
git clone https://github.com/MiguelElGallo/evsnow.git
cd evsnow

# Install dependencies
uv sync

# Quickstart
uv run evsnow validate-config
uv run evsnow run --dry-run
```

## Configure

1) Copy and edit the environment file

```bash
cp .env.example .env
```

Then set your values in `.env`. The pipeline needs:

- **Azure Event Hub** (namespace, event hub name, consumer group, optional connection string)
- **Snowflake connection** (account, user, key pair, warehouse, database, schema, role)
- **Checkpoint/control table** for offsets
- **Topic → table mappings**

Key settings (example):

```bash
# Azure Event Hub
EVENTHUB_NAMESPACE=eventhu1.servicebus.windows.net
EVENTHUBNAME_1=topic1
EVENTHUBNAME_1_CONSUMER_GROUP=$Default

# Snowflake Connection
SNOWFLAKE_ACCOUNT=aaaaaa-bbbbbbb
SNOWFLAKE_USER=STREAMEV
SNOWFLAKE_PRIVATE_KEY_FILE=/path/to/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=compute_wh
SNOWFLAKE_DATABASE=INGESTION
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=STREAM

# Control Table (for checkpointing)
TARGET_DB=CONTROL
TARGET_SCHEMA=PUBLIC
TARGET_TABLE=INGESTION_STATUS

# Topic → Table Mapping
SNOWFLAKE_1_DATABASE=INGESTION
SNOWFLAKE_1_SCHEMA=PUBLIC
SNOWFLAKE_1_TABLE=events_table
SNOWFLAKE_1_BATCH=100
```

### Snowflake authentication

Generate RSA key pair for authentication:

```bash
# Generate keys
./generate_snowflake_keys.sh

# Assign public key to Snowflake user
# See SNOWFLAKE_QUICKSTART.md for detailed instructions
# Example: ALTER USER STREAMEV SET RSA_PUBLIC_KEY='MIIBIjANBgkqhki...';
```

### Azure authentication

The pipeline uses `DefaultAzureCredential`. Make sure you're logged in:

```bash
az login
```

Or provide a connection string in `.env`:

```bash
AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKey=..."
EVENTHUBNAME_1_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKey=..."
```

## Use

```bash
# Validate configuration
uv run evsnow validate-config

# Run the pipeline
uv run evsnow run

# Check status
uv run evsnow status

# Dry run (test without ingesting)
uv run evsnow run --dry-run
```

### Starting Fresh (No Checkpoints)

When starting the pipeline **without existing checkpoints** (e.g., after truncating checkpoint tables), you can control how the consumer processes existing messages:

```bash
# Example: Starting fresh after truncating tables
snowsql -q "truncate table ingestion.public.events_table;"
snowsql -q "truncate table control.public.ingestion_status;"

# Consumer will process based on STARTING_POSITION_ON_NO_CHECKPOINT setting
uv run evsnow run
```

**Starting Position Options** (in `.env`): choose where the consumer begins when **no checkpoints exist**. After checkpoints are saved, the setting is ignored (resume from checkpoint).

```bash
# Option 1: BEGINNING of stream (default, recommended)
# Processes ALL existing messages in the Event Hub partition
# Use to ensure no messages are lost when starting fresh
EVENTHUBNAME_1_STARTING_POSITION_ON_NO_CHECKPOINT=-1

# Option 2: LATEST position
# Only processes messages that arrive AFTER the consumer connects
# Skips messages already in Event Hub
EVENTHUBNAME_1_STARTING_POSITION_ON_NO_CHECKPOINT=@latest

# Option 3: EARLIEST retained message
# Similar to -1 but explicitly starts from the oldest retained message
# Depends on Event Hub retention (typically 1-7 days)
EVENTHUBNAME_1_STARTING_POSITION_ON_NO_CHECKPOINT=0
```

**How It Works:**

1. **First run (no checkpoints):** Uses `STARTING_POSITION_ON_NO_CHECKPOINT` (`-1/0` = all existing messages; `@latest` = only new messages).
2. **Subsequent runs (checkpoints exist):** Always resumes from the last saved checkpoint; the setting is ignored.
3. **After truncating checkpoints:** Same as first run again; uses the configured starting position.

**Official Azure Documentation:**
- [Event Hubs Event Position](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers)
- [Starting Position Options](https://learn.microsoft.com/en-us/python/api/azure-eventhub/azure.eventhub.eventhubconsumerclient#azure-eventhub-eventhubconsumerclient-receive)

**Recommendation:** 
- Use `-1` (default) for production to prevent message loss
- Use `@latest` for development/testing when you only want new messages
- Avoid `0` unless you specifically need the earliest retained message (usually same as `-1`)

## Optional features

### Smart Retry (LLM-Powered)

Use LLM analysis to classify errors and decide whether to retry:

```bash
# Add to .env
SMART_RETRY_ENABLED=true
SMART_RETRY_LLM_PROVIDER=azure
SMART_RETRY_LLM_MODEL=gpt-4o-mini
SMART_RETRY_LLM_API_KEY=your-key
SMART_RETRY_LLM_ENDPOINT=https://your-deployment.cognitiveservices.azure.com/...
SMART_RETRY_MAX_ATTEMPTS=3
SMART_RETRY_TIMEOUT_SECONDS=10
SMART_RETRY_ENABLE_CACHING=true
```

Run with `--smart` flag:

```bash
uv run evsnow run --smart
```

### Logfire Observability

Send structured traces/logs for observability:

```bash
# Add to .env
LOGFIRE_ENABLED=true
LOGFIRE_TOKEN=your_logfire_token
LOGFIRE_SERVICE_NAME=evsnow
LOGFIRE_ENVIRONMENT=production
LOGFIRE_SEND_TO_LOGFIRE=true
LOGFIRE_CONSOLE_LOGGING=true
LOGFIRE_LOG_LEVEL=INFO
```

Get your token at [logfire.pydantic.dev](https://logfire.pydantic.dev)

### Snowpipe Streaming Configuration

The pipeline uses Snowflake's high-performance Snowpipe Streaming SDK (requires PIPE object):

```bash
# Add to .env
SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE
SNOWFLAKE_SCHEMA_NAME=PUBLIC

# Create PIPE in Snowflake (see setup_snowpipe_streaming.sql)
```

## Configuration reference

See [`.env.example`](./.env.example) for all available configuration options with detailed comments.

## Docs

- [Snowflake Quick Start](./SNOWFLAKE_QUICKSTART.md) - Setup guide for Snowflake
- [Troubleshooting](./TROUBLESHOOTING.md) - Common issues and solutions

## License

See [LICENSE](./LICENSE) for details.
