# evsnow

[![Tests](https://github.com/MiguelElGallo/evsnow/actions/workflows/tests.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/tests.yml)
[![CI/CD Pipeline](https://github.com/MiguelElGallo/evsnow/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/MiguelElGallo/evsnow/branch/main/graph/badge.svg)](https://codecov.io/gh/MiguelElGallo/evsnow)

Stream data from Azure Event Hubs to Snowflake in real-time with built-in checkpointing and observability.

![alt text](<media/ChatGPT Image Nov 9, 2025, 01_36_42 PM.png>)


## Installation

```bash
# Clone the repository
git clone https://github.com/MiguelElGallo/evsnow.git
cd evsnow

# Install dependencies
uv sync
```

## Configuration

1. **Copy the example environment file:**

```bash
cp .env.example .env
```

2. **Edit `.env` with your credentials:**

The pipeline needs three things configured:
- **Azure Event Hub**: Your namespace and topic names
- **Snowflake**: Connection details with key-pair authentication
- **Table Mapping**: Which Event Hub topics go to which Snowflake tables

### Required Settings

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

### Snowflake Authentication Setup

Generate RSA key pair for authentication:

```bash
# Generate keys
./generate_snowflake_keys.sh

# Assign public key to Snowflake user
# See SNOWFLAKE_QUICKSTART.md for detailed instructions
```

### Azure Authentication

The pipeline uses `DefaultAzureCredential`. Make sure you're logged in:

```bash
az login
```

Or provide a connection string in `.env`:

```bash
AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKey=..."
EVENTHUBNAME_1_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKey=..."
```

## Usage

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

## Optional Features

### Smart Retry (LLM-Powered)

Enable intelligent retry decisions using LLM analysis:

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

Monitor your pipeline with real-time tracing:

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

## Configuration Reference

See [`.env.example`](./.env.example) for all available configuration options with detailed comments.

## Documentation

- [Snowflake Quick Start](./SNOWFLAKE_QUICKSTART.md) - Setup guide for Snowflake
- [Troubleshooting](./TROUBLESHOOTING.md) - Common issues and solutions

## License

See [LICENSE](./LICENSE) for details.
