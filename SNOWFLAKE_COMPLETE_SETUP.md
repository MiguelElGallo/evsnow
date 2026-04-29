# ❄️ Complete Snowflake + Iceberg + DuckDB Setup Guide

This guide walks you through the **complete setup** for EvSnow — from generating keys to streaming Event Hub data into Snowflake Iceberg tables and querying with DuckDB.

> 🎯 **Goal**: Set up a high-performance streaming pipeline from Azure Event Hub to Snowflake Iceberg tables.

---

## 📋 Table of Contents

1. [Prerequisites](#-prerequisites)
2. [Architecture Overview](#-architecture-overview)
3. [Generate RSA Keys](#-step-1-generate-rsa-keys)
4. [Initial Snowflake Setup](#-step-2-initial-snowflake-setup)
5. [Create External Volume](#-step-3-create-external-volume)
6. [Create Iceberg Table & Pipe](#-step-4-create-iceberg-table--pipe)
7. [Configure Environment](#-step-5-configure-environment)
8. [Run the Pipeline](#-step-6-run-the-pipeline)
9. [Query with DuckDB (Optional)](#-step-7-query-with-duckdb-optional)
10. [Object Reference](#-object-reference)
11. [Troubleshooting](#-troubleshooting)

---

## 📦 Prerequisites

Before starting, make sure you have:

| Requirement | Description |
|-------------|-------------|
| ✅ Azure CLI | Authenticated with `az login` |
| ✅ Python 3.13+ | With `uv` package manager |
| ✅ Snowflake Account | With ACCOUNTADMIN access (for initial setup) |
| ✅ Azure Event Hub | Namespace and topic configured |
| ✅ Azure Storage Account | For Iceberg external volume |
| ✅ DuckDB (optional) | For direct Iceberg queries |

---

## 🏗️ Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Azure Event    │     │    EvSnow       │     │   Snowflake     │
│     Hub         │────▶│   Pipeline      │────▶│  Iceberg Table  │
│   (topic1k)     │     │                 │     │ (EVENTS_TABLE1) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
                                                ┌─────────────────┐
                                                │  Azure Blob     │
                                                │  Storage        │
                                                │  (External Vol) │
                                                └─────────────────┘
                                                        │
                                                        ▼
                                                ┌─────────────────┐
                                                │    DuckDB       │
                                                │  (Query Layer)  │
                                                └─────────────────┘
```

---

## 🔑 Step 1: Generate RSA Keys

> **Where**: 🐚 Shell

Snowflake uses RSA key-pair authentication for secure, passwordless connections.

### Run the key generator script

```bash
./generate_snowflake_keys.sh
```

This creates two files in the `snowflake/` directory:

| File | Description | Environment Variable |
|------|-------------|---------------------|
| `rsa_key_encrypted.p8` | Private key (encrypted) | `SNOWFLAKE_PRIVATE_KEY_FILE` |
| `rsa_key_pub_value.txt` | Public key (for Snowflake) | — |

> 💡 **Tip**: Remember the password you set! You'll need it for `SNOWFLAKE_PRIVATE_KEY_PASSWORD` in your `.env` file.

### Verify the keys were created

```bash
ls -la snowflake/
# Should show: rsa_key_encrypted.p8, rsa_key_pub_value.txt
```

---

## ❄️ Step 2: Initial Snowflake Setup

> **Where**: ❄️ Snowflake SQL Worksheet

This step creates the user, role, databases, and control table.

### 2.1 Create the Role and User

```sql
-- Create the STREAM role for pipeline operations
CREATE ROLE IF NOT EXISTS STREAM;

-- Create the STREAMEV user
CREATE USER IF NOT EXISTS STREAMEV
    LOGIN_NAME = 'STREAMEV'
    DEFAULT_ROLE = 'STREAM'
    DEFAULT_WAREHOUSE = 'COMPUTE_WH';

-- Grant role to user
GRANT ROLE STREAM TO USER STREAMEV;
```

| Object | Name | Maps to `.env.example` |
|--------|------|------------------------|
| User | `STREAMEV` | `SNOWFLAKE_USER` |
| Role | `STREAM` | `SNOWFLAKE_ROLE` |

### 2.2 Assign RSA Public Key to User

Copy the content from `snowflake/rsa_key_pub_value.txt` and run:

```sql
-- Replace <YOUR_PUBLIC_KEY> with the content from rsa_key_pub_value.txt
ALTER USER STREAMEV SET RSA_PUBLIC_KEY='<YOUR_PUBLIC_KEY>';
```

> ⚠️ **Important**: The public key should be a single line without the `-----BEGIN/END PUBLIC KEY-----` headers.

### 2.3 Create INGESTION Database

```sql
-- Create the main database for event data
CREATE DATABASE IF NOT EXISTS INGESTION;
CREATE SCHEMA IF NOT EXISTS INGESTION.PUBLIC;

-- Grant permissions to STREAM role
GRANT USAGE ON DATABASE INGESTION TO ROLE STREAM;
GRANT USAGE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT CREATE TABLE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
```

| Object | Name | Maps to `.env.example` |
|--------|------|------------------------|
| Database | `INGESTION` | `SNOWFLAKE_DATABASE`, `SNOWFLAKE_1_DATABASE` |
| Schema | `PUBLIC` | `SNOWFLAKE_SCHEMA_NAME`, `SNOWFLAKE_1_SCHEMA` |

### 2.4 Create CONTROL Database

```sql
-- Create the database for checkpointing
CREATE DATABASE IF NOT EXISTS CONTROL;
CREATE SCHEMA IF NOT EXISTS CONTROL.PUBLIC;

-- Grant permissions to STREAM role
GRANT USAGE ON DATABASE CONTROL TO ROLE STREAM;
GRANT USAGE ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM;
GRANT CREATE TABLE ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM;
```

| Object | Name | Maps to `.env.example` |
|--------|------|------------------------|
| Database | `CONTROL` | `TARGET_DB` |
| Schema | `PUBLIC` | `TARGET_SCHEMA` |

### 2.5 Create Control Table (INGESTION_STATUS)

Choose **one** of the following options based on your Snowflake account:

#### Option A: Hybrid Table (Paid accounts only)

```sql
USE DATABASE CONTROL;
USE SCHEMA PUBLIC;

CREATE HYBRID TABLE IF NOT EXISTS INGESTION_STATUS (
    TS_INSERTED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    EVENTHUB_NAMESPACE VARCHAR(500),
    EVENTHUB VARCHAR(200),
    TARGET_DB VARCHAR(200),
    TARGET_SCHEMA VARCHAR(200),
    TARGET_TABLE VARCHAR(200),
    WATERLEVEL NUMBER(38, 0),
    PARTITION_ID VARCHAR(50) NOT NULL,
    METADATA VARIANT,
    PRIMARY KEY (EVENTHUB_NAMESPACE, EVENTHUB, TARGET_DB, TARGET_SCHEMA, TARGET_TABLE, PARTITION_ID)
);

GRANT SELECT, INSERT, UPDATE ON TABLE CONTROL.PUBLIC.INGESTION_STATUS TO ROLE STREAM;
```

> 💡 Set `USE_HYBRID_TABLE=true` in your `.env` file.

#### Option B: Standard Table (Trial accounts)

```sql
USE DATABASE CONTROL;
USE SCHEMA PUBLIC;

CREATE TABLE IF NOT EXISTS INGESTION_STATUS (
    TS_INSERTED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    EVENTHUB_NAMESPACE VARCHAR(500),
    EVENTHUB VARCHAR(200),
    TARGET_DB VARCHAR(200),
    TARGET_SCHEMA VARCHAR(200),
    TARGET_TABLE VARCHAR(200),
    WATERLEVEL NUMBER(38, 0),
    PARTITION_ID VARCHAR(50) NOT NULL,
    METADATA VARIANT
);

GRANT SELECT, INSERT, UPDATE ON TABLE CONTROL.PUBLIC.INGESTION_STATUS TO ROLE STREAM;
```

> 💡 Set `USE_HYBRID_TABLE=false` in your `.env` file.

| Object | Name | Maps to `.env.example` |
|--------|------|------------------------|
| Table | `INGESTION_STATUS` | `TARGET_TABLE` |

### 2.6 Grant Warehouse Access

```sql
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE STREAM;
```

| Object | Name | Maps to `.env.example` |
|--------|------|------------------------|
| Warehouse | `COMPUTE_WH` | `SNOWFLAKE_WAREHOUSE` |

---

## ☁️ Step 3: Create External Volume

> **Where**: ❄️ Snowflake SQL Worksheet

The External Volume connects Snowflake to Azure Blob Storage for Iceberg table data.

### 3.1 Create the External Volume

```sql
CREATE OR REPLACE EXTERNAL VOLUME EXVOL
  STORAGE_LOCATIONS = (
    (
      NAME = 'azure-iceberg-storage'
      STORAGE_PROVIDER = 'AZURE'
      STORAGE_BASE_URL = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/<YOUR_CONTAINER>/'
      AZURE_TENANT_ID = '<YOUR_AZURE_TENANT_ID>'
    )
  );
```

| Parameter | Description | Example |
|-----------|-------------|---------|
| `<YOUR_STORAGE_ACCOUNT>` | Azure Storage account name | `mystorageaccount` |
| `<YOUR_CONTAINER>` | Blob container name | `iceberg-data` |
| `<YOUR_AZURE_TENANT_ID>` | Azure AD tenant ID | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |

After creating the external volume, complete the Azure consent and storage RBAC steps shown by Snowflake for the volume. The Snowflake-managed Azure service principal needs access to the storage account/container before `SYSTEM$VERIFY_EXTERNAL_VOLUME` can succeed.

### 3.2 Verify the External Volume

```sql
-- Check volume was created
SHOW EXTERNAL VOLUMES LIKE 'EXVOL';

-- Verify connectivity (may take a moment)
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('EXVOL');
```

> ✅ You should see a success message confirming Azure connectivity.

### 3.3 Grant Permissions

```sql
GRANT USAGE ON EXTERNAL VOLUME EXVOL TO ROLE STREAM;
```

---

## 🧊 Step 4: Create Iceberg Table & Pipe

> **Where**: ❄️ Snowflake SQL Worksheet

### 4.1 Create the Iceberg Table

```sql
USE DATABASE INGESTION;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE ICEBERG TABLE EVENTS_TABLE1 (
    EVENT_BODY STRING,
    PARTITION_ID STRING,
    SEQUENCE_NUMBER DECIMAL(38, 0),
    OFFSET STRING,
    ENQUEUED_TIME TIMESTAMP_LTZ(6),
    PROPERTIES STRING,
    SYSTEM_PROPERTIES STRING,
    INGESTION_TIMESTAMP TIMESTAMP_LTZ(6) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_LTZ(6))
)
CLUSTER BY (ENQUEUED_TIME)
EXTERNAL_VOLUME = 'EXVOL'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'events/';
```

| Object | Name | Maps to `.env.example` |
|--------|------|------------------------|
| Table | `EVENTS_TABLE1` | `SNOWFLAKE_1_TABLE` |

### 4.2 Create the Streaming Pipe

> ⚠️ **Important**: The high-performance Snowpipe Streaming SDK **requires** a PIPE object.

```sql
CREATE OR REPLACE PIPE EVENTS_TABLE_PIPE AS
COPY INTO INGESTION.PUBLIC.EVENTS_TABLE1 (
    EVENT_BODY,
    PARTITION_ID,
    SEQUENCE_NUMBER,
    OFFSET,
    ENQUEUED_TIME,
    PROPERTIES,
    SYSTEM_PROPERTIES
)
FROM (
    SELECT
        TO_VARCHAR($1:event_body) AS EVENT_BODY,
        TO_VARCHAR($1:partition_id) AS PARTITION_ID,
        $1:sequence_number::NUMBER(38,0) AS SEQUENCE_NUMBER,
        TO_VARCHAR($1:offset) AS OFFSET,
        $1:enqueued_time::TIMESTAMP_NTZ(6) AS ENQUEUED_TIME,
        TO_VARCHAR($1:properties) AS PROPERTIES,
        TO_VARCHAR($1:system_properties) AS SYSTEM_PROPERTIES
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);
```

| Object | Name | Maps to `.env.example` |
|--------|------|------------------------|
| Pipe | `EVENTS_TABLE_PIPE` | `SNOWFLAKE_PIPE_NAME` |

### 4.3 Grant Pipe Permissions

```sql
-- Grant PIPE permissions to STREAM role
GRANT OPERATE ON PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE TO ROLE STREAM;
GRANT MONITOR ON PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE TO ROLE STREAM;

-- Grant table permissions
GRANT INSERT ON TABLE INGESTION.PUBLIC.EVENTS_TABLE1 TO ROLE STREAM;
GRANT SELECT ON TABLE INGESTION.PUBLIC.EVENTS_TABLE1 TO ROLE STREAM;
GRANT USAGE ON EXTERNAL VOLUME EXVOL TO ROLE STREAM;
```

### 4.4 Verify Setup

```sql
-- Check pipe was created
SHOW PIPES LIKE 'EVENTS_TABLE_PIPE';

-- Check grants
SHOW GRANTS ON PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE;

-- Verify table exists
SHOW TABLES LIKE 'EVENTS_TABLE1' IN SCHEMA INGESTION.PUBLIC;
```

> ✅ All objects should be listed without errors.

---

## ⚙️ Step 5: Configure Environment

> **Where**: 🐚 Shell / Editor

### 5.1 Copy the Example File

```bash
cp .env.example .env
```

### 5.2 Fill in Your Values

Edit `.env` with your configuration:

```bash
# ============================================================================
# SNOWFLAKE CONNECTION
# ============================================================================
SNOWFLAKE_ACCOUNT=<YOUR_ACCOUNT>              # e.g., VWBIVWS-SV93109
SNOWFLAKE_USER=STREAMEV
SNOWFLAKE_PRIVATE_KEY_FILE=/path/to/snowflake/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=<YOUR_KEY_PASSWORD>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=INGESTION
SNOWFLAKE_SCHEMA_NAME=PUBLIC
SNOWFLAKE_ROLE=STREAM

# ============================================================================
# EVENT HUB
# ============================================================================
EVENTHUB_NAMESPACE=<YOUR_NAMESPACE>.servicebus.windows.net
EVENTHUBNAME_1=<YOUR_TOPIC>
EVENTHUBNAME_1_CONSUMER_GROUP=$Default

# ============================================================================
# SNOWFLAKE MAPPING (EventHub → Snowflake)
# ============================================================================
SNOWFLAKE_1_DATABASE=INGESTION
SNOWFLAKE_1_SCHEMA=PUBLIC
SNOWFLAKE_1_TABLE=EVENTS_TABLE1
SNOWFLAKE_1_BATCH=100
SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE

# ============================================================================
# CONTROL TABLE
# ============================================================================
TARGET_DB=CONTROL
TARGET_SCHEMA=PUBLIC
TARGET_TABLE=INGESTION_STATUS
USE_HYBRID_TABLE=false
```

> 📝 See [.env.example](.env.example) for all available options including Postgres backend, Smart Retry, and Logfire observability.

### 5.3 Validate Configuration

```bash
# Install dependencies
uv sync

# Validate your configuration
uv run python src/main.py validate-config
```

> ✅ You should see all checks passing.

---

## 🚀 Step 6: Run the Pipeline

> **Where**: 🐚 Shell

### Start Streaming

```bash
# Normal run
uv run python src/main.py run

# Dry run (no actual data ingestion)
uv run python src/main.py run --dry-run

# With smart retry enabled
uv run python src/main.py run --smart
```

### Monitor Status

```bash
# Check pipeline status
uv run python src/main.py status

# Show version
uv run python src/main.py version
```

> 🎉 **Success**: Events from your Event Hub topic are now streaming into the Snowflake Iceberg table!

---

## 🦆 Step 7: Query with DuckDB (Optional)

> This section enables you to query your Snowflake Iceberg table directly from DuckDB.

### 7.1 Create Programmatic Access Token

> **Where**: ❄️ Snowflake SQL Worksheet

```sql
ALTER USER STREAMEV
  ADD PROGRAMMATIC ACCESS TOKEN HORIZON_DUCKDB_PAT
  ROLE_RESTRICTION = 'STREAM'
  DAYS_TO_EXPIRY = 90;
```

> 📝 **Save the `client_secret`** from the output — you'll need it in the next step!

### 7.2 Get OAuth Token

> **Where**: 🐚 Shell

```bash
curl -i --fail -X POST \
  "https://<YOUR_ACCOUNT>.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens" \
  --header "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "scope=session:role:STREAM" \
  --data-urlencode "client_secret=<CLIENT_SECRET_FROM_7.1>"
```

| Parameter | Description |
|-----------|-------------|
| `<YOUR_ACCOUNT>` | Your Snowflake account (e.g., `VWBIVWS-SV93109`) |
| `<CLIENT_SECRET_FROM_7.1>` | The secret from Step 7.1 |

> 📝 **Save the `access_token`** from the JSON response!

### 7.3 Install DuckDB Extensions

> **Where**: 🦆 DuckDB

```sql
-- Install required extensions
INSTALL iceberg;
LOAD iceberg;

INSTALL httpfs;
LOAD httpfs;

INSTALL azure;
LOAD azure;
```

### 7.4 Create Azure Storage Secret

> **Where**: 🦆 DuckDB

```sql
CREATE SECRET azure_auto (
  TYPE azure,
  PROVIDER credential_chain,
  ACCOUNT_NAME '<YOUR_STORAGE_ACCOUNT>'
);
```

> 💡 This uses your local Azure CLI credentials. Make sure you're logged in with `az login`.

### 7.5 Create Iceberg Catalog Secret

> **Where**: 🦆 DuckDB

```sql
CREATE OR REPLACE SECRET sf_horizon_token (
  TYPE iceberg,
  TOKEN '<ACCESS_TOKEN_FROM_7.2>'
);
```

> ⚠️ Use the `access_token` from Step 7.2, NOT the `client_secret` from Step 7.1!

### 7.6 Attach Snowflake Catalog

> **Where**: 🦆 DuckDB

```sql
ATTACH 'sf' AS sf (
  TYPE iceberg,
  CLIENT_ID 'snowflake',
  ENDPOINT 'https://<YOUR_ACCOUNT>.snowflakecomputing.com/polaris/api/catalog',
  CATALOG 'INGESTION'
);
```

### 7.7 Query Your Data! 🎉

> **Where**: 🦆 DuckDB

```sql
-- Query all events
SELECT * FROM sf.PUBLIC.EVENTS_TABLE1;

-- Query with filters
SELECT 
    EVENT_BODY,
    PARTITION_ID,
    ENQUEUED_TIME
FROM sf.PUBLIC.EVENTS_TABLE1
WHERE ENQUEUED_TIME > '2025-01-01'
ORDER BY ENQUEUED_TIME DESC
LIMIT 100;

-- Count events by partition
SELECT 
    PARTITION_ID,
    COUNT(*) as event_count
FROM sf.PUBLIC.EVENTS_TABLE1
GROUP BY PARTITION_ID;
```

---

## 📚 Object Reference

| Object Type | Name | Environment Variable | Created In |
|-------------|------|----------------------|------------|
| User | `STREAMEV` | `SNOWFLAKE_USER` | Step 2.1 |
| Role | `STREAM` | `SNOWFLAKE_ROLE` | Step 2.1 |
| Warehouse | `COMPUTE_WH` | `SNOWFLAKE_WAREHOUSE` | (pre-existing) |
| Database | `INGESTION` | `SNOWFLAKE_DATABASE` | Step 2.3 |
| Database | `CONTROL` | `TARGET_DB` | Step 2.4 |
| Schema | `PUBLIC` | `SNOWFLAKE_SCHEMA_NAME` | Steps 2.3, 2.4 |
| External Volume | `EXVOL` | — | Step 3.1 |
| Iceberg Table | `EVENTS_TABLE1` | `SNOWFLAKE_1_TABLE` | Step 4.1 |
| Pipe | `EVENTS_TABLE_PIPE` | `SNOWFLAKE_PIPE_NAME` | Step 4.2 |
| Control Table | `INGESTION_STATUS` | `TARGET_TABLE` | Step 2.5 |

---

## 🔧 Troubleshooting

### ❌ RSA Key Authentication Failed

**Symptom**: `JWT token is invalid`

**Solutions**:

1. Verify the public key was assigned:

   ```sql
   DESC USER STREAMEV;
   ```

2. Check the key format in `rsa_key_pub_value.txt` — it should be a single line
3. Regenerate keys with `./generate_snowflake_keys.sh`

---

### ❌ PIPE Not Found

**Symptom**: `ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED`

**Solutions**:

1. Verify the pipe exists:

   ```sql
   SHOW PIPES LIKE 'EVENTS_TABLE_PIPE' IN SCHEMA INGESTION.PUBLIC;
   ```

2. Check grants:

   ```sql
   SHOW GRANTS ON PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE;
   ```

3. Ensure `SNOWFLAKE_PIPE_NAME` matches the created pipe name

---

### ❌ External Volume Error

**Symptom**: `SYSTEM$VERIFY_EXTERNAL_VOLUME failed`

**Solutions**:

1. Verify Azure Storage credentials and tenant ID
2. Check that the container exists and is accessible
3. Ensure the storage account allows Snowflake access

---

### ❌ DuckDB OAuth Token Failed

**Symptom**: `Authentication failed`

**Solutions**:

1. Tokens expire — regenerate with Step 7.2
2. Ensure you're using `access_token`, not `client_secret`
3. Verify the role in the scope matches: `session:role:STREAM`

---

### ❌ Azure CLI Credential Failed

**Symptom**: `AzureCliCredential failed` during Event Hub startup

**Solutions**:

1. Run `az login` to refresh credentials
2. Verify your account has access to the storage account
3. Check `az account show` to confirm the correct subscription

---

## 📎 Related Files

| File | Description |
|------|-------------|
| [.env.example](.env.example) | Environment variable template |
| [generate_snowflake_keys.sh](generate_snowflake_keys.sh) | RSA key generator script |
| [setup_snowflake.py](setup_snowflake.py) | Python setup script (alternative to SQL) |
| [setup_snowpipe_streaming.sql](setup_snowpipe_streaming.sql) | Iceberg table and pipe SQL |

---

## ✅ Quick Checklist

- [ ] Generated RSA keys (`./generate_snowflake_keys.sh`)
- [ ] Created user `STREAMEV` and role `STREAM` in Snowflake
- [ ] Assigned RSA public key to user
- [ ] Created `INGESTION` and `CONTROL` databases
- [ ] Created External Volume `EXVOL`
- [ ] Created Iceberg table `EVENTS_TABLE1`
- [ ] Created Pipe `EVENTS_TABLE_PIPE`
- [ ] Configured `.env` file
- [ ] Validated config with `uv run python src/main.py validate-config`
- [ ] Started pipeline with `uv run python src/main.py run`

---

**🎉 Congratulations!** Your Snowflake Iceberg streaming pipeline is ready!
