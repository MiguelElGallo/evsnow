# Snowflake Setup Quick Start Guide

This guide will walk you through setting up EvSnow to connect to Snowflake in 5 simple steps.

Official detailed documentation is available in [Link](https://docs.snowflake.com/en/user-guide/key-pair-auth).

## Prerequisites

- Snowflake account with appropriate permissions (ACCOUNTADMIN or equivalent)
- Azure Event Hub namespace and credentials
- OpenSSL installed on your system
- EvSnow installed (`uv sync`)

> 📄 **Configuration Reference:** All environment variables are documented in [`.env.example`](./.env.example). Copy it to `.env` and customize the values as you follow this guide.

## Quick Setup (5 Steps)

### Step 1: Generate RSA Keys

Run the automated key generation script:

```bash
./generate_snowflake_keys.sh
```

**What this does:**

- Creates `snowflake/` directory
- Generates encrypted RSA private key (you'll set a password)
- Extracts public key for Snowflake
- Displays your public key value

**⚠️ Important:**

- Remember the password you set - you'll need it for `.env`
- The public key value will be displayed at the end - copy it!

### Step 2: Assign Public Key to Snowflake User

1. Log into Snowflake (Web UI or SnowSQL)
2. Run this SQL command (replace placeholders):

```sql
USE ROLE ACCOUNTADMIN;

ALTER USER <your_username> 
SET RSA_PUBLIC_KEY='<paste_the_public_key_value_from_step1>';
```

**Example:**

```sql
USE ROLE ACCOUNTADMIN;

ALTER USER john_doe 
SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAy...';
```

### Step 2.5: Create Role, Databases, and Grant Permissions

Before configuring your `.env` file, you need to create the required Snowflake resources and grant permissions. Run this SQL script in Snowflake as `ACCOUNTADMIN`:

```sql
-- ============================================
-- Run as ACCOUNTADMIN in Snowflake
-- ============================================
USE ROLE ACCOUNTADMIN;

-- ============================================
-- Create the STREAM role (matches SNOWFLAKE_ROLE in .env)
-- ============================================
CREATE ROLE IF NOT EXISTS STREAM;

-- Grant the role to your user (matches SNOWFLAKE_USER in .env)
GRANT ROLE STREAM TO USER STREAMEV;

-- ============================================
-- Create databases if they don't exist
-- These match TARGET_DB and SNOWFLAKE_DATABASE in .env
-- ============================================
CREATE DATABASE IF NOT EXISTS CONTROL;      -- For checkpoint/control table (TARGET_DB)
CREATE DATABASE IF NOT EXISTS INGESTION;    -- For event data (SNOWFLAKE_DATABASE, SNOWFLAKE_1_DATABASE)

-- ============================================
-- Create schemas if they don't exist
-- These match TARGET_SCHEMA and SNOWFLAKE_SCHEMA in .env
-- ============================================
CREATE SCHEMA IF NOT EXISTS CONTROL.PUBLIC;
CREATE SCHEMA IF NOT EXISTS INGESTION.PUBLIC;

-- ============================================
-- Warehouse permissions (matches SNOWFLAKE_WAREHOUSE in .env)
-- ============================================
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE STREAM;

-- ============================================
-- CONTROL database - FULL permissions
-- Used for: TARGET_DB, TARGET_SCHEMA, TARGET_TABLE (INGESTION_STATUS)
-- ============================================
GRANT ALL PRIVILEGES ON DATABASE CONTROL TO ROLE STREAM;
GRANT ALL PRIVILEGES ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA CONTROL.PUBLIC TO ROLE STREAM;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA CONTROL.PUBLIC TO ROLE STREAM;

-- ============================================
-- INGESTION database - FULL permissions
-- Used for: SNOWFLAKE_1_DATABASE, SNOWFLAKE_1_SCHEMA, SNOWFLAKE_1_TABLE
-- ============================================
GRANT ALL PRIVILEGES ON DATABASE INGESTION TO ROLE STREAM;
GRANT ALL PRIVILEGES ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA INGESTION.PUBLIC TO ROLE STREAM;

-- ============================================
-- Verify the grants
-- ============================================
SHOW GRANTS TO ROLE STREAM;
SHOW GRANTS ON DATABASE CONTROL;
SHOW GRANTS ON DATABASE INGESTION;
```

**If you still get "Insufficient privileges" errors**, run these additional commands to transfer ownership:

```sql
-- Transfer ownership of databases and schemas to STREAM role
GRANT OWNERSHIP ON DATABASE CONTROL TO ROLE STREAM COPY CURRENT GRANTS;
GRANT OWNERSHIP ON DATABASE INGESTION TO ROLE STREAM COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM COPY CURRENT GRANTS;
```

**How this relates to `.env.example`:**

| SQL Resource | `.env` Variable | Purpose |
|--------------|-----------------|---------|
| `ROLE STREAM` | `SNOWFLAKE_ROLE=STREAM` | Role used for all operations |
| `USER STREAMEV` | `SNOWFLAKE_USER=STREAMEV` | User connecting to Snowflake |
| `DATABASE CONTROL` | `TARGET_DB=CONTROL` | Stores checkpoint/control table |
| `SCHEMA CONTROL.PUBLIC` | `TARGET_SCHEMA=PUBLIC` | Schema for control table |
| `TABLE INGESTION_STATUS` | `TARGET_TABLE=INGESTION_STATUS` | Created automatically by EvSnow |
| `DATABASE INGESTION` | `SNOWFLAKE_DATABASE=INGESTION`, `SNOWFLAKE_1_DATABASE=INGESTION` | Stores ingested event data |
| `SCHEMA INGESTION.PUBLIC` | `SNOWFLAKE_SCHEMA=PUBLIC`, `SNOWFLAKE_1_SCHEMA=PUBLIC` | Schema for event tables |
| `WAREHOUSE COMPUTE_WH` | `SNOWFLAKE_WAREHOUSE=compute_wh` | Warehouse for query execution |

**⚠️ Customize for your environment:**

- Replace `STREAMEV` with your actual Snowflake username
- Replace `COMPUTE_WH` with your warehouse name
- Add additional databases/schemas if you're using different ones in your `.env`

### Step 2.6: Create Target Table and PIPE Object (Required for High-Performance SDK)

EvSnow uses Snowflake's **high-performance Snowpipe Streaming SDK**, which requires a **PIPE object**. Run this SQL to create the target table and PIPE:

```sql
-- ============================================================================
-- SNOWPIPE STREAMING HIGH-PERFORMANCE ARCHITECTURE SETUP
-- Reference: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started
-- ============================================================================

USE ROLE STREAM;  -- Or ACCOUNTADMIN if STREAM doesn't have create privileges yet
USE DATABASE INGESTION;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- Create the target table for Event Hub data
-- Matches: SNOWFLAKE_1_TABLE=events_table in .env
-- ============================================================================
CREATE TABLE IF NOT EXISTS EVENTS_TABLE (
    EVENT_BODY VARIANT,
    PARTITION_ID VARCHAR(50),
    SEQUENCE_NUMBER NUMBER(38,0),
    ENQUEUED_TIME TIMESTAMP_NTZ,
    PROPERTIES VARIANT,
    SYSTEM_PROPERTIES VARIANT,
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- Create PIPE for HIGH-PERFORMANCE Snowpipe Streaming
-- Matches: SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE in .env
-- 
-- IMPORTANT: High-performance architecture REQUIRES a PIPE object
-- The PIPE uses DATA_SOURCE(TYPE => 'STREAMING') as the source
-- ============================================================================
CREATE OR REPLACE PIPE EVENTS_TABLE_PIPE
AS
    COPY INTO EVENTS_TABLE (EVENT_BODY, PARTITION_ID, SEQUENCE_NUMBER, ENQUEUED_TIME, PROPERTIES, SYSTEM_PROPERTIES)
    FROM (
        SELECT 
            $1:event_body::VARIANT,
            $1:partition_id::VARCHAR,
            $1:sequence_number::NUMBER,
            $1:enqueued_time::TIMESTAMP_NTZ,
            $1:properties::VARIANT,
            $1:system_properties::VARIANT
        FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
    );

-- ============================================================================
-- Verify the PIPE was created
-- ============================================================================
SHOW PIPES LIKE 'EVENTS_TABLE_PIPE';

-- ============================================================================
-- Grant permissions to STREAM role
-- ============================================================================
GRANT OPERATE ON PIPE EVENTS_TABLE_PIPE TO ROLE STREAM;
GRANT MONITOR ON PIPE EVENTS_TABLE_PIPE TO ROLE STREAM;
GRANT INSERT ON TABLE EVENTS_TABLE TO ROLE STREAM;
GRANT SELECT ON TABLE EVENTS_TABLE TO ROLE STREAM;

-- Verify grants
SHOW GRANTS ON PIPE EVENTS_TABLE_PIPE;

SELECT 'Snowpipe Streaming HIGH-PERFORMANCE setup complete!' AS STATUS;
```

**How this relates to [`.env.example`](./.env.example):**

| SQL Resource | `.env` Variable | Purpose |
|--------------|-----------------|---------|
| `TABLE EVENTS_TABLE` | `SNOWFLAKE_1_TABLE=events_table` | Target table for ingested events |
| `PIPE EVENTS_TABLE_PIPE` | `SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE` | Required for high-performance SDK |

> ⚠️ **Error `ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED`?** This means the PIPE hasn't been created or your role doesn't have permissions. Run the SQL above to fix it.

> 📄 **Alternative:** You can also run the pre-made script: [`setup_snowpipe_streaming.sql`](./setup_snowpipe_streaming.sql)

### Step 3: Update `.env` File

Your `.env` file has been pre-configured with Snowflake settings. See [`.env.example`](./.env.example) for all available configuration options with detailed comments.

Update these values:

```bash
# Snowflake Connection Settings
SNOWFLAKE_ACCOUNT=xy12345.us-east-1              # Your Snowflake account identifier
SNOWFLAKE_USER=john_doe                          # Your Snowflake username
SNOWFLAKE_PRIVATE_KEY_FILE=/Users/miguelperedo/Github/evsnow/snowflake/rsa_key_encrypted.p8  # Already set
SNOWFLAKE_PRIVATE_KEY_PASSWORD=YourPassword123   # Password from Step 1
SNOWFLAKE_WAREHOUSE=COMPUTE_WH                   # Your warehouse name
SNOWFLAKE_DATABASE=MYDB                          # Your database
SNOWFLAKE_SCHEMA=PUBLIC                          # Your schema
SNOWFLAKE_ROLE=DATA_ENGINEER                     # Optional: your role

# Control table configuration
TARGET_DB=MYDB                                   # Database for control table
TARGET_SCHEMA=CONTROL                            # Schema for control table
TARGET_TABLE=INGESTION_STATUS                    # Leave as-is

# Ingestion configuration
SNOWFLAKE_1_DATABASE=MYDB                        # Where to ingest EventHub data
SNOWFLAKE_1_SCHEMA=INGEST                        # Schema for ingested data
SNOWFLAKE_1_TABLE=events_table                   # Table name for ingested data
SNOWFLAKE_1_BATCH=1000                           # Batch size (leave as-is)
```

**How to find your Snowflake account identifier:**

- **Option 1:** In Snowflake UI, look at the URL: `https://<account_identifier>.snowflakecomputing.com`
- **Option 2:** Run in Snowflake: `SELECT CURRENT_ACCOUNT(), CURRENT_REGION();`
- **Format:** `<account_locator>.<region>` (e.g., `xy12345.us-east-1`)

### Step 4: Verify Configuration

Run the verification script to check if everything is set up correctly:

```bash
./verify_snowflake_setup.sh
```

This will check:

- ✓ All required environment variables are set
- ✓ Private key file exists and has correct permissions
- ✓ No placeholder values remain

**Expected output:**

```
✅ All required configuration values are set!
```

### Step 5: Test Connection and Create Control Table

Run EvSnow's built-in validation:

```bash
evsnow validate-config
```

**What this does:**

- Tests Snowflake connection using key-pair authentication
- Creates the `INGESTION_STATUS` hybrid table (if it doesn't exist)
- Verifies permissions

**Expected output:**

```
✓ Configuration is valid!
✓ Snowflake control table verified/created successfully
```

## You're Ready! 🎉

Start the pipeline:

```bash
evsnow run
```

Or test with dry-run mode first:

```bash
evsnow run --dry-run
```

## Troubleshooting

### Error: "Private key file not found"

```bash
# Check if the file exists
ls -la snowflake/rsa_key_encrypted.p8

# If not, regenerate keys
./generate_snowflake_keys.sh
```

### Error: "Authentication failed"

```bash
# Verify public key is assigned in Snowflake
# Run this in Snowflake:
DESC USER <your_username>;

# Look for RSA_PUBLIC_KEY_FP (fingerprint) - should not be empty
```

### Error: "Invalid private key password"

```bash
# Test the password manually
openssl rsa -in snowflake/rsa_key_encrypted.p8 -check

# If it fails, you need to regenerate the keys
./generate_snowflake_keys.sh
```

### Error: "Insufficient privileges"

```bash
# Your Snowflake user needs these permissions:
# 1. CREATE TABLE on the control schema
# 2. INSERT, SELECT, UPDATE on INGESTION_STATUS table
# 3. INSERT, CREATE TABLE on ingestion schema

# Ask your Snowflake admin to grant:
GRANT CREATE TABLE ON SCHEMA <TARGET_SCHEMA> TO ROLE <your_role>;
GRANT INSERT, SELECT, UPDATE ON TABLE <TARGET_DB>.<TARGET_SCHEMA>.INGESTION_STATUS TO ROLE <your_role>;
```

### Still having issues?

1. Check detailed errors: `evsnow validate-config --verbose`
2. Verify Snowflake connectivity: `snowsql -a <account> -u <user> --private-key-path snowflake/rsa_key_encrypted.p8`
3. Check the full setup guide: [snowflake_setup.md](./snowflake_setup.md)

## Key Files Reference

| File | Purpose | Commit to Git? |
|------|---------|----------------|
| `.env` | Your configuration with secrets | ❌ No (already in .gitignore) |
| `snowflake/rsa_key_encrypted.p8` | Private key | ❌ No (already in .gitignore) |
| `snowflake/rsa_key_pub.pem` | Public key file | ❌ No (already in .gitignore) |
| [`.env.example`](./.env.example) | Configuration template with all options documented | ✅ Yes |
| `generate_snowflake_keys.sh` | Key generation script | ✅ Yes |
| `verify_snowflake_setup.sh` | Verification script | ✅ Yes |
| `snowflake_setup.md` | Detailed setup guide | ✅ Yes |

## Security Best Practices

1. **Never commit keys to Git:**
   - The `snowflake/` directory is in `.gitignore`
   - Never add `*.pem` or `*.p8` files to version control

2. **Secure file permissions:**

   ```bash
   chmod 600 snowflake/rsa_key_encrypted.p8
   ```

3. **Rotate keys regularly:**
   - Generate new keys every 90 days
   - Update Snowflake with new public key
   - Delete old private keys securely

4. **Store passwords securely:**
   - Use a password manager for `SNOWFLAKE_PRIVATE_KEY_PASSWORD`
   - Consider using environment variables from a secret manager in production

5. **Revoke compromised keys immediately:**

   ```sql
   ALTER USER <username> UNSET RSA_PUBLIC_KEY;
   ```

## What's Next?

- **Multiple Event Hubs:** Add `EVENTHUBNAME_2`, `SNOWFLAKE_2_*` configurations (see [`.env.example`](./.env.example))
- **Monitoring:** Enable Logfire observability (see [`.env.example`](./.env.example) for `LOGFIRE_*` settings)
- **Smart Retry:** Configure LLM-powered retry logic (see [`.env.example`](./.env.example) for `SMART_RETRY_*` settings)
- **Production:** Review [snowflake_setup.md](./snowflake_setup.md) for advanced topics

## Need Help?

- **Detailed Setup:** [snowflake_setup.md](./snowflake_setup.md)
- **Snowflake Docs:** [Key-Pair Authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth)
- **EvSnow README:** [README.md](./README.md)
