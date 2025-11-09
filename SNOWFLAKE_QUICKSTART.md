# Snowflake Setup Quick Start Guide

This guide will walk you through setting up EvSnow to connect to Snowflake in 5 simple steps.

## Prerequisites

- Snowflake account with appropriate permissions (ACCOUNTADMIN or equivalent)
- Azure Event Hub namespace and credentials
- OpenSSL installed on your system
- EvSnow installed (`uv sync`)

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

### Step 3: Update `.env` File

Your `.env` file has been pre-configured with Snowflake settings. Update these values:

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
| `.env.example` | Configuration template | ✅ Yes |
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

- **Multiple Event Hubs:** Add `EVENTHUBNAME_2`, `SNOWFLAKE_2_*` configurations
- **Monitoring:** Enable Logfire observability (see `.env.example`)
- **Smart Retry:** Configure LLM-powered retry logic for error handling
- **Production:** Review [snowflake_setup.md](./snowflake_setup.md) for advanced topics

## Need Help?

- **Detailed Setup:** [snowflake_setup.md](./snowflake_setup.md)
- **Snowflake Docs:** [Key-Pair Authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth)
- **EvSnow README:** [README.md](./README.md)
