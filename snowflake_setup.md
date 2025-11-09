# Snowflake Key-Pair Authentication Setup Guide

This guide explains how to set up key-pair authentication for EvSnow to connect to Snowflake using private key authentication (JWT).

## Overview

EvSnow uses Snowflake's key-pair authentication method for secure, password-less connections. This method uses an RSA key pair:

- **Private key**: Stored securely on your local system (never shared)
- **Public key**: Uploaded to Snowflake and associated with your user account

## Prerequisites

- OpenSSL installed on your system
- Snowflake account with appropriate permissions
- Access to execute SQL commands in Snowflake (via SnowSQL, web UI, or another SQL client)

## Step 1: Generate RSA Key Pair

### Generate Private Key

Generate an RSA private key using OpenSSL:

```bash
# Generate unencrypted private key (2048-bit)
openssl genrsa -out rsa_key.pem 2048

# OR generate encrypted private key (recommended for production)
openssl genrsa -out rsa_key.pem 2048
openssl pkcs8 -topk8 -inform PEM -in rsa_key.pem -out rsa_key_encrypted.p8 -v2 aes256

# Set appropriate file permissions (Unix/Linux/Mac)
chmod 600 rsa_key.pem
# OR for encrypted key
chmod 600 rsa_key_encrypted.p8
```

**Important Security Notes:**

- Store the private key in a secure location
- Never commit the private key to version control
- Use file permissions to restrict access (chmod 600)
- Consider using encrypted keys for production environments

### Generate Public Key

Extract the public key from the private key:

```bash
# For unencrypted private key
openssl rsa -in rsa_key.pem -pubout -out rsa_key_pub.pem

# For encrypted private key
openssl rsa -in rsa_key_encrypted.p8 -pubout -out rsa_key_pub.pem
```

## Step 2: Extract Public Key Value

Remove the header and footer from the public key file and concatenate it to a single line:

```bash
# Unix/Linux/Mac
grep -v "BEGIN PUBLIC" rsa_key_pub.pem | grep -v "END PUBLIC" | tr -d '\n' > rsa_key_pub_value.txt

# View the result
cat rsa_key_pub_value.txt
```

The output should be a long string like:

```
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAy...
```

## Step 3: Assign Public Key to Snowflake User

### Option A: Using SnowSQL

Connect to Snowflake and run:

```sql
USE ROLE ACCOUNTADMIN;

ALTER USER <your_username> SET RSA_PUBLIC_KEY='<your_public_key_value>';
```

Replace:

- `<your_username>`: Your Snowflake username
- `<your_public_key_value>`: The public key string from `rsa_key_pub_value.txt`

Example:

```sql
ALTER USER john_doe SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAy...';
```

### Option B: Using Snowflake Web UI

1. Log in to Snowflake web interface
2. Switch to ACCOUNTADMIN role (or a role with user management privileges)
3. Navigate to: **Admin** → **Users & Roles** → **Users**
4. Find and click on your username
5. In the user details, find **RSA Public Key** field
6. Paste the public key value (without header/footer)
7. Click **Save**

## Step 4: Verify Key-Pair Authentication

Test the connection using SnowSQL:

```bash
snowsql -a <account_identifier> \
        -u <username> \
        --private-key-path rsa_key.pem \
        -w <warehouse> \
        -d <database> \
        -s <schema>
```

If using an encrypted private key:

```bash
snowsql -a <account_identifier> \
        -u <username> \
        --private-key-path rsa_key_encrypted.p8 \
        -w <warehouse> \
        -d <database> \
        -s <schema>
# You'll be prompted for the private key password
```

## Step 5: Configure EvSnow

Update your `.env` file with Snowflake connection settings:

### For Unencrypted Private Key

```bash
# Snowflake Connection Settings
SNOWFLAKE_ACCOUNT=myaccount.us-east-1
SNOWFLAKE_USER=john_doe
SNOWFLAKE_PRIVATE_KEY_FILE=/path/to/rsa_key.pem
SNOWFLAKE_PRIVATE_KEY_PASSWORD=  # Leave empty for unencrypted keys
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MYDB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=DATA_ENGINEER  # Optional
```

### For Encrypted Private Key (Recommended)

```bash
# Snowflake Connection Settings
SNOWFLAKE_ACCOUNT=myaccount.us-east-1
SNOWFLAKE_USER=john_doe
SNOWFLAKE_PRIVATE_KEY_FILE=/path/to/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=your_encryption_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MYDB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=DATA_ENGINEER  # Optional
```

## Step 6: Test EvSnow Connection

Validate your configuration:

```bash
evsnow validate-config
```

You should see:

```
✓ Configuration is valid!
✓ Snowflake control table verified/created successfully
```

## Checkpoint Management with Hybrid Tables

EvSnow uses Snowflake **Hybrid Tables** for checkpoint management (tracking waterlevel per partition). Hybrid tables provide:

- **OLTP capabilities**: Row-level locking for concurrent updates
- **Primary key constraints**: Ensures data integrity per partition
- **Optimized for frequent updates**: Perfect for checkpoint operations

### Hybrid Table Schema

The control table (`INGESTION_STATUS`) is automatically created as a hybrid table with the following schema:

```sql
CREATE HYBRID TABLE IF NOT EXISTS <database>.<schema>.INGESTION_STATUS (
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
```

### Key Features

1. **Composite Primary Key**: Ensures one checkpoint record per EventHub partition
2. **MERGE Operations**: EvSnow uses MERGE (upsert) to update checkpoints idempotently
3. **Concurrent Safe**: Multiple workers can update different partitions simultaneously
4. **No Duplicates**: Primary key constraint prevents duplicate checkpoint records

### Permissions Required

Your Snowflake user needs these permissions on the control table:

```sql
-- Grant permissions for checkpoint management
GRANT SELECT, INSERT, UPDATE ON TABLE <database>.<schema>.INGESTION_STATUS TO ROLE <your_role>;

-- If the table doesn't exist, you also need CREATE TABLE permission
GRANT CREATE TABLE ON SCHEMA <database>.<schema> TO ROLE <your_role>;
```

### Verifying Hybrid Table

After running `evsnow validate-config`, you can verify the hybrid table in Snowflake:

```sql
-- Check table type
SHOW TABLES LIKE 'INGESTION_STATUS' IN SCHEMA <database>.<schema>;

-- View table DDL
SELECT GET_DDL('TABLE', '<database>.<schema>.INGESTION_STATUS');

-- Check current checkpoints
SELECT * FROM <database>.<schema>.INGESTION_STATUS ORDER BY TS_INSERTED DESC;
```

### References

- [Snowflake Hybrid Tables Documentation](https://docs.snowflake.com/en/user-guide/tables-hybrid)
- [Hybrid Tables Best Practices](https://docs.snowflake.com/en/user-guide/tables-hybrid-best-practices)

## Troubleshooting

### Error: "Private key file not found"

- Check the path in `SNOWFLAKE_PRIVATE_KEY_FILE`
- Ensure the file exists and is readable
- Use absolute paths (e.g., `/home/user/keys/rsa_key.pem`)

### Error: "Invalid private key"

- Verify the key format is correct (PEM or PKCS#8)
- If encrypted, ensure `SNOWFLAKE_PRIVATE_KEY_PASSWORD` is correct
- Regenerate the key pair if corrupted

### Error: "Authentication failed"

- Verify the public key is correctly assigned to the user in Snowflake
- Check that the username matches exactly (case-sensitive)
- Ensure the user has necessary permissions

### Error: "JWT token is invalid"

- The private/public key pair may not match
- Try removing and re-adding the public key in Snowflake
- Ensure the public key was extracted correctly (no extra characters)

## Security Best Practices

1. **Use Encrypted Private Keys in Production**
   - Always encrypt private keys with a strong passphrase
   - Store the passphrase in a secure secret manager (e.g., AWS Secrets Manager, Azure Key Vault)

2. **Restrict File Permissions**

   ```bash
   chmod 600 rsa_key.pem
   chown $USER:$USER rsa_key.pem
   ```

3. **Rotate Keys Regularly**
   - Generate new key pairs periodically (e.g., every 90 days)
   - Update Snowflake and your `.env` file
   - Safely delete old keys

4. **Never Commit Keys to Version Control**
   - Add `*.pem`, `*.p8`, `*.key` to `.gitignore`
   - Use environment variables or secret managers
   - Scan repositories for accidentally committed secrets

5. **Monitor Key Usage**
   - Review Snowflake login history regularly
   - Set up alerts for failed authentication attempts
   - Revoke compromised keys immediately

## Key Rotation Procedure

When rotating keys:

1. Generate a new key pair (Steps 1-2 above)
2. Assign the new public key to your Snowflake user:

   ```sql
   ALTER USER <username> SET RSA_PUBLIC_KEY='<new_public_key>';
   ```

3. Update `.env` with the new private key path
4. Test the connection: `evsnow validate-config`
5. Once confirmed working, securely delete the old private key:

   ```bash
   shred -u old_rsa_key.pem  # Linux
   # OR
   srm old_rsa_key.pem  # Mac with srm installed
   ```

## Additional Resources

- [Snowflake Key-Pair Authentication Documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth)
- [Snowflake Security Best Practices](https://docs.snowflake.com/en/user-guide/security-best-practices)
- [OpenSSL Documentation](https://www.openssl.org/docs/)

## Support

For issues with EvSnow configuration:

- Run `evsnow validate-config --show-rbac` for detailed guidance
- Check application logs for detailed error messages
- Ensure Snowflake user has required permissions (INSERT, SELECT on target tables)

For Snowflake-specific issues:

- Contact your Snowflake administrator
- Review Snowflake query history for failed authentication attempts
- Check Snowflake account settings and user permissions
