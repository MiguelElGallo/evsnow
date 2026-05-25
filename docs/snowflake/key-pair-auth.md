# Snowflake Key-Pair Authentication Setup

Set up key-pair authentication for EvSnow using RSA keys (JWT).

## Prerequisites

- OpenSSL installed
- Snowflake account with a role that can set user keys
- Ability to run SQL in Snowflake (Snowsight or Snowflake CLI `snow`)

## 1) Generate RSA keys (PKCS#8, encrypted)

The repo helper creates the encrypted private key and public key value used by
the quickstart:

```bash
./generate_snowflake_keys.sh
```

For headless validation, set `EVSNOW_KEY_PASSWORD` instead of piping a password
to OpenSSL.

Manual equivalent:

```bash
# Encrypted private key (recommended) - uses DES3 like generate_snowflake_keys.sh
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key_encrypted.p8 -v2 des3

# Public key
openssl rsa -in rsa_key_encrypted.p8 -pubout -out rsa_key_pub.pem

# Lock down permissions
chmod 600 rsa_key_encrypted.p8 rsa_key_pub.pem
```

## 2) Extract the public key value

```bash
grep -v "BEGIN PUBLIC" rsa_key_pub.pem | grep -v "END PUBLIC" | tr -d '\n' > rsa_key_pub_value.txt
cat rsa_key_pub_value.txt  # use this value in the SQL below
```

## 3) Assign public key to your Snowflake user

```sql
USE ROLE ACCOUNTADMIN;
ALTER USER <your_username> SET RSA_PUBLIC_KEY='<public_key_value>';
```

For non-interactive ingestion users, prefer a Snowflake `SERVICE` user.
The quickstart bootstrap in [Snowflake quickstart](../getting-started/snowflake-quickstart.md)
creates `STREAMEV` as a service user and assigns the public key.

## 4) Test authentication with Snowflake CLI

```bash
snow connection test \
  --account <account_identifier> \
  --user <username> \
  --authenticator SNOWFLAKE_JWT \
  --private-key-file rsa_key_encrypted.p8
```

Private-key authentication requires `SNOWFLAKE_JWT`. EvSnow uses the same key
file through `SNOWFLAKE_PRIVATE_KEY_FILE`.

## 5) Configure EvSnow

Update `.env` (examples):

```bash
SNOWFLAKE_ACCOUNT=myaccount.us-east-1
SNOWFLAKE_USER=john_doe
SNOWFLAKE_PRIVATE_KEY_FILE=/path/to/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=your_encryption_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MYDB
SNOWFLAKE_SCHEMA_NAME=PUBLIC
SNOWFLAKE_ROLE=DATA_ENGINEER
SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE
```

EvSnow passes the encrypted private-key file and
`SNOWFLAKE_PRIVATE_KEY_PASSWORD` directly to Snowpipe Streaming SDK `1.4.0`.
It does not create an unencrypted temporary private-key file.

Then validate:

```bash
uv run evsnow validate-config --show-rbac
```

## Checkpoint table (INGESTION_STATUS)

Create the control table using the working DDL:

```sql
CREATE OR REPLACE TABLE CONTROL.PUBLIC.INGESTION_STATUS (
    TS_INSERTED TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    EVENTHUB_NAMESPACE VARCHAR(500) NOT NULL,
    EVENTHUB VARCHAR(200) NOT NULL,
    TARGET_DB VARCHAR(200) NOT NULL,
    TARGET_SCHEMA VARCHAR(200) NOT NULL,
    TARGET_TABLE VARCHAR(200) NOT NULL,
    WATERLEVEL NUMBER(38,0),
    PARTITION_ID VARCHAR(50) NOT NULL,
    METADATA VARIANT,
    PRIMARY KEY (EVENTHUB_NAMESPACE, EVENTHUB, TARGET_DB, TARGET_SCHEMA, TARGET_TABLE, PARTITION_ID)
);
```

Required permissions:

```sql
GRANT CREATE SCHEMA ON DATABASE CONTROL TO ROLE <role>;
GRANT CREATE TABLE ON SCHEMA CONTROL.PUBLIC TO ROLE <role>;
GRANT SELECT, INSERT, UPDATE ON TABLE CONTROL.PUBLIC.INGESTION_STATUS TO ROLE <role>;
```

## Troubleshooting

- **Private key file not found**: Check `SNOWFLAKE_PRIVATE_KEY_FILE` path and permissions.
- **Invalid/incorrect key password**: Re-run OpenSSL with the right passphrase.
- **Authentication failed / JWT invalid**: Reassign the public key to the user; ensure usernames match case.
- **Permissions**: User/role needs `CREATE SCHEMA` on the control database,
  `CREATE TABLE` on the control schema, and DML on `INGESTION_STATUS`.

## Security best practices

- Encrypt private keys; store passwords in a secret manager.
- Lock down permissions: `chmod 600` on key files.
- Rotate keys regularly and remove old keys from Snowflake.
- Never commit keys to Git; keep `*.pem` / `*.p8` ignored.
