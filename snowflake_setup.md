# Snowflake Key-Pair Authentication Setup

Set up key-pair authentication for EvSnow using RSA keys (JWT).

## Prerequisites

- OpenSSL installed
- Snowflake account with a role that can set user keys
- Ability to run SQL in Snowflake (Snowsight or SnowSQL)

## 1) Generate RSA keys (PKCS#8, encrypted)

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

## 4) Test authentication with SnowSQL

```bash
snowsql -a <account_identifier> \
        -u <username> \
        --private-key-path rsa_key_encrypted.p8 \
        -w <warehouse> -d <database> -s <schema>
# Prompts for the key password
```

## 5) Configure EvSnow

Update `.env` (examples):

```bash
SNOWFLAKE_ACCOUNT=myaccount.us-east-1
SNOWFLAKE_USER=john_doe
SNOWFLAKE_PRIVATE_KEY_FILE=/path/to/rsa_key_encrypted.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=your_encryption_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MYDB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=DATA_ENGINEER
```

Then validate:

```bash
evsnow validate-config
```

## Checkpoint table (Hybrid vs. Standard)

EvSnow creates `INGESTION_STATUS` automatically. If your account supports Hybrid Tables, it will use them; otherwise it creates a standard table. Required permissions:

```sql
GRANT CREATE TABLE ON SCHEMA <db>.<schema> TO ROLE <role>;
GRANT SELECT, INSERT, UPDATE ON TABLE <db>.<schema>.INGESTION_STATUS TO ROLE <role>;
```

## Troubleshooting

- **Private key file not found**: Check `SNOWFLAKE_PRIVATE_KEY_FILE` path and permissions.
- **Invalid/incorrect key password**: Re-run OpenSSL with the right passphrase.
- **Authentication failed / JWT invalid**: Reassign the public key to the user; ensure usernames match case.
- **Permissions**: User/role needs CREATE TABLE on control schema and DML on `INGESTION_STATUS`.

## Security best practices

- Encrypt private keys; store passwords in a secret manager.
- Lock down permissions: `chmod 600` on key files.
- Rotate keys regularly and remove old keys from Snowflake.
- Never commit keys to Git; keep `*.pem` / `*.p8` ignored.
