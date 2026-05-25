# 🦆 Querying Snowflake Iceberg Tables with DuckDB

This guide shows you how to query your Snowflake Iceberg table directly using DuckDB and its Iceberg extension.

---

## 📋 Prerequisites

- ✅ Snowflake account with the Iceberg table created
- ✅ Snowflake-managed Iceberg table created
- ✅ DuckDB installed locally
- ✅ Azure CLI authenticated only if you use the optional Azure storage secret for customer-managed external-volume tables
- ✅ A valid PAT created in Snowflake (see Step 1)

---

## 🔐 Step 1: Create a Programmatic Access Token in Snowflake

Run this SQL in Snowflake to create a token for DuckDB access:

```sql
ALTER USER <YOUR_USERNAME>
  ADD PROGRAMMATIC ACCESS TOKEN EVSNOW_DUCKDB_PAT
  ROLE_RESTRICTION = '<YOUR_ROLE>'
  DAYS_TO_EXPIRY = 90;
```

| Parameter | Description | Example |
|-----------|-------------|---------|
| `<YOUR_USERNAME>` | Your Snowflake username | `STREAMEV` |
| `<YOUR_ROLE>` | Least-privilege role with read access to the table | `STREAM_READER` |

> 📝 **Note**: Save the `client_secret` value returned - you'll need it in the next step!

Do not use `ACCOUNTADMIN` for query tokens. Create or reuse a role that can
read only the Iceberg objects you need to inspect.

---

## 🌐 Step 2: Get OAuth Token via REST API

Use `curl` to exchange your client secret for an OAuth token:

```bash
curl -i --fail -X POST "https://<YOUR_ACCOUNT>.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens" \
  --header "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "scope=session:role:<YOUR_ROLE>" \
  --data-urlencode "client_secret=<YOUR_CLIENT_SECRET_FROM_STEP_1>"
```

| Parameter | Description |
|-----------|-------------|
| `<YOUR_ACCOUNT>` | Your Snowflake account identifier (e.g., `ABC12345-XY98765`) |
| `<YOUR_ROLE>` | Same role used in Step 1 |
| `<YOUR_CLIENT_SECRET_FROM_STEP_1>` | The secret from Step 1 |

> ✅ **Success**: You'll receive a JSON response with an `access_token`. Save this token for Step 5!

---

## 🦆 Step 3: Install DuckDB Extensions

Open DuckDB and install the required extensions:

```sql
-- Install and load Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Install and load HTTP filesystem support
INSTALL httpfs;
LOAD httpfs;

-- Install and load Azure support
INSTALL azure;
LOAD azure;
```

---

## ☁️ Step 4: Optional Azure Storage Secret

Skip this step for EvSnow's default Snowflake-managed internal Iceberg storage unless your external query workflow requires direct cloud-storage credentials. For customer-managed external-volume Iceberg tables, create a secret to authenticate with the Azure Storage account that contains the table files.

```sql
CREATE SECRET azure_auto (
  TYPE azure,
  PROVIDER credential_chain,
  ACCOUNT_NAME '<YOUR_STORAGE_ACCOUNT>'
);
```

| Parameter | Description |
|-----------|-------------|
| `<YOUR_STORAGE_ACCOUNT>` | Azure Storage account used by a customer-managed external volume |

> 💡 **Tip**: The `credential_chain` provider uses your local Azure CLI credentials. Make sure you're logged in with `az login`.

---

## 🎫 Step 5: Create Iceberg Catalog Secret

Create a secret with the OAuth token from **Step 2** (not Step 1!):

```sql
CREATE OR REPLACE SECRET sf_horizon_token (
  TYPE iceberg,
  TOKEN '<YOUR_OAUTH_TOKEN_FROM_STEP_2>'
);
```

> ⚠️ **Important**: Use the `access_token` from the curl response in Step 2, NOT the `client_secret` from Step 1.

---

## 🔗 Step 6: Attach the Snowflake Iceberg Catalog

Attach your Snowflake catalog to DuckDB:

```sql
ATTACH 'sf' AS sf (
  TYPE iceberg,
  CLIENT_ID 'snowflake',
  ENDPOINT 'https://<YOUR_ACCOUNT>.snowflakecomputing.com/polaris/api/catalog',
  CATALOG 'INGESTION'
);
```

| Parameter | Description |
|-----------|-------------|
| `<YOUR_ACCOUNT>` | Your Snowflake account identifier |
| `INGESTION` | The database name (matches `setup_snowpipe_streaming.sql`) |

---

## 🚀 Step 7: Query Your Data!

Now you can query the Iceberg table directly from DuckDB:

```sql
-- Query all events
SELECT * FROM sf.PUBLIC.EVENTS_TABLE1;
```

> 🎉 **Congratulations**: You've successfully queried your Snowflake Iceberg table using DuckDB!
