# Query Snowflake Iceberg Tables With DuckDB

Use this guide to inspect a Snowflake-managed Iceberg table from DuckDB through
Snowflake's Iceberg REST catalog.

## Prerequisites

- Snowflake account with the EvSnow Iceberg table created
- Least-privilege Snowflake role with read access to the target table
- DuckDB installed locally
- Azure CLI authentication only if you query customer-managed external-volume
  Iceberg tables through direct storage credentials

## Create A Programmatic Access Token

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

Save the returned `client_secret` value for the OAuth token exchange. Do not use
`ACCOUNTADMIN` for query tokens.

## Exchange The Secret For An OAuth Token

Use `curl` to exchange the client secret for an OAuth token:

```bash
curl -i --fail -X POST "https://<YOUR_ACCOUNT>.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens" \
  --header "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "scope=session:role:<YOUR_ROLE>" \
  --data-urlencode "client_secret=<YOUR_CLIENT_SECRET_FROM_STEP_1>"
```

| Parameter | Description |
|-----------|-------------|
| `<YOUR_ACCOUNT>` | Snowflake account identifier, for example `ABC12345-XY98765` |
| `<YOUR_ROLE>` | Same role used in the token SQL |
| `<YOUR_CLIENT_SECRET_FROM_STEP_1>` | Secret returned when the PAT was created |

Save the `access_token` value from the response. DuckDB uses this token in the
Iceberg catalog secret.

## Install DuckDB Extensions

Open DuckDB and install the required extensions:

```sql
INSTALL iceberg;
LOAD iceberg;

INSTALL httpfs;
LOAD httpfs;

INSTALL azure;
LOAD azure;
```

## Add Optional Azure Storage Credentials

Skip this step for EvSnow's default Snowflake-managed internal Iceberg storage.
Use it only when your query workflow needs direct cloud-storage access for a
customer-managed external-volume table.

```sql
CREATE SECRET azure_auto (
  TYPE azure,
  PROVIDER credential_chain,
  ACCOUNT_NAME '<YOUR_STORAGE_ACCOUNT>'
);
```

The `credential_chain` provider uses local Azure CLI credentials from
`az login`.

## Create The Iceberg Catalog Secret

Create a DuckDB secret with the OAuth `access_token`:

```sql
CREATE OR REPLACE SECRET sf_horizon_token (
  TYPE iceberg,
  TOKEN '<YOUR_OAUTH_TOKEN_FROM_STEP_2>'
);
```

Use the `access_token` from the OAuth response, not the `client_secret` returned
by the PAT creation SQL.

## Attach The Snowflake Iceberg Catalog

Attach the Snowflake catalog to DuckDB:

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
| `<YOUR_ACCOUNT>` | Snowflake account identifier |
| `INGESTION` | Database name created by `setup_snowpipe_streaming.sql` |

## Query The Table

```sql
SELECT * FROM sf.PUBLIC.EVENTS_TABLE1;
```

Expected result: DuckDB returns rows from the Snowflake Iceberg table without
starting the EvSnow pipeline.
