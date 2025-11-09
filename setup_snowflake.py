#!/usr/bin/env python3
"""
Snowflake setup script using PAT authentication.

This script uses a PAT token to:
1. Assign RSA public key to STREAMEV user
2. Create INGESTION database and schema
3. Create CONTROL database and schema
"""

import sys
from pathlib import Path

try:
    import snowflake.connector
except ImportError:
    print("❌ Error: snowflake-connector-python not found")
    print("Install it with: uv add snowflake-connector-python")
    sys.exit(1)


def read_pat_config() -> dict[str, str]:
    """Read PAT configuration from snow.pat file."""
    pat_file = Path("snowflake/snow.pat")
    if not pat_file.exists():
        print(f"❌ Error: {pat_file} not found")
        sys.exit(1)

    config = {}
    with open(pat_file) as f:
        for line in f:
            line = line.strip()
            if "=" in line:
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip().strip('"')

    return config


def read_public_key() -> str:
    """Read the RSA public key value."""
    pub_key_file = Path("snowflake/rsa_key_pub_value.txt")
    if not pub_key_file.exists():
        print(f"❌ Error: {pub_key_file} not found")
        print("Run ./generate_snowflake_keys.sh first")
        sys.exit(1)

    with open(pub_key_file) as f:
        return f.read().strip()


def setup_snowflake():
    """Setup Snowflake using PAT authentication."""
    print("=" * 60)
    print("Snowflake Setup Script")
    print("=" * 60)
    print()

    # Read configuration
    print("📋 Reading configuration...")
    pat_config = read_pat_config()
    public_key = read_public_key()

    account = pat_config.get("account")
    user = pat_config.get("user")
    pat_token = pat_config.get("pat")

    if not all([account, user, pat_token]):
        print("❌ Error: Missing configuration in snow.pat")
        print("Required: account, user, pat")
        sys.exit(1)

    print(f"   Account: {account}")
    print(f"   User: {user}")
    print()

    # Connect to Snowflake
    print("🔌 Connecting to Snowflake...")
    try:
        # Try using PAT as password
        try:
            conn = snowflake.connector.connect(
                account=account,
                user=user,
                password=pat_token,
            )
            print("   ✅ Connected successfully with password!")
        except Exception as pwd_error:
            print(f"   ⚠️  Password auth failed: {pwd_error}")
            print("   Trying externalbrowser authenticator...")
            # Try externalbrowser
            conn = snowflake.connector.connect(
                account=account,
                user=user,
                authenticator="externalbrowser",
            )
            print("   ✅ Connected successfully with externalbrowser!")

        cursor = conn.cursor()
        print()
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        sys.exit(1)

    try:
        # Step 1: Assign RSA public key to user
        print(f"🔑 Assigning RSA public key to user {user}...")
        try:
            cursor.execute(f"ALTER USER {user} SET RSA_PUBLIC_KEY='{public_key}'")
            print(f"   ✅ Public key assigned to {user}")
        except Exception as e:
            print(f"   ❌ Failed to assign public key: {e}")
            print("   (You may need ACCOUNTADMIN role)")

        print()

        # Step 2: Create INGESTION database and schema
        print("📦 Creating INGESTION database and schema...")
        try:
            cursor.execute("CREATE DATABASE IF NOT EXISTS INGESTION")
            print("   ✅ Database INGESTION created/verified")

            cursor.execute("USE DATABASE INGESTION")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
            print("   ✅ Schema INGESTION.PUBLIC created/verified")
        except Exception as e:
            print(f"   ⚠️  Note: {e}")

        print()

        # Step 3: Create CONTROL database and schema
        print("📦 Creating CONTROL database and schema...")
        try:
            cursor.execute("CREATE DATABASE IF NOT EXISTS CONTROL")
            print("   ✅ Database CONTROL created/verified")

            cursor.execute("USE DATABASE CONTROL")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
            print("   ✅ Schema CONTROL.PUBLIC created/verified")
        except Exception as e:
            print(f"   ⚠️  Note: {e}")

        print()

        # Step 4: Create control table (INGESTION_STATUS)
        print("📊 Creating INGESTION_STATUS hybrid table...")
        try:
            cursor.execute("USE DATABASE CONTROL")
            cursor.execute("USE SCHEMA PUBLIC")

            create_table_sql = """
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
            )
            """
            cursor.execute(create_table_sql)
            print("   ✅ Table CONTROL.PUBLIC.INGESTION_STATUS created/verified")
        except Exception as e:
            print(f"   ⚠️  Note: {e}")

        print()

        # Step 5: Verify setup
        print("✅ Verifying setup...")

        # Check RSA public key
        cursor.execute(f"DESC USER {user}")
        user_desc = cursor.fetchall()
        has_key = any("RSA_PUBLIC_KEY_FP" in str(row) for row in user_desc)
        if has_key:
            print(f"   ✅ RSA public key is assigned to {user}")
        else:
            print(f"   ⚠️  Could not verify RSA public key for {user}")

        # Check databases
        cursor.execute("SHOW DATABASES LIKE 'INGESTION'")
        if cursor.fetchone():
            print("   ✅ Database INGESTION exists")

        cursor.execute("SHOW DATABASES LIKE 'CONTROL'")
        if cursor.fetchone():
            print("   ✅ Database CONTROL exists")

        # Check control table
        cursor.execute("SHOW TABLES LIKE 'INGESTION_STATUS' IN SCHEMA CONTROL.PUBLIC")
        if cursor.fetchone():
            print("   ✅ Table CONTROL.PUBLIC.INGESTION_STATUS exists")

        print()
        print("=" * 60)
        print("✅ Snowflake setup completed successfully!")
        print("=" * 60)
        print()
        print("Next steps:")
        print("1. Update your .env file with Snowflake configuration")
        print("2. Run: ./verify_snowflake_setup.sh")
        print("3. Run: evsnow validate-config")
        print()

    except Exception as e:
        print(f"❌ Error during setup: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    setup_snowflake()
