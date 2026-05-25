"""Static guards for the default Snowflake-managed Iceberg setup."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SETUP_SQL = REPO_ROOT / "setup_snowpipe_streaming.sql"
SETUP_GRANTS_SQL = REPO_ROOT / "setup_snowflake.sql"
MESSAGES_CREATION_SQL = REPO_ROOT / "messages" / "creation.sql"
QUICKSTART = REPO_ROOT / "docs" / "getting-started" / "snowflake-quickstart.md"
COMPLETE_SETUP = REPO_ROOT / "docs" / "snowflake" / "complete-setup.md"
PARAMETERS = REPO_ROOT / "docs" / "reference" / "parameters.md"


def _normalized(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").upper().split())


def test_streaming_setup_uses_internal_snowflake_managed_iceberg():
    sql = _normalized(SETUP_SQL)

    assert "CREATE ICEBERG TABLE IF NOT EXISTS INGESTION.PUBLIC.EVENTS_TABLE1" in sql
    assert "CATALOG = SNOWFLAKE" in sql
    assert "EXTERNAL_VOLUME = SNOWFLAKE_MANAGED" in sql
    assert "ICEBERG_VERSION = 3" in sql
    assert "CREATE PIPE IF NOT EXISTS INGESTION.PUBLIC.EVENTS_TABLE_PIPE" in sql
    assert "INGESTION_TIMESTAMP" in sql
    assert "CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(6) AS INGESTION_TIMESTAMP" in sql
    assert "DATA_SOURCE(TYPE => 'STREAMING')" in sql


def test_streaming_setup_default_does_not_require_external_volume():
    sql = _normalized(SETUP_SQL)

    assert "EXTERNAL_VOLUME = 'EXVOL'" not in sql
    assert "CREATE OR REPLACE EXTERNAL VOLUME" not in sql
    assert "SYSTEM$VERIFY_EXTERNAL_VOLUME" not in sql
    assert "GRANT USAGE ON EXTERNAL VOLUME" not in sql
    assert "BASE_LOCATION" not in sql
    assert "INGESTION_TIMESTAMP TIMESTAMP_LTZ(6) DEFAULT" not in sql
    assert "PARTITION BY" not in sql
    assert "CLUSTER BY" not in sql


def test_default_docs_do_not_require_exvol():
    docs = "\n".join(_normalized(path) for path in (QUICKSTART, COMPLETE_SETUP))

    assert "REQUIRES AN EXISTING EXTERNAL VOLUME" not in docs
    assert "EXTERNAL_VOLUME = 'EXVOL'" not in docs
    assert "GRANT USAGE ON EXTERNAL VOLUME EXVOL" not in docs
    assert "CREATE OR REPLACE EXTERNAL VOLUME EXVOL" not in docs
    assert "EXTERNAL_VOLUME = SNOWFLAKE_MANAGED" in docs


def test_setup_grants_include_iceberg_and_pipe_creation():
    grants = _normalized(SETUP_GRANTS_SQL)
    docs = "\n".join(_normalized(path) for path in (QUICKSTART, COMPLETE_SETUP))

    assert "GRANT CREATE ICEBERG TABLE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM" in grants
    assert "GRANT CREATE PIPE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM" in grants
    assert "GRANT CREATE ICEBERG TABLE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM" in docs
    assert "GRANT CREATE PIPE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM" in docs


def test_sample_message_creation_sql_uses_internal_storage():
    sql = _normalized(MESSAGES_CREATION_SQL)

    assert "CREATE OR REPLACE ICEBERG TABLE INGESTION.PUBLIC.EVENTS_TABLE" in sql
    assert "CATALOG = SNOWFLAKE" in sql
    assert "EXTERNAL_VOLUME = SNOWFLAKE_MANAGED" in sql
    assert "ICEBERG_VERSION = 3" in sql
    assert "BASE_LOCATION" not in sql
    assert "EXTERNAL_VOLUME = 'MY_EXT_VOLUME'" not in sql
    assert "VARCHAR(" not in sql


def test_parameter_reference_covers_config_surfaces():
    docs = _normalized(PARAMETERS)

    required_terms = [
        "EVENTHUB_NAMESPACE",
        "EVENTHUBNAME_{N}_CONSUMER_GROUP",
        "EVENTHUB_CREDENTIAL_MODE",
        "EVENTHUBNAME_{N}_USE_CONNECTION_STRING",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_SCHEMA_NAME",
        "SNOWFLAKE_{N}_SCHEMA",
        "CONTROL_TABLE_BACKEND",
        "CONTROL_PG_AUTH_MODE",
        "LOGFIRE_LOG_LEVEL",
        "SMART_RETRY_LLM_PROVIDER",
        "VALIDATE-CONFIG",
        "--SHOW-RBAC",
        "--CAPTURE",
    ]

    for term in required_terms:
        assert term in docs
