"""Static guards for the default Snowflake-managed Iceberg setup."""

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SETUP_SQL = REPO_ROOT / "setup_snowpipe_streaming.sql"
SETUP_GRANTS_SQL = REPO_ROOT / "setup_snowflake.sql"
MESSAGES_CREATION_SQL = REPO_ROOT / "messages" / "creation.sql"
QUICKSTART = REPO_ROOT / "docs" / "getting-started" / "snowflake-quickstart.md"
EVENTHUB_QUICKSTART = REPO_ROOT / "docs" / "getting-started" / "event-hub-quickstart.md"
COMPLETE_SETUP = REPO_ROOT / "docs" / "snowflake" / "complete-setup.md"
PARAMETERS = REPO_ROOT / "docs" / "reference" / "parameters.md"
QUICKSTART_HARNESS = REPO_ROOT / "tools" / "quickstart_harness.py"
ENV_EXAMPLE = REPO_ROOT / ".env.example"
CONFIGURATION = REPO_ROOT / "docs" / "configuration.md"
ZENSICAL = REPO_ROOT / "zensical.toml"
FIRST_RUN = REPO_ROOT / "docs" / "tutorial" / "first-run.md"
EVENTHUB_SENDER = REPO_ROOT / "docs" / "tools" / "eventhub-sender.md"
WORKFLOWS = REPO_ROOT / "docs" / "project" / "workflows.md"
DOCS_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "docs.yml"


def _normalized(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").upper().split())


def _load_quickstart_harness():
    spec = importlib.util.spec_from_file_location("quickstart_harness", QUICKSTART_HARNESS)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


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

    assert "TYPE = SERVICE" in grants
    assert "ALTER USER STREAMEV SET TYPE = SERVICE" in grants
    assert "GRANT CREATE SCHEMA ON DATABASE CONTROL TO ROLE STREAM" in grants
    assert "GRANT CREATE TABLE ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM" in grants
    assert "GRANT CREATE ICEBERG TABLE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM" in grants
    assert "GRANT CREATE PIPE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM" in grants
    assert "GRANT CREATE SCHEMA ON DATABASE CONTROL TO ROLE STREAM" in docs
    assert "GRANT CREATE TABLE ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM" in docs
    assert "GRANT CREATE ICEBERG TABLE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM" in docs
    assert "GRANT CREATE PIPE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM" in docs
    assert "TYPE = SERVICE" in docs
    assert "WARNING: COULD NOT VERIFY SNOWFLAKE CONTROL TABLE" in docs
    assert "NEXT: RUN SETUP_SNOWPIPE_STREAMING.SQL, UPDATE .ENV" in grants


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


def test_eventhub_quickstart_covers_creation_and_rbac():
    docs = "\n".join(_normalized(path) for path in (EVENTHUB_QUICKSTART, FIRST_RUN))
    config = _normalized(ZENSICAL)

    assert "EVENT HUB QUICKSTART" in docs
    assert "AZ EVENTHUBS NAMESPACE CREATE" in docs
    assert "AZ EVENTHUBS EVENTHUB CREATE" in docs
    assert "AZURE EVENT HUBS DATA RECEIVER" in docs
    assert "AZURE EVENT HUBS DATA SENDER" in docs
    assert "EVENTHUB-RBAC-SMOKE" in docs
    assert "FULLY QUALIFIED NAMESPACE" in docs
    assert "GETTING-STARTED/EVENT-HUB-QUICKSTART.MD" in config


def test_env_example_keeps_first_run_shape_in_toml():
    active_keys = {
        line.split("=", 1)[0]
        for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#") and "=" in line
    }

    shape_keys = {
        "EVENTHUB_NAMESPACE",
        "EVENTHUBNAME_1",
        "TARGET_DB",
        "TARGET_SCHEMA",
        "TARGET_TABLE",
        "CONTROL_TABLE_BACKEND",
        "CONTROL_OWNERSHIP_MODE",
        "SNOWFLAKE_1_DATABASE",
        "SNOWFLAKE_1_SCHEMA",
        "SNOWFLAKE_1_TABLE",
    }

    assert active_keys.isdisjoint(shape_keys)


def test_docs_explain_default_and_explicit_env_precedence():
    docs = "\n".join(_normalized(path) for path in (CONFIGURATION, PARAMETERS))

    assert "DEFAULT .ENV" in docs
    assert "DOES NOT REPLACE VARIABLES ALREADY" in docs
    assert "EXPLICIT --ENV-FILE" in docs
    assert "OVERRIDE SEMANTICS" in docs


def test_zensical_enables_code_copy_buttons():
    config = ZENSICAL.read_text(encoding="utf-8")

    assert '"content.code.copy"' in config
    assert '"search.highlight"' in config
    assert "[project.markdown_extensions.attr_list]" in config
    assert 'name = "mermaid"' in config


def test_first_run_includes_arrival_proof():
    docs = _normalized(FIRST_RUN)

    assert "RUN_ID=\"EVSNOW-FIRST-RUN-" in docs
    assert "ROWS_ARRIVED" in docs
    assert "SEQUENCE_IDS" in docs
    assert "WAIT 15 SECONDS AND RERUN" in docs
    assert "ROWS_ARRIVED = 3" in docs


def test_eventhub_sender_env_only_docs_match_cli_defaults():
    docs = _normalized(EVENTHUB_SENDER)

    assert "EVENTHUB_NAME` IS ONLY A LOCAL SHELL VARIABLE" in docs
    assert "READS `EVENTHUBNAME_1` FROM `.ENV`" in docs


def test_docs_workflow_deploys_built_artifact_once():
    workflow = DOCS_WORKFLOW.read_text(encoding="utf-8")
    docs = _normalized(WORKFLOWS)

    assert "Upload GitHub Pages artifact" in workflow
    assert "if: github.event_name == 'push'" in workflow
    assert workflow.count("uv run zensical build --clean --strict") == 1
    assert "DEPLOY JOB PUBLISHES THE ARTIFACT FROM THE BUILD JOB" in docs
    assert "CODSPEED" in docs


def test_quickstart_harness_fails_on_validation_warnings():
    harness = QUICKSTART_HARNESS.read_text(encoding="utf-8")

    assert 'os.environ.get("EVSNOW_QUICKSTART_CONNECTION", "default")' in harness
    assert "failure_patterns" in harness
    assert "Configuration has errors" in harness
    assert "Warnings:" in harness
    assert "⚠" in harness
    assert "Warning: Could not verify Snowflake control table" in harness
    assert "Insufficient privileges" in harness
    assert "SNOWFLAKE_DATABASE=INGESTION" not in harness
    assert "SNOWFLAKE_SCHEMA_NAME=PUBLIC" not in harness
    assert "database/schema are derived from config/evsnow.toml" in harness


def test_quickstart_harness_resolves_cli_default_connection():
    harness_module = _load_quickstart_harness()

    selected = harness_module.Harness.resolve_connection_item(
        [
            {
                "connection_name": "automa_bagwcin_os33166",
                "is_default": True,
                "parameters": {"account": "BAGWCIN-OS33166"},
            }
        ],
        "default",
    )

    assert selected["connection_name"] == "automa_bagwcin_os33166"


def test_quickstart_harness_marks_failure_patterns(tmp_path):
    harness_module = _load_quickstart_harness()
    harness = harness_module.Harness(connection="default", run_root=tmp_path, keep_going=True)
    harness.run_dir.mkdir(parents=True)

    result = harness.run(
        "warning command",
        [
            sys.executable,
            "-c",
            "print('Warning: Could not verify Snowflake control table')",
        ],
        cwd=tmp_path,
        failure_patterns=["Warning: Could not verify Snowflake control table"],
    )

    assert result.exit_code == 1
    assert harness.failed is True
    assert "Harness detected failure pattern" in result.stderr
