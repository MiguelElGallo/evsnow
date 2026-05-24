"""Tests for Postgres control table utilities."""

from unittest.mock import MagicMock, Mock

import pytest

from utils import postgres as pg
from utils.config_models import PostgresConnectionConfig


def test_get_connection_password_auth_uses_password(mocker):
    """Ensure password auth passes password to psycopg.connect."""
    dummy_conn = MagicMock()
    dummy_conn.closed = False

    mock_connect = mocker.patch("utils.postgres.psycopg.connect", return_value=dummy_conn)

    config = PostgresConnectionConfig(
        host="localhost",
        port=5432,
        user="pguser",
        password="secret",
        sslmode="require",
        auth_mode="password",
    )

    conn = pg.get_connection(config, "control_db", use_cache=False)

    assert conn is dummy_conn
    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args.kwargs
    assert call_kwargs["password"] == "secret"
    assert call_kwargs["dbname"] == "control_db"


def test_get_partition_checkpoints_returns_dict(monkeypatch):
    """Verify checkpoint query returns partition dict."""

    class DummyCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *args, **kwargs):
            return None

        def fetchall(self):
            return [("0", 123), ("1", 456)]

    class DummyConn:
        def cursor(self):
            return DummyCursor()

    monkeypatch.setattr(pg, "get_connection", lambda *args, **kwargs: DummyConn())

    result = pg.get_partition_checkpoints(
        eventhub_namespace="test.servicebus.windows.net",
        eventhub="test-hub",
        target_db="CONTROL",
        target_schema="public",
        target_table="INGESTION_STATUS",
        config=MagicMock(),
    )

    assert result == {
        "0": {"offset": "123", "sequence_number": 123},
        "1": {"offset": "456", "sequence_number": 456},
    }


def test_insert_partition_checkpoint_executes(monkeypatch):
    """Verify insert checkpoint executes statement."""
    executed = {}

    class DummyCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *args, **kwargs):
            executed["called"] = True

    class DummyConn:
        def cursor(self):
            return DummyCursor()

    monkeypatch.setattr(pg, "get_connection", lambda *args, **kwargs: DummyConn())

    pg.insert_partition_checkpoint(
        eventhub_namespace="test.servicebus.windows.net",
        eventhub="test-hub",
        target_db="SNOW_DB",
        target_schema="PUBLIC",
        target_table="EVENTS",
        partition_id="0",
        waterlevel=123,
        metadata={"offset": "123"},
        config=MagicMock(),
        control_db="CONTROL",
        control_schema="public",
        control_table="INGESTION_STATUS",
    )

    assert executed.get("called") is True


def test_ensure_postgres_ownership_table_uses_advisory_lock():
    """Ownership table DDL is serialized for concurrent fresh consumers."""
    cursor = MagicMock()

    pg._ensure_postgres_ownership_table(cursor, "public", "ingestion_status_ownership")

    assert cursor.execute.call_count == 3
    lock_query, lock_params = cursor.execute.call_args_list[0].args
    create_query = cursor.execute.call_args_list[1].args[0]
    unlock_query, unlock_params = cursor.execute.call_args_list[2].args

    assert lock_query == "SELECT pg_advisory_lock(%s)"
    assert unlock_query == "SELECT pg_advisory_unlock(%s)"
    assert lock_params == unlock_params
    assert isinstance(lock_params[0], int)
    assert "CREATE TABLE IF NOT EXISTS" in str(create_query)


def test_ensure_postgres_ownership_table_unlocks_after_ddl_failure():
    """The session advisory lock is released even if ownership DDL fails."""
    calls = []

    class DummyCursor:
        def execute(self, query, params=None):
            calls.append((query, params))
            if len(calls) == 2:
                raise RuntimeError("ddl failed")

    with pytest.raises(RuntimeError, match="ddl failed"):
        pg._ensure_postgres_ownership_table(
            DummyCursor(),
            "public",
            "ingestion_status_ownership",
        )

    assert len(calls) == 3
    assert calls[0][0] == "SELECT pg_advisory_lock(%s)"
    assert calls[2][0] == "SELECT pg_advisory_unlock(%s)"
    assert calls[0][1] == calls[2][1]


def test_get_connection_azure_token_auth_success(mocker):
    """Ensure azure_token auth retrieves token and passes it to psycopg.connect."""
    # Reset the global azure credential cache to ensure clean test
    pg._azure_credential = None

    dummy_conn = MagicMock()
    dummy_conn.closed = False

    mock_connect = mocker.patch("utils.postgres.psycopg.connect", return_value=dummy_conn)

    # Mock the Azure credential and token
    mock_token = Mock()
    mock_token.token = "mock-azure-token-12345"
    mock_token.expires_on = 1234567890

    mock_credential = Mock()
    mock_credential.get_token = Mock(return_value=mock_token)

    mock_default_azure_credential = mocker.patch(
        "utils.postgres.DefaultAzureCredential",
        return_value=mock_credential
    )

    config = PostgresConnectionConfig(
        host="localhost",
        port=5432,
        user="pguser",
        sslmode="require",
        auth_mode="azure_token",
    )

    conn = pg.get_connection(config, "control_db", use_cache=False)

    assert conn is dummy_conn
    mock_default_azure_credential.assert_called_once()
    mock_credential.get_token.assert_called_once_with(
        "https://ossrdbms-aad.database.windows.net/.default"
    )
    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args.kwargs
    assert call_kwargs["password"] == "mock-azure-token-12345"
    assert call_kwargs["dbname"] == "control_db"


def test_get_connection_azure_token_auth_failure(mocker):
    """Ensure azure_token auth raises exception when token acquisition fails."""
    # Reset the global azure credential cache to ensure clean test
    pg._azure_credential = None

    # Mock psycopg.connect so we don't make real connection attempts
    mocker.patch("utils.postgres.psycopg.connect")

    mock_credential = Mock()
    mock_credential.get_token = Mock(
        side_effect=Exception("Failed to acquire Azure token")
    )

    mocker.patch(
        "utils.postgres.DefaultAzureCredential",
        return_value=mock_credential
    )

    config = PostgresConnectionConfig(
        host="localhost",
        port=5432,
        user="pguser",
        sslmode="require",
        auth_mode="azure_token",
    )

    # Should raise an exception when trying to get connection
    with pytest.raises(Exception) as exc_info:
        pg.get_connection(config, "control_db", use_cache=False)

    assert "Failed to acquire Azure token" in str(exc_info.value)
    mock_credential.get_token.assert_called_once_with(
        "https://ossrdbms-aad.database.windows.net/.default"
    )
