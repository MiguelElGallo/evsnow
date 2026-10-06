"""Channel settings validation, source precedence and generated schema."""

import json
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from utils.config import SnowflakeConnectionConfig, load_config
from utils.config_file import EvSnowFileConfig


@pytest.fixture(autouse=True)
def isolated_settings(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for name in list(os.environ):
        if name.startswith(("SNOWFLAKE_", "EVENTHUB", "CONTROL", "TARGET")):
            monkeypatch.delenv(name, raising=False)


def connection_with(sample_snowflake_connection_config, **settings):
    return SnowflakeConnectionConfig(**(sample_snowflake_connection_config.model_dump() | settings))


def test_named_and_positive_waits_are_defaults(sample_snowflake_connection_config):
    values = sample_snowflake_connection_config.model_dump()
    for field in ("channel_mode", "ack_timeout_seconds", "close_timeout_seconds"):
        values.pop(field, None)
    connection = SnowflakeConnectionConfig(**values)
    assert connection.channel_mode == "named"
    assert connection.ack_timeout_seconds == 60
    assert connection.close_timeout_seconds == 60


@pytest.mark.parametrize("value", ["elastic", " ELASTIC ", "Elastic"])
def test_channel_mode_is_case_insensitive(sample_snowflake_connection_config, value):
    assert (
        connection_with(sample_snowflake_connection_config, channel_mode=value).channel_mode
        == "elastic"
    )


@pytest.mark.parametrize(
    "field,value",
    [("channel_mode", "legacy"), ("ack_timeout_seconds", 0), ("close_timeout_seconds", -1)],
)
def test_invalid_channel_settings_are_rejected(sample_snowflake_connection_config, field, value):
    with pytest.raises(ValidationError):
        connection_with(sample_snowflake_connection_config, **{field: value})


def toml_with_connection(tmp_path, connection):
    config_file = tmp_path / "evsnow.toml"
    lines = ['eventhub_namespace = "test.servicebus.windows.net"', "[snowflake_connection]"]
    for field, value in connection.model_dump().items():
        if value is not None:
            lines.append(f"{field} = {json.dumps(value)}")
    config_file.write_text("\n".join(lines), encoding="utf-8")
    return config_file


def test_environment_overrides_complete_toml_settings(
    sample_snowflake_connection_config, monkeypatch, tmp_path
):
    toml_connection = connection_with(
        sample_snowflake_connection_config,
        channel_mode="named",
        ack_timeout_seconds=17,
        close_timeout_seconds=18,
    )
    config_file = toml_with_connection(tmp_path, toml_connection)
    monkeypatch.setenv("SNOWFLAKE_CHANNEL_MODE", "elastic")
    monkeypatch.setenv("SNOWFLAKE_ACK_TIMEOUT_SECONDS", "31")
    monkeypatch.setenv("SNOWFLAKE_CLOSE_TIMEOUT_SECONDS", "32")
    config = load_config(config_file=config_file)
    assert config.snowflake_connection is not None
    assert config.snowflake_connection.channel_mode == "elastic"
    assert config.snowflake_connection.ack_timeout_seconds == 31
    assert config.snowflake_connection.close_timeout_seconds == 32


def test_toml_settings_apply_without_env_override(sample_snowflake_connection_config, tmp_path):
    config_file = toml_with_connection(
        tmp_path,
        connection_with(
            sample_snowflake_connection_config, channel_mode="elastic", ack_timeout_seconds=23
        ),
    )
    config = load_config(config_file=config_file)
    assert config.snowflake_connection is not None
    assert config.snowflake_connection.channel_mode == "elastic"
    assert config.snowflake_connection.ack_timeout_seconds == 23


def test_environment_only_settings_are_loaded(sample_snowflake_connection_config, monkeypatch):
    for field, value in sample_snowflake_connection_config.model_dump().items():
        if value is not None:
            monkeypatch.setenv(f"SNOWFLAKE_{field.upper()}", str(value))
    monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
    monkeypatch.setenv("SNOWFLAKE_CHANNEL_MODE", "elastic")
    monkeypatch.setenv("SNOWFLAKE_ACK_TIMEOUT_SECONDS", "21")
    config = load_config()
    assert config.snowflake_connection is not None
    assert config.snowflake_connection.channel_mode == "elastic"
    assert config.snowflake_connection.ack_timeout_seconds == 21


@pytest.mark.parametrize(
    "field,value",
    [("CHANNEL_MODE", "wrong"), ("ACK_TIMEOUT_SECONDS", "0"), ("CLOSE_TIMEOUT_SECONDS", "-1")],
)
def test_invalid_explicit_env_is_not_swallowed_as_optional_connection(monkeypatch, field, value):
    monkeypatch.setenv("EVENTHUB_NAMESPACE", "test.servicebus.windows.net")
    monkeypatch.setenv(f"SNOWFLAKE_{field}", value)
    with pytest.raises(ValidationError):
        load_config()


def test_generated_schema_has_modes_and_positive_waits():
    schema_path = Path(__file__).resolve().parents[2] / "schemas" / "evsnow.schema.json"
    checked_in = json.loads(schema_path.read_text(encoding="utf-8"))
    generated = EvSnowFileConfig.model_json_schema()
    assert checked_in == generated
    properties = checked_in["$defs"]["SnowflakeConnectionConfig"]["properties"]
    assert properties["channel_mode"]["enum"] == ["named", "elastic"]
    assert properties["channel_mode"]["default"] == "named"
    assert properties["ack_timeout_seconds"]["exclusiveMinimum"] == 0
    assert properties["close_timeout_seconds"]["exclusiveMinimum"] == 0
