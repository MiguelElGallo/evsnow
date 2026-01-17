"""Unit tests for CLI bootstrap helpers.

These tests focus on increasing coverage for `utils.cli_bootstrap` without
requiring real Logfire or dotenv behavior.
"""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace


def test_load_dotenv_if_present_uses_cwd_dotenv(tmp_path, monkeypatch):
    """Loads `.env` from CWD when present."""
    from utils import cli_bootstrap

    called: dict[str, Path] = {}

    def fake_load_dotenv(path: Path) -> None:
        called["path"] = Path(path)

    monkeypatch.setitem(
        __import__("sys").modules,
        "dotenv",
        SimpleNamespace(load_dotenv=fake_load_dotenv),
    )

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("FOO=bar\n")

    cli_bootstrap.load_dotenv_if_present(caller_file=str(tmp_path / "x.py"))

    assert called["path"] == tmp_path / ".env"


def test_load_dotenv_if_present_falls_back_to_project_root(tmp_path, monkeypatch):
    """Falls back to inferred project root when CWD has no `.env`."""
    from utils import cli_bootstrap

    called: dict[str, Path] = {}

    def fake_load_dotenv(path: Path) -> None:
        called["path"] = Path(path)

    monkeypatch.setitem(
        __import__("sys").modules,
        "dotenv",
        SimpleNamespace(load_dotenv=fake_load_dotenv),
    )

    # Create fake caller file at: <root>/a/b/c/caller.py
    root = tmp_path / "root"
    caller_file = root / "a" / "b" / "c" / "caller.py"
    caller_file.parent.mkdir(parents=True)
    caller_file.write_text("# caller\n")

    # `.env` exists at inferred root (<caller>.parent.parent.parent == <root>/a)
    inferred_root = caller_file.parent.parent.parent
    (inferred_root / ".env").write_text("FOO=baz\n")

    # Ensure CWD has no `.env`
    other = tmp_path / "other"
    other.mkdir()
    monkeypatch.chdir(other)

    cli_bootstrap.load_dotenv_if_present(caller_file=str(caller_file))

    assert called["path"] == inferred_root / ".env"


def test_configure_early_logfire_suppresses_config_error(monkeypatch):
    """Does not raise if Logfire is not configured."""
    from utils import cli_bootstrap

    def raise_config_error(*_args, **_kwargs):
        raise cli_bootstrap.LogfireConfigError("not configured")

    monkeypatch.setattr(cli_bootstrap.logfire, "configure", raise_config_error)

    # Should not raise
    cli_bootstrap.configure_early_logfire()


def test_initialize_logfire_disabled_logs_and_returns(caplog, monkeypatch):
    """When disabled, it should log and not call logfire.configure."""
    from utils import cli_bootstrap

    caplog.set_level(logging.INFO)

    mock_configure_calls: list[tuple[tuple, dict]] = []

    def fake_configure(*args, **kwargs):
        mock_configure_calls.append((args, kwargs))

    monkeypatch.setattr(cli_bootstrap.logfire, "configure", fake_configure)

    config = SimpleNamespace(enabled=False)
    logger = logging.getLogger("test")

    cli_bootstrap.initialize_logfire(logfire_config=config, logger=logger)

    assert "Logfire observability disabled" in caplog.text
    assert mock_configure_calls == []


def test_initialize_logfire_enabled_configures_without_console(monkeypatch):
    """When enabled, it should call logfire.configure (console_logging=False path)."""
    from utils import cli_bootstrap

    configure_calls: list[dict] = []

    def fake_configure(**kwargs):
        configure_calls.append(kwargs)

    monkeypatch.setattr(cli_bootstrap.logfire, "configure", fake_configure)

    # Avoid adding a real handler instance
    monkeypatch.setattr(cli_bootstrap.logfire, "LogfireLoggingHandler", logging.NullHandler)

    config = SimpleNamespace(
        enabled=True,
        token="test-token",
        service_name="evsnow-test",
        environment="test",
        send_to_logfire=True,
        console_logging=False,
        log_level="INFO",
    )

    logger = logging.getLogger("test")

    cli_bootstrap.initialize_logfire(logfire_config=config, logger=logger)

    assert configure_calls
    assert configure_calls[0]["service_name"] == "evsnow-test"
    assert configure_calls[0]["send_to_logfire"] is True
    assert configure_calls[0]["console"] is False


def test_load_dotenv_if_present_ignores_missing_dotenv(monkeypatch, tmp_path):
    """Missing python-dotenv should not raise."""
    from utils import cli_bootstrap
    import builtins

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "dotenv":
            raise ImportError("missing")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    monkeypatch.chdir(tmp_path)

    cli_bootstrap.load_dotenv_if_present(caller_file=str(tmp_path / "caller.py"))


def test_initialize_logfire_enabled_with_console_adds_handler(monkeypatch):
    """When console logging is enabled, it should add a Logfire logging handler."""
    from utils import cli_bootstrap

    configure_calls: list[dict] = []

    def fake_configure(**kwargs):
        configure_calls.append(kwargs)

    console_calls: list[dict] = []

    def fake_console_options(**kwargs):
        console_calls.append(kwargs)
        return {"console": True}

    class DummyHandler(logging.Handler):
        def emit(self, record):
            return None

    monkeypatch.setattr(cli_bootstrap.logfire, "configure", fake_configure)
    monkeypatch.setattr(cli_bootstrap.logfire, "ConsoleOptions", fake_console_options)
    monkeypatch.setattr(cli_bootstrap.logfire, "LogfireLoggingHandler", DummyHandler)

    config = SimpleNamespace(
        enabled=True,
        token="test-token",
        service_name="evsnow-test",
        environment="test",
        send_to_logfire=False,
        console_logging=True,
        log_level="INFO",
    )

    logger = logging.getLogger("test")
    root_logger = logging.getLogger()
    before_handlers = len(root_logger.handlers)

    cli_bootstrap.initialize_logfire(logfire_config=config, logger=logger)

    assert configure_calls
    assert console_calls
    assert configure_calls[0]["console"] == {"console": True}
    assert len(root_logger.handlers) == before_handlers + 1

    for handler in list(root_logger.handlers):
        if isinstance(handler, DummyHandler):
            root_logger.removeHandler(handler)
