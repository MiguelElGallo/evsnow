"""Unit tests for the `evsnow` package entrypoints.

These are tiny modules but should be covered to avoid 0% coverage.
"""

from __future__ import annotations


def test_evsnow_init_exposes_version_and_main(capsys):
    import evsnow

    assert isinstance(evsnow.__version__, str)

    evsnow.main()
    out = capsys.readouterr().out
    assert "Hello from evsnow!" in out


def test_evsnow_module_main_delegates_to_cli(monkeypatch):
    import evsnow.__main__ as evsnow_main

    called = {"count": 0}

    def fake_cli_main() -> None:
        called["count"] += 1

    monkeypatch.setattr(evsnow_main, "cli_main", fake_cli_main)

    evsnow_main.main()

    assert called["count"] == 1
