#!/usr/bin/env python3
"""Run the Snowflake quickstart with command/output logging.

The harness intentionally runs in a scratch copy of the repository so generated
keys, config files, and .env content do not modify the working tree.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONNECTION = os.environ.get("EVSNOW_QUICKSTART_CONNECTION", "default")
DEFAULT_RUN_ROOT = REPO_ROOT / ".quickstart-runs"


@dataclass
class CommandResult:
    name: str
    command: str
    cwd: str
    exit_code: int
    stdout: str
    stderr: str
    started_at: str
    ended_at: str


class Harness:
    def __init__(self, *, connection: str, run_root: Path, keep_going: bool) -> None:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.run_dir = run_root / timestamp
        self.workspace = self.run_dir / "workspace"
        self.log_path = self.run_dir / "commands.jsonl"
        self.summary_path = self.run_dir / "summary.json"
        self.connection = connection
        self.keep_going = keep_going
        self.redactions: dict[str, str] = {}
        self.results: list[CommandResult] = []
        self.failed = False

    def prepare(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=False)
        shutil.copytree(
            REPO_ROOT,
            self.workspace,
            ignore=shutil.ignore_patterns(
                ".git",
                ".venv",
                "site",
                ".quickstart-runs",
                ".cache",
                ".pytest_cache",
                ".ruff_cache",
                "__pycache__",
                "snowflake",
            ),
        )

    def redact(self, value: str) -> str:
        redacted = value
        for secret, replacement in self.redactions.items():
            if secret:
                redacted = redacted.replace(secret, replacement)
        return redacted

    def record(self, result: CommandResult) -> None:
        self.results.append(result)
        self.write_log()

    def write_log(self) -> None:
        with self.log_path.open("w", encoding="utf-8") as handle:
            for result in self.results:
                payload = self.result_payload(result)
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def record_existing_redactions(self) -> None:
        if self.results:
            self.write_log()

    def result_payload(self, result: CommandResult) -> dict[str, Any]:
        payload = {
            "name": result.name,
            "command": self.redact(result.command),
            "cwd": result.cwd,
            "exit_code": result.exit_code,
            "stdout": self.redact(result.stdout),
            "stderr": self.redact(result.stderr),
            "started_at": result.started_at,
            "ended_at": result.ended_at,
        }
        return payload

    def run(
        self,
        name: str,
        command: list[str],
        *,
        cwd: Path | None = None,
        stdin: str | None = None,
        display_command: str | None = None,
        env: dict[str, str] | None = None,
        failure_patterns: list[str] | None = None,
        timeout: int = 180,
    ) -> CommandResult:
        cwd = cwd or self.workspace
        started_at = datetime.now(UTC).isoformat()
        completed = subprocess.run(
            command,
            cwd=cwd,
            input=stdin,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, **(env or {})},
            check=False,
        )
        ended_at = datetime.now(UTC).isoformat()
        exit_code = completed.returncode
        stdout = completed.stdout
        stderr = completed.stderr
        for pattern in failure_patterns or []:
            if pattern in stdout or pattern in stderr:
                exit_code = 1
                stderr = (
                    f"{stderr.rstrip()}\n"
                    f"Harness detected failure pattern in command output: {pattern}\n"
                ).lstrip()
                break

        result = CommandResult(
            name=name,
            command=display_command or " ".join(command),
            cwd=str(cwd),
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            started_at=started_at,
            ended_at=ended_at,
        )
        self.record(result)
        if exit_code != 0:
            self.failed = True
            if not self.keep_going:
                self.write_summary()
                raise SystemExit(exit_code)
        return result

    def note(self, name: str, message: str) -> None:
        now = datetime.now(UTC).isoformat()
        self.record(
            CommandResult(
                name=name,
                command="harness note",
                cwd=str(self.workspace),
                exit_code=0,
                stdout=message,
                stderr="",
                started_at=now,
                ended_at=now,
            )
        )

    def connection_account(self) -> str:
        result = self.run(
            "read Snowflake CLI connection metadata",
            ["snow", "connection", "list", "--format", "json"],
            display_command="snow connection list --format json",
        )
        connections = json.loads(result.stdout)
        item = self.resolve_connection_item(connections, self.connection)
        if item:
            connection_name = item.get("connection_name")
            if self.connection == "default" and connection_name:
                self.connection = connection_name
                self.note(
                    "resolve default Snowflake connection",
                    f"Using Snowflake CLI default connection: {connection_name}.",
                )
            account = item.get("parameters", {}).get("account")
            if account:
                return account
        raise SystemExit(f"Connection not found or missing account: {self.connection}")

    @staticmethod
    def resolve_connection_item(
        connections: list[dict[str, Any]], connection: str
    ) -> dict[str, Any] | None:
        if connection == "default":
            return next((item for item in connections if item.get("is_default")), None)
        return next(
            (item for item in connections if item.get("connection_name") == connection),
            None,
        )

    def render_sql(self, public_key: str) -> Path:
        rendered_dir = self.workspace / ".quickstart"
        rendered_dir.mkdir(exist_ok=True)
        rendered = rendered_dir / "setup_snowflake.rendered.sql"
        source = self.workspace / "setup_snowflake.sql"
        rendered.write_text(
            source.read_text(encoding="utf-8").replace("<PUBLIC_KEY_VALUE>", public_key),
            encoding="utf-8",
        )
        self.note(
            "render setup_snowflake.sql",
            "Rendered setup_snowflake.sql with the generated public key.",
        )
        return rendered

    def write_env(self, *, account: str, password: str) -> None:
        env_path = self.workspace / ".env"
        key_path = self.workspace / "snowflake" / "rsa_key_encrypted.p8"
        env_path.write_text(
            "\n".join(
                [
                    f"SNOWFLAKE_ACCOUNT={account}",
                    "SNOWFLAKE_USER=STREAMEV",
                    f"SNOWFLAKE_PRIVATE_KEY_FILE={key_path}",
                    f"SNOWFLAKE_PRIVATE_KEY_PASSWORD={password}",
                    "SNOWFLAKE_WAREHOUSE=COMPUTE_WH",
                    "SNOWFLAKE_ROLE=STREAM",
                    "SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        self.note(
            "create .env",
            (
                "Created .env with generated key path and quickstart defaults. "
                "The Snowflake session database/schema are derived from config/evsnow.toml."
            ),
        )

    def write_summary(self) -> None:
        first_failure = next((item for item in self.results if item.exit_code != 0), None)
        summary: dict[str, Any] = {
            "run_dir": str(self.run_dir),
            "workspace": str(self.workspace),
            "connection": self.connection,
            "status": "failed" if self.failed else "passed",
            "first_failure": None
            if first_failure is None
            else {
                "name": first_failure.name,
                "command": self.redact(first_failure.command),
                "exit_code": first_failure.exit_code,
            },
            "commands_log": str(self.log_path),
        }
        self.summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    def execute_quickstart(self) -> None:
        self.prepare()
        password = secrets.token_urlsafe(24)
        self.redactions[password] = "<KEY_PASSWORD>"

        account = self.connection_account()
        self.run(
            "test setup connection",
            ["snow", "connection", "test", "--connection", self.connection, "--format", "json"],
            display_command=f"snow connection test --connection {self.connection} --format json",
        )
        self.run(
            "generate RSA key",
            ["./generate_snowflake_keys.sh"],
            display_command="EVSNOW_KEY_PASSWORD=<KEY_PASSWORD> ./generate_snowflake_keys.sh",
            env={"EVSNOW_KEY_PASSWORD": password},
            timeout=120,
        )
        public_key_path = self.workspace / "snowflake" / "rsa_key_pub_value.txt"
        public_key = public_key_path.read_text(encoding="utf-8").strip()
        self.redactions[public_key] = "<PUBLIC_KEY_VALUE>"
        self.record_existing_redactions()
        self.note("read public key", "Read snowflake/rsa_key_pub_value.txt.")

        rendered_setup = self.render_sql(public_key)
        self.run(
            "run setup_snowflake.sql",
            [
                "snow",
                "sql",
                "--connection",
                self.connection,
                "--format",
                "json",
                "--filename",
                str(rendered_setup),
            ],
            display_command=(
                "snow sql --connection "
                f"{self.connection} --format json --filename .quickstart/setup_snowflake.rendered.sql"
            ),
        )
        self.run(
            "run setup_snowpipe_streaming.sql",
            [
                "snow",
                "sql",
                "--connection",
                self.connection,
                "--format",
                "json",
                "--filename",
                "setup_snowpipe_streaming.sql",
            ],
            display_command=(
                "snow sql --connection "
                f"{self.connection} --format json --filename setup_snowpipe_streaming.sql"
            ),
        )
        self.run(
            "copy example config",
            ["cp", "config/evsnow.example.toml", "config/evsnow.toml"],
            display_command="cp config/evsnow.example.toml config/evsnow.toml",
        )
        self.write_env(account=account, password=password)
        self.run(
            "validate EvSnow config",
            [
                "uv",
                "run",
                "evsnow",
                "validate-config",
                "--config-file",
                "config/evsnow.toml",
                "--env-file",
                ".env",
            ],
            stdin="n\n",
            display_command=(
                "printf 'n\\n' | uv run evsnow validate-config "
                "--config-file config/evsnow.toml --env-file .env"
            ),
            failure_patterns=[
                "Configuration has errors",
                "Warnings:",
                "⚠",
                "Warning: Could not verify Snowflake control table",
                "Insufficient privileges",
                "ERROR",
                "✗",
            ],
            timeout=240,
        )
        self.write_summary()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection", default=DEFAULT_CONNECTION)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Continue after failures so the log captures later command behavior.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    harness = Harness(
        connection=args.connection,
        run_root=args.run_root,
        keep_going=args.keep_going,
    )
    try:
        harness.execute_quickstart()
    finally:
        harness.write_summary()
        print(f"summary: {harness.summary_path}")
        print(f"commands: {harness.log_path}")
    return 1 if harness.failed else 0


if __name__ == "__main__":
    sys.exit(main())
