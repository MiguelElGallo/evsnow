"""Offline SDK migration checks; no Copilot runtime or model is started."""

import inspect
import unittest
from collections.abc import Callable
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import agent
from copilot import CopilotClient, PermissionNoResult, UserInputRequest
from copilot.generated.rpc import (
    PermissionDecisionApproveOnce,
    PermissionDecisionReject,
    PermissionDecisionUserNotAvailable,
)
from copilot.generated.session_events import PermissionRequestMcp, PermissionRequestRead
from rich.console import Console
from typer.testing import CliRunner

from main import app


class SessionStub:
    def __init__(self, *, fail_send: bool = False, session_error: str | None = None) -> None:
        self.fail_send = fail_send
        self.session_error = session_error
        self.handler: Callable[[Any], None] | None = None
        self.prompt = ""
        self.disconnected = False

    def on(self, handler):
        self.handler = handler

    async def send(self, prompt: str) -> str:
        if not isinstance(prompt, str):
            raise TypeError("Copilot SDK 1 expects a string prompt")
        self.prompt = prompt
        if self.fail_send:
            raise RuntimeError("stub send failed")
        assert self.handler is not None
        for event_type, content in (
            ("assistant.message_delta", "CONNECTION_SUCCESS"),
            ("assistant.message", ""),
        ):
            self.handler(
                SimpleNamespace(type=event_type, data=SimpleNamespace(delta_content=content))
            )
        if self.session_error:
            self.handler(
                SimpleNamespace(
                    type="session.error",
                    data=SimpleNamespace(message=self.session_error, code="STUB_FAILURE"),
                )
            )
        else:
            self.handler(SimpleNamespace(type="session.idle", data=SimpleNamespace()))
        return "stub-message"

    async def disconnect(self) -> None:
        self.disconnected = True


class ClientStub:
    def __init__(self, session: SessionStub) -> None:
        self.session = session
        self.config: dict[str, Any] = {}

    async def create_session(self, **kwargs):
        # Validate against the installed SDK signature without starting its runtime.
        inspect.signature(CopilotClient.create_session).bind(self, **kwargs)
        self.config = kwargs
        return self.session


class SessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_connection_uses_current_sdk_contract_and_disconnects(self) -> None:
        session = SessionStub()
        client = ClientStub(session)
        result = await agent.run_connection_agent(
            cast(CopilotClient, client), "example", "USER", Path("token")
        )
        self.assertTrue(result)
        self.assertTrue(session.disconnected)
        self.assertIn("snow connection test", session.prompt)
        self.assertIs(client.config["on_permission_request"], agent.handle_permission_request)
        self.assertIs(client.config["on_user_input_request"], agent.handle_user_input)
        self.assertEqual(client.config["system_message"]["mode"], "append")

    async def test_full_setup_uses_current_sdk_contract_and_disconnects(self) -> None:
        session = SessionStub()
        client = ClientStub(session)
        await agent.run_full_setup_agent(cast(CopilotClient, client), "example", "USER", "token")
        self.assertTrue(session.disconnected)
        self.assertIn("SNOWFLAKE_COMPLETE_SETUP.md", session.prompt)
        self.assertIs(client.config["on_permission_request"], agent.handle_permission_request)

    async def test_both_phases_disconnect_after_send_failure(self) -> None:
        for full_setup in (False, True):
            with self.subTest(full_setup=full_setup):
                session = SessionStub(fail_send=True)
                client = ClientStub(session)
                with self.assertRaisesRegex(RuntimeError, "stub send failed"):
                    if full_setup:
                        await agent.run_full_setup_agent(
                            cast(CopilotClient, client), "example", "USER", "token"
                        )
                    else:
                        await agent.run_connection_agent(
                            cast(CopilotClient, client), "example", "USER", Path("token")
                        )
                self.assertTrue(session.disconnected)

    async def test_failed_setup_stops_client_and_removes_private_token_file(self) -> None:
        client = Mock(start=AsyncMock(), stop=AsyncMock())
        token_paths: list[Path] = []

        async def fail_connection(_client, _account, _user, path):
            token_paths.append(path)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.read_text(), "offline-test-token")
            raise RuntimeError("stub connection failed")

        with (
            patch.object(agent, "CopilotClient", return_value=client),
            patch.object(agent, "run_connection_agent", side_effect=fail_connection),
            self.assertRaisesRegex(RuntimeError, "stub connection failed"),
        ):
            await agent.run_setup_agent("example", "USER", "offline-test-token")
        client.stop.assert_awaited_once()
        self.assertEqual(len(token_paths), 1)
        self.assertFalse(token_paths[0].exists())

    async def test_both_phases_propagate_session_errors_and_disconnect(self) -> None:
        for full_setup in (False, True):
            with self.subTest(full_setup=full_setup):
                session = SessionStub(session_error="offline session failed")
                client = ClientStub(session)
                with self.assertRaisesRegex(RuntimeError, "STUB_FAILURE.*offline session failed"):
                    if full_setup:
                        await agent.run_full_setup_agent(
                            cast(CopilotClient, client), "example", "USER", "token"
                        )
                    else:
                        await agent.run_connection_agent(
                            cast(CopilotClient, client), "example", "USER", Path("token")
                        )
                self.assertTrue(session.disconnected)

    async def test_full_setup_error_does_not_report_completion_and_cleans_up(self) -> None:
        sessions = [SessionStub(), SessionStub(session_error="offline setup failed")]
        client = Mock(
            start=AsyncMock(),
            stop=AsyncMock(),
            create_session=AsyncMock(side_effect=sessions),
        )
        token_paths: list[Path] = []
        original = agent.tempfile.NamedTemporaryFile
        output = StringIO()

        def create_file(**kwargs):
            token_file = original(**kwargs)
            token_paths.append(Path(token_file.name))
            return token_file

        with (
            patch.object(agent.tempfile, "NamedTemporaryFile", side_effect=create_file),
            patch.object(agent, "CopilotClient", return_value=client),
            patch.object(agent.Prompt, "ask", return_value="yes"),
            patch.object(agent, "console", Console(file=output)),
            self.assertRaisesRegex(RuntimeError, "offline setup failed"),
        ):
            await agent.run_setup_agent("example", "USER", "offline-test-token")
        self.assertNotIn("Setup process completed", output.getvalue())
        self.assertTrue(all(session.disconnected for session in sessions))
        client.stop.assert_awaited_once()
        self.assertEqual(len(token_paths), 1)
        self.assertFalse(token_paths[0].exists())

    async def test_constructor_failure_also_removes_token_file(self) -> None:
        token_paths: list[Path] = []
        original = agent.tempfile.NamedTemporaryFile

        def create_file(**kwargs):
            token_file = original(**kwargs)
            token_paths.append(Path(token_file.name))
            return token_file

        with (
            patch.object(agent.tempfile, "NamedTemporaryFile", side_effect=create_file),
            patch.object(
                agent, "CopilotClient", side_effect=RuntimeError("stub constructor failed")
            ),
            self.assertRaisesRegex(RuntimeError, "stub constructor failed"),
        ):
            await agent.run_setup_agent("example", "USER", "offline-test-token")
        self.assertEqual(len(token_paths), 1)
        self.assertFalse(token_paths[0].exists())


class InputTests(unittest.IsolatedAsyncioTestCase):
    async def test_selected_choice_is_not_reported_as_freeform(self) -> None:
        request = UserInputRequest(question="Choose", choices=["yes", "no"], allowFreeform=True)
        with patch.object(agent.Prompt, "ask", return_value="yes"):
            response = await agent.handle_user_input(request, {"session_id": "stub"})
        self.assertEqual(response, {"answer": "yes", "wasFreeform": False})

    async def test_freeform_answer_is_reported_as_freeform(self) -> None:
        with patch.object(agent.Prompt, "ask", return_value="another answer"):
            response = await agent.handle_user_input(UserInputRequest(question="Ask"), {})
        self.assertTrue(response["wasFreeform"])

    async def test_fixed_choices_are_enforced(self) -> None:
        request = UserInputRequest(question="Choose", choices=["yes", "no"], allowFreeform=False)
        with patch.object(agent.Prompt, "ask", return_value="no") as ask:
            await agent.handle_user_input(request, {})
        self.assertEqual(ask.call_args.kwargs["choices"], ["yes", "no"])


class PermissionTests(unittest.TestCase):
    def test_mcp_arguments_are_visible_before_approval(self) -> None:
        sql = "CREATE TABLE TEST_TABLE (ID INT)"
        request = PermissionRequestMcp(
            read_only=False,
            server_name="snowflake-test",
            tool_name="execute_sql",
            tool_title="Execute SQL",
            args={"sql": sql},
        )
        output = StringIO()

        def approve(*args, **kwargs):
            self.assertIn(sql, output.getvalue())
            self.assertIn("execute_sql", output.getvalue())
            return True

        with (
            patch.object(agent, "console", Console(file=output, width=120)),
            patch.object(agent.sys.stdin, "isatty", return_value=True),
            patch.object(agent.Confirm, "ask", side_effect=approve),
        ):
            decision = agent.handle_permission_request(request, {"session_id": "stub"})
        self.assertIsInstance(decision, PermissionDecisionApproveOnce)

    def test_operator_grants_only_one_operation(self) -> None:
        request = PermissionRequestRead(intention="Read setup", path="example.toml")
        with (
            patch.object(agent.sys.stdin, "isatty", return_value=True),
            patch.object(agent.Confirm, "ask", return_value=True) as ask,
        ):
            decision = agent.handle_permission_request(request, {"session_id": "stub"})
        assert isinstance(decision, PermissionDecisionApproveOnce)
        self.assertTrue(decision.approved_interactively)
        self.assertFalse(ask.call_args.kwargs["default"])

    def test_operator_can_reject(self) -> None:
        request = PermissionRequestRead(intention="Read setup", path="example.toml")
        with (
            patch.object(agent.sys.stdin, "isatty", return_value=True),
            patch.object(agent.Confirm, "ask", return_value=False),
        ):
            decision = agent.handle_permission_request(request, {"session_id": "stub"})
        self.assertIsInstance(decision, PermissionDecisionReject)

    def test_noninteractive_input_does_not_approve(self) -> None:
        request = PermissionRequestRead(intention="Read setup", path="example.toml")
        with patch.object(agent.sys.stdin, "isatty", return_value=False):
            decision = agent.handle_permission_request(request, {"session_id": "stub"})
        self.assertIsInstance(decision, PermissionDecisionUserNotAvailable)

    def test_managed_approval_is_left_to_the_host(self) -> None:
        request = PermissionRequestRead(
            intention="Read setup", path="example.toml", managed_approval_required=True
        )
        decision = agent.handle_permission_request(request, {"session_id": "stub"})
        self.assertIsInstance(decision, PermissionNoResult)


class CliTests(unittest.TestCase):
    def test_help_and_version_without_starting_copilot(self) -> None:
        runner = CliRunner()
        with patch.object(agent, "CopilotClient", side_effect=AssertionError("unexpected runtime")):
            for args in (["--help"], ["setup", "--help"], ["version"]):
                with self.subTest(args=args):
                    result = runner.invoke(app, args)
                    self.assertEqual(result.exit_code, 0, result.output)


if __name__ == "__main__":
    unittest.main()
