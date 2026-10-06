"""Offline compatibility checks against real AI and telemetry SDKs.

Provider constructors use dummy credentials. Remote model requests and HTTP
transports are blocked; the only inference uses Pydantic AI's TestModel.
"""

import os
import socket
import sys

import httpx
import httpx2
import logfire
import pydantic_ai.models
import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel as PydanticTestModel

from utils.smart_retry import ExceptionAnalyzer, RetryDecision


@pytest.fixture(autouse=True)
def offline_provider_credentials(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    # Legacy streaming tests replace these package entries with MagicMock at
    # collection time. Restore the real packages for lazy SDK imports only
    # during these checks, and let monkeypatch restore the previous entries.
    monkeypatch.setitem(sys.modules, "pydantic_ai", pydantic_ai)
    monkeypatch.setitem(sys.modules, "logfire", logfire)
    # CLI tests can import this lazy integration while the package mock is
    # active. Re-import it against the real Agent instead of its cached mock.
    monkeypatch.delitem(sys.modules, "logfire._internal.integrations.pydantic_ai", raising=False)
    for variable in (
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "OPENAI_API_VERSION",
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_VERSION",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_BASE_URL",
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        "GROQ_API_KEY",
        "GROQ_BASE_URL",
        "CO_API_KEY",
        "CO_BASE_URL",
        "LOGFIRE_TOKEN",
    ):
        monkeypatch.delenv(variable, raising=False)
    # Register teardown restoration for environment values the analyzer sets.
    for variable in (
        "OPENAI_API_KEY",
        "OPENAI_API_VERSION",
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_VERSION",
        "ANTHROPIC_API_KEY",
        "GOOGLE_API_KEY",
        "GROQ_API_KEY",
        "CO_API_KEY",
    ):
        monkeypatch.setenv(variable, "offline-test-key")

    def no_network(*args, **kwargs):
        raise AssertionError("Offline SDK compatibility tests must not access the network")

    async def no_async_network(*args, **kwargs):
        no_network()

    monkeypatch.setattr(pydantic_ai.models, "ALLOW_MODEL_REQUESTS", False)
    monkeypatch.setattr(httpx.Client, "send", no_network)
    monkeypatch.setattr(httpx.AsyncClient, "send", no_async_network)
    monkeypatch.setattr(httpx2.Client, "send", no_network)
    monkeypatch.setattr(httpx2.AsyncClient, "send", no_async_network)
    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)


@pytest.mark.parametrize(
    "provider,model,expected_class,key_variable,expected_system",
    [
        ("openai", "gpt-4o-mini", "OpenAIChatModel", "OPENAI_API_KEY", "openai"),
        ("azure", "gpt-4o-mini", "OpenAIChatModel", "AZURE_OPENAI_API_KEY", "azure"),
        ("anthropic", "claude-sonnet-4-5", "AnthropicModel", "ANTHROPIC_API_KEY", "anthropic"),
        ("gemini", "gemini-2.5-flash", "GoogleModel", "GOOGLE_API_KEY", "google"),
        ("groq", "llama-3.3-70b-versatile", "GroqModel", "GROQ_API_KEY", "groq"),
        ("cohere", "command-r-plus", "CohereModel", "CO_API_KEY", "cohere"),
    ],
)
def test_each_declared_provider_constructs_real_expected_model(
    provider, model, expected_class, key_variable, expected_system, monkeypatch
):
    # Remove the provider key first to prove the analyzer supplies the configured
    # key instead of accidentally using an inherited developer credential.
    monkeypatch.delenv(key_variable)
    analyzer = ExceptionAnalyzer(
        llm_provider=provider,
        llm_model=model,
        llm_api_key="configured-dummy-key",
        llm_endpoint="https://offline-example.openai.azure.com/" if provider == "azure" else None,
    )
    assert isinstance(analyzer.agent, Agent)
    actual_model = analyzer.agent.model
    assert isinstance(actual_model, pydantic_ai.models.Model)
    assert type(actual_model).__name__ == expected_class
    assert actual_model.system == expected_system
    assert actual_model.model_name == model
    assert os.environ[key_variable] == "configured-dummy-key"
    assert analyzer.get_stats()["api_calls"] == 0


@pytest.mark.asyncio
async def test_real_structured_retry_output_and_logfire_instrumentation_stay_offline(monkeypatch):
    expected = RetryDecision(
        should_retry=True,
        reasoning="Transient source connection timeout",
        suggested_wait_seconds=3,
        confidence=0.9,
    )
    model = PydanticTestModel(custom_output_args=expected.model_dump())
    real_agent = Agent(model)
    monkeypatch.setattr("utils.smart_retry.Agent", lambda configured_model: real_agent)

    exporter = InMemorySpanExporter()
    telemetry = logfire.configure(
        local=True,
        send_to_logfire=False,
        console=False,
        metrics=False,
        additional_span_processors=[SimpleSpanProcessor(exporter)],
    )
    telemetry.instrument_pydantic_ai(real_agent)
    monkeypatch.setattr(logfire, "span", telemetry.span)
    monkeypatch.setattr(logfire, "info", telemetry.info)
    monkeypatch.setattr(logfire, "error", telemetry.error)
    analyzer = ExceptionAnalyzer(llm_api_key="configured-dummy-key")
    source_error = TimeoutError("Connection timed out")
    decision = await analyzer.analyze_exception(source_error, {"operation": "ingest"})
    cached = await analyzer.analyze_exception(source_error, {"operation": "ingest"})

    assert isinstance(decision, RetryDecision)
    assert decision == expected
    assert cached is decision
    assert analyzer.get_stats() == {"api_calls": 1, "cached_decisions": 1, "cache_enabled": True}
    request = model.last_model_request_parameters
    assert request is not None
    assert "should_retry" in request.output_tools[0].parameters_json_schema["properties"]
    span_names = {span.name for span in exporter.get_finished_spans()}
    assert "smart_retry.analyze_exception" in span_names
    assert "smart_retry.llm_call" in span_names
    assert any("agent" in name for name in span_names)
