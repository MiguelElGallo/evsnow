"""Unit tests for the local Event Hub sender utility."""

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SENDER = REPO_ROOT / "tools" / "eventhub_sender" / "main.py"


def _load_sender():
    spec = importlib.util.spec_from_file_location("eventhub_sender_main", SENDER)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_build_producer_can_force_azure_cli_credential(monkeypatch):
    sender = _load_sender()
    credential = object()
    captured: dict[str, object] = {}

    async def fake_build_eventhub_credential(*, namespace, logger, credential_mode):
        captured["namespace"] = namespace
        captured["credential_mode"] = credential_mode
        captured["logger"] = logger
        return credential, 123, "AzureCliCredential"

    class FakeProducerClient:
        def __init__(self, **kwargs):
            captured["producer_kwargs"] = kwargs

    monkeypatch.setattr(sender, "build_eventhub_credential", fake_build_eventhub_credential)
    monkeypatch.setattr(sender, "EventHubProducerClient", FakeProducerClient)

    producer, returned_credential = await sender._build_producer(
        None,
        "example.servicebus.windows.net",
        "topic1",
        "azure_cli",
    )

    assert isinstance(producer, FakeProducerClient)
    assert returned_credential is credential
    assert captured["namespace"] == "example.servicebus.windows.net"
    assert captured["credential_mode"] == "azure_cli"
    assert captured["producer_kwargs"] == {
        "fully_qualified_namespace": "example.servicebus.windows.net",
        "eventhub_name": "topic1",
        "credential": credential,
    }
