"""
Azure identity helpers for Event Hub clients.

Provides a small helper to create and validate Azure credentials with
predictable logging so we can see which identity is used during Event Hub
authentication.
"""

import logging
from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    from azure.identity.aio import AzureCliCredential, DefaultAzureCredential
else:  # Fallback for runtime without importing azure.identity during type checking
    AzureCliCredential = Any
    DefaultAzureCredential = Any


EVENTHUB_TOKEN_SCOPE = "https://eventhubs.azure.net/.default"
EventHubCredentialMode = Literal["default", "azure_cli"]


async def build_eventhub_credential(
    namespace: str,
    logger: logging.Logger,
    *,
    credential_mode: EventHubCredentialMode = "default",
    managed_identity_client_id: str | None = None,
) -> tuple["DefaultAzureCredential | AzureCliCredential", int, str]:
    """Create an Azure credential and verify it can fetch an Event Hub token.

    Args:
        namespace: Event Hub namespace for logging context.
        logger: Logger instance to emit diagnostic messages.
        credential_mode: Credential strategy to use. Defaults to the production-capable
            DefaultAzureCredential chain.
        managed_identity_client_id: Optional user-assigned managed identity client ID.

    Returns:
        Tuple of (credential, token expiry epoch seconds, credential label).

    Raises:
        Exception: If credential creation or token retrieval fails.
    """
    # Import inside the function so unit tests can monkeypatch azure.identity.aio cleanly.
    from azure.identity.aio import AzureCliCredential, DefaultAzureCredential

    azure_identity_logger = logging.getLogger("azure.identity")
    original_level = azure_identity_logger.level

    credential: DefaultAzureCredential | AzureCliCredential | None = None

    # Temporarily raise log level to see which credential in the chain is used.
    azure_identity_logger.setLevel(logging.INFO)
    try:
        if credential_mode == "azure_cli":
            # AzureCliCredential is deliberately an explicit dev opt-in because it shells
            # out to local tooling; the Azure SDK docs describe it as a developer-tool
            # credential. https://learn.microsoft.com/python/api/azure-identity/azure.identity.aio.azureclicredential
            credential = AzureCliCredential()
            credential_label = "AzureCliCredential"
        else:
            credential_kwargs: dict[str, str] = {}
            if managed_identity_client_id:
                credential_kwargs["managed_identity_client_id"] = managed_identity_client_id
            # DefaultAzureCredential keeps the same code path for local dev, service
            # principals, and managed identity. The managed_identity_client_id kwarg is
            # the SDK-supported hook for user-assigned managed identities.
            # https://learn.microsoft.com/python/api/azure-identity/azure.identity.aio.defaultazurecredential
            credential = DefaultAzureCredential(**credential_kwargs)
            credential_label = "DefaultAzureCredential"

        logger.info("✅ %s created successfully for namespace %s", credential_label, namespace)

        token = await credential.get_token(EVENTHUB_TOKEN_SCOPE)
        logger.info("✅ Successfully obtained token for EventHub scope")
        logger.info("Token expires at epoch: %s", token.expires_on)

        return credential, token.expires_on, credential_label
    except Exception:
        if credential is not None:
            try:
                await credential.close()
            except Exception:
                logger.debug("Ignoring credential close failure after token validation error")
        raise
    finally:
        azure_identity_logger.setLevel(original_level)


async def build_eventhub_cli_credential(
    namespace: str, logger: logging.Logger
) -> tuple["AzureCliCredential", int]:
    """Compatibility wrapper for callers that still explicitly request Azure CLI auth."""
    credential, expires_on, _label = await build_eventhub_credential(
        namespace=namespace,
        logger=logger,
        credential_mode="azure_cli",
    )
    return cast("AzureCliCredential", credential), expires_on
