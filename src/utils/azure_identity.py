"""
Azure identity helpers for Event Hub clients.

Provides a small helper to create and validate Azure CLI credentials with
predictable logging so we can see which identity is used during Event Hub
authentication.
"""

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from azure.identity.aio import AzureCliCredential
else:  # Fallback for runtime without importing azure.identity during type checking
    AzureCliCredential = Any


async def build_eventhub_cli_credential(
    namespace: str, logger: logging.Logger
) -> tuple["AzureCliCredential", int]:
    """Create an Azure CLI credential and verify it can fetch an Event Hub token.

    Args:
        namespace: Event Hub namespace for logging context.
        logger: Logger instance to emit diagnostic messages.

    Returns:
        Tuple of (credential, token expiry epoch seconds).

    Raises:
        Exception: If credential creation or token retrieval fails.
    """
    # Import inside function so tests can monkeypatch azure.identity.aio.AzureCliCredential
    from azure.identity.aio import AzureCliCredential

    azure_identity_logger = logging.getLogger("azure.identity")
    original_level = azure_identity_logger.level

    # Temporarily raise log level to see which credential in the chain is used
    azure_identity_logger.setLevel(logging.INFO)
    try:
        credential = AzureCliCredential()
        logger.info("✅ AzureCliCredential created successfully")

        token = await credential.get_token("https://eventhubs.azure.net/.default")
        logger.info("✅ Successfully obtained token for EventHub scope")
        logger.info("Token expires at epoch: %s", token.expires_on)

        return credential, token.expires_on
    finally:
        azure_identity_logger.setLevel(original_level)
