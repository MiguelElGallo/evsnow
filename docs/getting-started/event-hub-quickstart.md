# Event Hub Quickstart

Use this page when you do not already have an Azure Event Hubs namespace and
Event Hub for the first run.

## Before You Start

You need Azure CLI access to a subscription where you can create resource
groups, Event Hubs namespaces, Event Hubs, and role assignments.

Run these commands from the EvSnow repo root after cloning the repo and running
`uv sync`:

```bash
az login
az account set --subscription <subscription-id-or-name>
```

## Create The Event Hub

Pick a globally unique namespace name. EvSnow expects the fully qualified
namespace value to end with `.servicebus.windows.net`.

```bash
LOCATION=eastus
RESOURCE_GROUP=evsnow-quickstart-rg
EVENTHUB_NAMESPACE=<globally-unique-namespace>
EVENTHUB_NAME=topic1

az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION"

az eventhubs namespace create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$EVENTHUB_NAMESPACE" \
  --location "$LOCATION" \
  --sku Standard

az eventhubs eventhub create \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$EVENTHUB_NAMESPACE" \
  --name "$EVENTHUB_NAME" \
  --partition-count 2 \
  --retention-time 24
```

Use these values in `config/evsnow.toml`:

```toml
eventhub_namespace = "<globally-unique-namespace>.servicebus.windows.net"

[event_hubs.EVENTHUBNAME_1]
name = "topic1"
namespace = "<globally-unique-namespace>.servicebus.windows.net"
consumer_group = "$Default"
```

## Grant Local Smoke-Test Access

The EvSnow consumer needs receive access. The included sender utility also
needs send access when the same signed-in Azure CLI identity publishes test
messages.

```bash
ASSIGNEE_OBJECT_ID="$(az ad signed-in-user show --query id -o tsv)"
NAMESPACE_SCOPE="$(az eventhubs namespace show \
  --resource-group "$RESOURCE_GROUP" \
  --name "$EVENTHUB_NAMESPACE" \
  --query id -o tsv)"

az role assignment create \
  --assignee-object-id "$ASSIGNEE_OBJECT_ID" \
  --assignee-principal-type User \
  --role "Azure Event Hubs Data Receiver" \
  --scope "$NAMESPACE_SCOPE"

az role assignment create \
  --assignee-object-id "$ASSIGNEE_OBJECT_ID" \
  --assignee-principal-type User \
  --role "Azure Event Hubs Data Sender" \
  --scope "$NAMESPACE_SCOPE"
```

!!! note "RBAC propagation"

    Role assignments can take a few minutes to propagate. If the first receiver
    or sender attempt fails with an authorization error, wait and retry before
    changing the EvSnow configuration.

## Verify The Resources

```bash
az eventhubs namespace show \
  --resource-group "$RESOURCE_GROUP" \
  --name "$EVENTHUB_NAMESPACE" \
  --query "{name:name,serviceBusEndpoint:serviceBusEndpoint}" \
  -o table

az eventhubs eventhub show \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$EVENTHUB_NAMESPACE" \
  --name "$EVENTHUB_NAME" \
  --query "{name:name,partitionCount:partitionCount,status:status}" \
  -o table
```

The namespace endpoint should contain
`<globally-unique-namespace>.servicebus.windows.net`, and the Event Hub status
should be `Active`.

## Verify Sender RBAC

Send one message with the same Azure CLI identity you will use for the local
first run:

```bash
uv sync

uv run python tools/eventhub_sender/main.py \
  --namespace "${EVENTHUB_NAMESPACE}.servicebus.windows.net" \
  --eventhub "$EVENTHUB_NAME" \
  --count 1 \
  --batch-size 1 \
  --credential-mode azure_cli \
  --payload '{"purpose":"eventhub-rbac-smoke"}'
```

If this fails with an authorization error, wait a few minutes for role
propagation and retry. Continue only after the sender smoke succeeds. The
receiver role is proven in [First run](../tutorial/first-run.md), when EvSnow
connects and starts reading from the Event Hub.

This message proves sender RBAC only. It is not the row-arrival proof. The
First Run tutorial sends a separate batch with a unique `run_id`.

After the Event Hub exists, continue with [Snowflake quickstart](snowflake-quickstart.md)
if Snowflake objects are missing, or [First run](../tutorial/first-run.md) if
Snowflake is already ready.

## Clean Up Quickstart Resources

If this was only a disposable smoke test, delete the resource group:

```bash
az group delete --name "$RESOURCE_GROUP" --yes --no-wait
```
