# Setup

Use these pages when a required cloud object does not exist yet.
[Install EvSnow](../tutorial/first-run.md#install) first, then run setup commands
from the repository root. If both services are new, complete Event Hub setup
before Snowflake setup so the configuration checks have both source and target settings.

## Choose A Setup Path

- [Event Hub quickstart](event-hub-quickstart.md) creates the Azure Event Hub
  namespace, Event Hub, and local sender/receiver RBAC grants.
- [Snowflake quickstart](snowflake-quickstart.md) creates the runtime role,
  service user, control table, target table, and Snowpipe Streaming pipe.
- [Snowflake key-pair auth](../snowflake/key-pair-auth.md) helps troubleshoot
  RSA/JWT authentication.

Run only the missing setup path or paths. When the Event Hub checks and
Snowflake checks you need are green, return to
[First run](../tutorial/first-run.md).

## Setup Checks

Use this as the setup gate before returning to the tutorial:

1. Snowflake setup passes the object
   checks in [Snowflake quickstart](snowflake-quickstart.md).
2. Event Hub setup passes the sender access check in
   [Event Hub quickstart](event-hub-quickstart.md).

The receiver startup and row-arrival check happen in
[First run](../tutorial/first-run.md).
