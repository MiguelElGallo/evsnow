# Setup

Use these pages when a required cloud object does not exist yet.

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

1. Snowflake setup passes with the quickstart harness or the manual object
   checks in [Snowflake quickstart](snowflake-quickstart.md).
2. Event Hub setup passes the sender RBAC smoke in
   [Event Hub quickstart](event-hub-quickstart.md).

The receiver startup and row-arrival proof happen in
[First run](../tutorial/first-run.md).
