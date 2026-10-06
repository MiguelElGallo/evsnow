# Snowflake Setup CLI

This experimental helper offers interactive Snowflake setup using the Copilot SDK.
For the standard setup path, follow the
[Snowflake quickstart](https://miguelelgallo.github.io/evsnow/getting-started/snowflake-quickstart/).

From this directory:

```bash
uv sync --locked
uv run snowflake-setup --help
```

Each permission request asks you to approve the operation once. Unattended input
does not approve operations. Session errors stop the setup and clean up the
session's temporary token file.
