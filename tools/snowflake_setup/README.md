# Snowflake Setup CLI

This experimental tool guide has moved into the Zensical documentation site.

- Source page: [docs/tools/snowflake-setup-cli.md](../../docs/tools/snowflake-setup-cli.md)
- Hosted page: <https://miguelelgallo.github.io/evsnow/tools/snowflake-setup-cli/>

The helper uses Copilot SDK 1.x. Session creation uses keyword arguments,
messages are strings, and sessions disconnect when their work finishes. Each
permission request asks the operator to approve that operation once; unattended
input does not approve operations. Managed approvals remain with their host.
MCP requests show their tool arguments before approval. Session errors stop the
setup, disconnect sessions, and remove the temporary token file.
These interfaces follow the [official Python SDK guide](https://github.com/github/copilot-sdk/blob/main/python/README.md).

Run the offline checks from this directory:

```bash
uv sync --locked
uv run python -m unittest discover -s tests -v
uv run snowflake-setup --help
uv run snowflake-setup version
```

The checks use stubs and do not start Copilot, call a model, or change Snowflake.
