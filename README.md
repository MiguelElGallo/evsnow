# EvSnow

EvSnow streams events from Azure Event Hubs into Snowflake with checkpointing,
configuration validation, and observability.

[![Tests](https://github.com/MiguelElGallo/evsnow/actions/workflows/tests.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/tests.yml)
[![CI/CD Pipeline](https://github.com/MiguelElGallo/evsnow/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/ci-cd.yml)
[![Documentation](https://github.com/MiguelElGallo/evsnow/actions/workflows/docs.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/docs.yml)
[![codecov](https://codecov.io/gh/MiguelElGallo/evsnow/branch/main/graph/badge.svg)](https://codecov.io/gh/MiguelElGallo/evsnow)

Read the hosted docs at <https://miguelelgallo.github.io/evsnow/>.

## Quick Start

Use TOML for pipeline shape and `.env` for secrets or local credentials:

```bash
git clone https://github.com/MiguelElGallo/evsnow.git
cd evsnow
uv sync
```

For the smallest complete path, start with
[First run](docs/tutorial/first-run.md). It walks through one Event Hub, one
Snowflake target, validation, a dry run, and a three-message arrival proof.

If the tutorial tells you an object is missing, use only the setup page you
need:

- [Event Hub quickstart](docs/getting-started/event-hub-quickstart.md)
- [Snowflake quickstart](docs/getting-started/snowflake-quickstart.md)

Setup pages assume commands are run from the repo root.

If the Event Hub and Snowflake objects already exist, create the local runtime
files and validate them:

```bash
cp config/evsnow.example.toml config/evsnow.toml
cp .env.example .env

uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env
```

Continue only when validation completes without warnings.

The full configuration surface is documented in
[Parameter reference](docs/reference/parameters.md).

## Documentation Development

```bash
uv sync --group docs --locked
uv run zensical build --clean --strict
uv run zensical serve
```

The Zensical source lives in [docs/](docs/), and the generated site is written
to `site/`. The GitHub Pages workflow deploys the site from `main`.

## License

See [LICENSE](LICENSE) for details.
