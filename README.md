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

Fresh Event Hub namespaces should start with
[Event Hub quickstart](docs/getting-started/event-hub-quickstart.md). Fresh
Snowflake accounts should start with
[Snowflake quickstart](docs/getting-started/snowflake-quickstart.md). Both
setup pages assume commands are run from the repo root after the install step
above.

If the Event Hub and Snowflake objects already exist, continue with the local
runtime files:

```bash
cp config/evsnow.example.toml config/evsnow.toml
cp .env.example .env

# Edit config/evsnow.toml for Event Hub, Snowflake target, and mappings.
# Edit .env for Snowflake credentials and local secrets.
uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env
```

Then continue with [First run](docs/tutorial/first-run.md).

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
