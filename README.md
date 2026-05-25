# evsnow

EvSnow streams events from Azure Event Hubs into Snowflake with checkpointing,
configuration validation, and observability.

The full documentation is built with Zensical and published through GitHub
Pages:

- Documentation site: <https://miguelelgallo.github.io/evsnow/>
- First local run: [docs/tutorial/first-run.md](docs/tutorial/first-run.md)
- Configuration reference: [docs/configuration.md](docs/configuration.md)
- Snowflake setup: [docs/getting-started/snowflake-quickstart.md](docs/getting-started/snowflake-quickstart.md)

[![Tests](https://github.com/MiguelElGallo/evsnow/actions/workflows/tests.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/tests.yml)
[![CI/CD Pipeline](https://github.com/MiguelElGallo/evsnow/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/ci-cd.yml)
[![Documentation](https://github.com/MiguelElGallo/evsnow/actions/workflows/docs.yml/badge.svg)](https://github.com/MiguelElGallo/evsnow/actions/workflows/docs.yml)
[![codecov](https://codecov.io/gh/MiguelElGallo/evsnow/branch/main/graph/badge.svg)](https://codecov.io/gh/MiguelElGallo/evsnow)

## Quick Start

```bash
git clone https://github.com/MiguelElGallo/evsnow.git
cd evsnow
uv sync
cp config/evsnow.example.toml config/evsnow.toml
cp .env.example .env
# Edit config/evsnow.toml and .env, then validate.
uv run evsnow validate-config --config-file config/evsnow.toml --env-file .env
```

Edit `.env` before validation. Keep pipeline shape in
`config/evsnow.toml`; use `.env` for secrets and local credentials.

## Documentation Development

```bash
uv sync --group docs
uv run zensical build --clean --strict
uv run zensical serve
```

The Zensical source lives in [docs/](docs/), and the generated site is written
to `site/`. The GitHub Pages workflow deploys the site from `main`.

## License

See [LICENSE](LICENSE) for details.
