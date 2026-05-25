# GitHub Actions Workflows

This document describes the GitHub Actions workflows configured for EvSnow.

## Workflows

### Tests (`.github/workflows/tests.yml`)

Runs on pull requests, pushes, and manual dispatch.

- Python version: `3.13`
- Dependency install: `uv sync --all-groups --locked`
- Unit and integration tests with JUnit XML output
- Coverage reports through `pytest-cov`
- Optional Codecov upload when `CODECOV_TOKEN` is available
- Test result publishing and artifact upload

Manual run:

```bash
gh workflow run tests.yml -f test_level=unit
```

`test_level` can be `unit`, `integration`, or `all`.

### CI/CD (`.github/workflows/ci-cd.yml`)

Runs the main quality and test pipeline.

- Python version: `3.13`
- Formatting: `uv run ruff format --check src/`
- Linting: `uv run ruff check src/`
- Type checking: `uv run ty check src/`
- Unit and integration tests
- Coverage summary and PR coverage comment
- Docker build job is present but disabled in the workflow

Manual run:

```bash
gh workflow run ci-cd.yml
```

### Copilot Setup (`.github/workflows/copilot-setup-steps.yml`)

Prepares the repository for GitHub Copilot coding-agent sessions.

## Secrets

- `CODECOV_TOKEN`: optional, used for Codecov upload and coverage comments.
- No Docker registry secrets are required while the Docker job remains disabled.

## Local Parity

Use the same locked dependency and quality checks locally:

```bash
uv sync --all-groups --locked
uv run ruff format --check src/
uv run ruff check src/
uv run ty check src/
uv run pytest
```

## Troubleshooting

- If CI fails on dependency resolution, run `uv lock --check --python 3.13`.
- If local tests differ from CI, confirm you are using Python 3.13 and the checked-in `uv.lock`.
- If Codecov does not report, verify `CODECOV_TOKEN` and the coverage artifact steps.

## See Also

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [pytest Documentation](https://docs.pytest.org/)
- [Codecov Documentation](https://docs.codecov.io/)
