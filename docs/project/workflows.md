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

### Documentation (`.github/workflows/docs.yml`)

Builds the Zensical documentation site on pull requests and deploys it to
GitHub Pages after changes land on `main`.

- Dependency install: `uv sync --group docs --locked`
- Build command: `uv run zensical build --clean --strict`
- Artifact path: `site`
- Deploy target: GitHub Pages environment
- Live site: `https://miguelelgallo.github.io/evsnow/`

The deploy job only runs on pushes to `main`. Pull requests build the site but
do not publish it.

Manual run:

```bash
gh workflow run docs.yml
```

Manual runs build the docs only. Deployment happens after a push or merge to
`main`, because the deploy job is gated to push events.

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

For docs-only changes:

```bash
uv sync --group docs --locked
uv run zensical build --clean --strict
```

After a docs PR is merged, verify the main-branch Documentation run and check
the live Pages content with a cache-busting query string:

```bash
gh run list --workflow docs.yml --branch main --limit 3
curl -fsSL "https://miguelelgallo.github.io/evsnow/?v=<commit-sha>" >/dev/null
curl -fsSL "https://miguelelgallo.github.io/evsnow/getting-started/snowflake-quickstart/?v=<commit-sha>" \
  | rg "Snowflake control table verified/created successfully"
curl -fsSL "https://miguelelgallo.github.io/evsnow/tutorial/first-run/?v=<commit-sha>" \
  | rg "Choose Your Starting Point"
curl -fsSL "https://miguelelgallo.github.io/evsnow/reference/parameters/?v=<commit-sha>" \
  | rg "explicit --env-file"
```

For other content-sensitive docs changes, search for one or two exact markers
from the changed page in the live HTML before calling the publication complete.

## Troubleshooting

- If CI fails on dependency resolution, run `uv lock --check --python 3.13`.
- If local tests differ from CI, confirm you are using Python 3.13 and the checked-in `uv.lock`.
- If Codecov does not report, verify `CODECOV_TOKEN` and the coverage artifact steps.
- If the live docs still show old content after a successful deployment, retry
  with a cache-busting query string and verify the GitHub Pages deploy job.

## See Also

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [pytest Documentation](https://docs.pytest.org/)
- [Codecov Documentation](https://docs.codecov.io/)
