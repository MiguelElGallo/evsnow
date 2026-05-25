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
- Published releases trigger the same checks; they do not publish Docker images

Manual run:

```bash
gh workflow run ci-cd.yml
```

### Documentation (`.github/workflows/docs.yml`)

Builds the Zensical documentation site on pull requests and deploys it to
GitHub Pages after changes land on `main`.

- Dependency install: `uv sync --group docs --locked`
- Build command: `uv run zensical build --clean --strict`
- Artifact path: `site`, uploaded by the build job on pushes to `main`
- Deploy target: GitHub Pages environment
- Live site: `https://miguelelgallo.github.io/evsnow/`

The deploy job only runs on pushes to `main`. Pull requests build the site but
do not publish it. The deploy job publishes the artifact from the build job; it
does not rebuild the documentation.

Manual run:

```bash
gh workflow run docs.yml
```

Manual runs build the docs only. Deployment happens after a push or merge to
`main`, because the deploy job is gated to push events.

### CodSpeed (`.github/workflows/codspeed.yml`)

Runs benchmark tests through CodSpeed on pull requests, pushes to `main`, and
manual dispatch.

- Python version: `3.13`
- Dependency install: `uv sync --all-groups`
- Benchmark command: `uv run pytest tests/benchmarks/ --codspeed`
- CodSpeed mode: simulation

Treat CodSpeed failures as benchmark or instrumentation failures first. Normal
unit and integration correctness is still covered by the Tests and CI/CD
workflows.

Manual run:

```bash
gh workflow run codspeed.yml
```

### Snowflake Quickstart Harness

Use the quickstart harness when docs or setup SQL changes could affect a fresh
Snowflake setup. The harness copies the repo to `.quickstart-runs/`, executes
the Snowflake quickstart commands, and writes every command plus stdout and
stderr to `commands.jsonl`.

```bash
uv run python tools/quickstart_harness.py --connection default
```

Use a named Snowflake CLI setup connection when the default is not the account
you want to audit:

```bash
uv run python tools/quickstart_harness.py --connection <setup-connection>
```

The acceptance gate is the generated summary reporting `passed`, plus a
`validate EvSnow config` command with no validation errors or warning lines.
The expected success marker is
`Snowflake control table verified/created successfully`.

The harness does not create Azure Event Hubs, start the receiver, send events,
or query row arrival. Use [First run](../tutorial/first-run.md) for that full
runtime proof.

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
curl -fsSL "https://miguelelgallo.github.io/evsnow/getting-started/event-hub-quickstart/?v=<commit-sha>" \
  | rg "eventhub-rbac-smoke"
curl -fsSL "https://miguelelgallo.github.io/evsnow/tutorial/first-run/?v=<commit-sha>" \
  | rg "Choose Your Starting Point"
curl -fsSL "https://miguelelgallo.github.io/evsnow/reference/parameters/?v=<commit-sha>" \
  | rg "explicit --env-file"
```

For other content-sensitive docs changes, search for one or two exact markers
from the changed page in the live HTML before calling the publication complete.

## Release Checks

The release event runs CI/CD checks, but the workflow is not a publishing
pipeline. Before creating a release, verify version values agree across
`pyproject.toml`, `src/main.py`, `src/evsnow/__init__.py`, and version tests.

After publishing a release:

```bash
gh release view <tag>
gh run list --event release --limit 5
```

Report skipped Docker jobs separately from failed checks.

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
