# GitHub Actions Workflows

This document describes the GitHub Actions workflows configured for StreamDuck.

## Available Workflows

### 1. Tests Workflow (`.github/workflows/tests.yml`)

Runs tests on every PR, push, and can be manually triggered.

**Triggers:**

- `pull_request` - On PRs to `main` and `develop`
- `push` - On push to `main`, `develop`, and `feature/*` branches
- `workflow_dispatch` - Manual trigger with test level selection

**Features:**

- Runs on Python 3.11, 3.12, and 3.13
- Unit and integration tests
- Code coverage reporting to Codecov
- Test result publishing
- Concurrency control (cancels in-progress runs)

**Steps:**

1. Checkout code
2. Set up Python (multiple versions)
3. Install UV and dependencies
4. Determine test scope (unit, integration, or all)
5. Run tests with JUnit XML output
6. Generate coverage report
7. Upload coverage to Codecov
8. Publish test results
9. Upload artifacts (reports, HTML coverage)

**Artifacts:**

- `test-results-*.xml` - JUnit test results
- `test-report.txt` - Full test report
- `htmlcov/` - HTML coverage report
- `.coverage` - Coverage data file

### 2. CI/CD Pipeline Workflow (`.github/workflows/ci-cd.yml`)

Comprehensive CI/CD pipeline with linting, testing, coverage, security, and Docker build.

**Triggers:**

- `push` to `main` and `develop`
- `pull_request` on `main` and `develop`
- `release` published
- `workflow_dispatch` - Manual trigger

**Jobs:**

#### Lint & Format

- Black formatting check
- isort import sorting check
- Pylint linting
- Runs on Python 3.13

#### Unit Tests

- Tests on Python 3.11, 3.12, 3.13
- JUnit XML output
- Artifact upload

#### Integration Tests

- Full integration test suite
- JUnit XML output
- Artifact upload

#### Coverage Report

- Generates XML, HTML, and terminal coverage reports
- Uploads to Codecov
- Comments on PRs with coverage percentage
- Enforces minimum thresholds (green: 75%, orange: 50%)

#### Security Scan

- Runs Bandit for security vulnerabilities
- Generates JSON report
- Artifact upload

#### Build Docker

- Only runs on `main` branch after successful tests
- Builds and pushes Docker image to GHCR
- Uses GitHub Actions cache for layer caching
- Automatic metadata and versioning

#### Test Results Summary

- Aggregates results from all jobs
- Fails pipeline if any critical job fails
- Creates GitHub Step Summary

## Manual Workflow Dispatch

### Tests Workflow - Manual Trigger

Trigger from Actions tab and select:

- `test_level`: Choose between `unit`, `integration`, or `all`

Example:

```bash
gh workflow run tests.yml -f test_level=unit
```

### CI/CD Workflow - Manual Trigger

Trigger from Actions tab with default behavior (full pipeline).

Example:

```bash
gh workflow run ci-cd.yml
```

## Environment Variables

### Required Secrets

None required for tests (GitHub Token is provided automatically)

### Optional Secrets for Docker Push

- `GHCR_TOKEN` - Optional, uses default `GITHUB_TOKEN` if not set
- `DOCKER_USERNAME` - If pushing to Docker Hub
- `DOCKER_PASSWORD` - If pushing to Docker Hub

## Configuration

### Python Versions

Edit the matrix in the workflow files:

```yaml
strategy:
  matrix:
    python-version: ['3.11', '3.12', '3.13']
```

### Coverage Thresholds

Edit the coverage threshold in `ci-cd.yml`:

```yaml
- name: Upload coverage to Codecov
  run: |
    uv run pytest tests/ --cov-fail-under=75  # Change 75 to your threshold
```

### Docker Registry

Change the registry in `ci-cd.yml`:

```yaml
env:
  REGISTRY: ghcr.io  # Change to docker.io for Docker Hub
  IMAGE_NAME: ${{ github.repository }}
```

## Status Badges

Add these to your README.md:

```markdown
## CI/CD Status

[![Tests](https://github.com/VEUKA/evsnow/actions/workflows/tests.yml/badge.svg)](https://github.com/VEUKA/evsnow/actions/workflows/tests.yml)
[![CI/CD Pipeline](https://github.com/VEUKA/evsnow/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/VEUKA/evsnow/actions/workflows/ci-cd.yml)
```

## Workflow Behavior

### On Pull Request

1. Runs tests on all Python versions
2. Checks code quality (linting)
3. Generates coverage report and comments on PR
4. Publishes test results
5. PR will fail if tests fail

### On Push to Main

1. Runs full CI/CD pipeline
2. Runs linting and all test suites
3. Generates coverage report
4. Runs security scan
5. Builds and pushes Docker image
6. Creates GitHub Step Summary
7. Branch protection rules enforce passing checks

### On Push to Develop

1. Same as main but doesn't build Docker image
2. For feature development

### On Release

1. Runs full CI/CD pipeline
2. Builds and tags Docker image with version
3. Publishes to GHCR

### Manual Trigger

1. Allows selecting specific test level
2. Useful for testing specific fixes
3. Great for debugging

## Troubleshooting

### Workflow Not Triggering

- Check branch protection rules
- Ensure workflow file is in `main` branch in `.github/workflows/`
- Check file permissions (should be readable)

### Tests Failing Locally but Passing in CI

- Ensure Python versions match
- Check for environment-specific issues
- Use `uv sync` to match exact dependency versions

### Docker Build Failing

- Check Docker credentials in secrets
- Ensure Dockerfile exists in repo root
- Check Docker registry limits

### Coverage Not Reporting

- Ensure `pytest-cov` is installed
- Check that `src/` directory structure matches config
- Verify coverage minimum isn't too high

## Best Practices

1. **Frequent Commits** - Small commits make debugging easier
2. **Meaningful Messages** - Clear commit messages help in CI logs
3. **Test Locally First** - Run `uv run pytest` before pushing
4. **Review Logs** - Check workflow logs in Actions tab
5. **Update Dependencies** - Keep Python and packages updated

## Performance Tips

1. **Cache Usage** - GitHub Actions automatically caches pip/uv
2. **Matrix Strategy** - Tests run in parallel across Python versions
3. **Artifact Retention** - Set to 30 days to save storage
4. **Concurrency Control** - In-progress runs are cancelled on new pushes

## See Also

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [pytest Documentation](https://docs.pytest.org/)
- [Codecov Documentation](https://docs.codecov.io/)
- [Docker Documentation](https://docs.docker.com/)
