# Development

Use these pages when changing EvSnow or validating a pull request.

## Pages

- [Testing](testing.md)
- [Integration tests](integration-tests.md)

## Local Gate

```bash
uv sync --all-groups --locked
uv run ruff format --check src/
uv run ruff check src/
uv run ty check src/
uv run pytest
```

