# Dependency and release peer review

Independent reviewers checked the six existing dependency PRs, current stable
library releases, API migrations, delivery behavior, package contents, tests,
documentation, and live evidence before publication.

Resolved findings:

- Preserve OpenAI Chat Completions when migrating to Pydantic AI 2; translate the
  public Gemini setting to Google and forward each provider's configured key.
- Declare Groq and Cohere extras for the supported provider list.
- Exercise real SDK constructors, structured retry output, caching, and Logfire
  instrumentation with model requests and network transports blocked.
- Isolate the compatibility tests from legacy collection-time package mocks.
- Migrate the helper to Copilot SDK 1.0.16 with explicit permission decisions,
  visible MCP arguments, accurate input responses, and cleanup in failure paths.
- Propagate session errors so failed setup cannot report completion.
- Include the CLI and all pipeline modules in distributions; verify the installed
  wheel outside the checkout. The Hatchling manifest preserves import paths.
- Add helper compatibility and installed-wheel checks to CI, and require helper
  success in the test status aggregation.
- Refresh obsolete action runtimes and retain the intentionally disabled Docker
  publication job. Declare the existing Avrea label for workflow linting.
- Preserve upstream transitive version constraints instead of overriding them.
- Distinguish the first implementation proof from the fresh dependency retest.

The final main suite passed 508 tests on Python 3.13.16. All 16 separate helper
tests passed, as did Ruff, ty, strict documentation builds, distribution builds,
installed-wheel checks, and workflow linting with the intentional disabled-job
condition excluded. No material review findings remain.

Fresh Azure Basic and Snowflake evidence proves 200,000 complete events, correct
persisted restart, exactly 40 replay duplicates, a 200,000-row deduplicated view,
zero processing errors, deleted Azure resources, a suspended test warehouse,
and a disabled test service user. The lock hash recorded during the live test
matches the final dependency lock. No model calls were made.
