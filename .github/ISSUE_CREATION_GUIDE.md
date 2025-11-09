# Creating GitHub Issues for Test Coverage

This document provides quick commands and templates to create the 9 GitHub issues for test coverage.

## Issue Creation Checklist

- [ ] Issue 1: CLI Application (`src/main.py`)
- [ ] Issue 2: EventHub Consumer (`src/consumers/eventhub.py`)
- [ ] Issue 3: Pipeline Orchestrator (`src/pipeline/orchestrator.py`)
- [ ] Issue 4: Snowflake Client (`src/streaming/snowflake_high_performance.py`)
- [ ] Issue 5: Configuration (`src/utils/config.py`)
- [ ] Issue 6: Smart Retry (`src/utils/smart_retry.py`)
- [ ] Issue 7: Snowflake Utils (`src/utils/snowflake.py`)
- [ ] Issue 8: Streaming Modules (`src/streaming/`)
- [ ] Issue 9: Integration Tests

## Quick Reference

### Labels to Use
- `testing` - All test-related issues
- `cli` - For CLI tests
- `eventhub` - For EventHub tests
- `pipeline` - For pipeline tests
- `snowflake` - For Snowflake tests
- `config` - For configuration tests
- `utilities` - For utility tests
- `integration` - For integration tests
- `priority:high` - Critical modules (Issues 1-5, 7)
- `priority:medium` - Medium priority (Issues 6, 8, 9)

### Issue Template Reference

All issue templates are in `TEST_ISSUES.md`. For each issue:

1. Copy the issue content from `TEST_ISSUES.md`
2. Use the title from the template
3. Add the appropriate labels
4. Assign to team member
5. Link to this PR

### Estimated Effort

**High Priority Issues (48-72 hours total):**
- Issue 1 (CLI): 4-6 hours
- Issue 2 (EventHub): 10-12 hours (most complex)
- Issue 3 (Orchestrator): 6-8 hours
- Issue 4 (Snowflake Client): 6-8 hours
- Issue 5 (Configuration): 6-8 hours
- Issue 7 (Snowflake Utils): 6-8 hours

**Medium Priority Issues (16-24 hours total):**
- Issue 6 (Smart Retry): 6-8 hours
- Issue 8 (Streaming): 2-4 hours
- Issue 9 (Integration): 8-12 hours

**Total Estimate:** 64-96 hours of development time

### Parallel Work Strategy

To maximize efficiency, issues can be worked on in parallel:

**Sprint 1 (Week 1):**
- Developer A: Issue 1 (CLI) + Issue 5 (Config)
- Developer B: Issue 2 (EventHub) - most complex
- Developer C: Issue 3 (Orchestrator) + Issue 8 (Streaming)

**Sprint 2 (Week 2):**
- Developer A: Issue 4 (Snowflake Client)
- Developer B: Issue 7 (Snowflake Utils)
- Developer C: Issue 6 (Smart Retry)

**Sprint 3 (Week 3):**
- All Developers: Issue 9 (Integration Tests)
- Code review and refinement
- Coverage verification

## Issue Dependencies

```
Issue 5 (Config) → Should be done first (used by all other tests)
  ↓
Issue 1 (CLI) → Uses config
Issue 2 (EventHub) → Uses config
Issue 3 (Orchestrator) → Uses config, EventHub, Snowflake Client
Issue 4 (Snowflake Client) → Uses config
Issue 7 (Snowflake Utils) → Uses config
Issue 6 (Smart Retry) → Independent
Issue 8 (Streaming) → Independent
  ↓
Issue 9 (Integration) → Should be done last (uses all modules)
```

## Quick Commands

### Create Issue via GitHub CLI

```bash
# Issue 1: CLI
gh issue create \
  --title "Write tests for CLI application (src/main.py)" \
  --label "testing,cli,priority:high" \
  --body-file .github/issues/issue_1_cli.md

# Issue 2: EventHub Consumer
gh issue create \
  --title "Write tests for EventHub consumer (src/consumers/eventhub.py)" \
  --label "testing,eventhub,priority:high" \
  --body-file .github/issues/issue_2_eventhub.md

# ... (repeat for all 9 issues)
```

### Create Issue Manually (GitHub Web)

1. Go to: https://github.com/VEUKA/evsnow/issues/new
2. Copy content from `TEST_ISSUES.md` for the specific issue
3. Paste into issue body
4. Add title from template
5. Add labels
6. Assign developer
7. Create issue

## Testing Infrastructure Files

Reference these in issue descriptions:

- **Standards:** [TESTING_STANDARDS.md](./TESTING_STANDARDS.md)
- **Issue Templates:** [TEST_ISSUES.md](./TEST_ISSUES.md)
- **Quick Start:** [tests/README.md](./tests/README.md)
- **Fixtures:** [tests/conftest.py](./tests/conftest.py)

## Success Metrics

Track these metrics for each issue:

- [ ] All test scenarios covered
- [ ] All classes/functions tested
- [ ] No real services called
- [ ] Coverage requirement met (80% overall, 90% critical)
- [ ] Tests run fast (<1s unit, <10s integration)
- [ ] Naming conventions followed
- [ ] Fixtures used from conftest.py
- [ ] Documentation updated if needed

## Team Communication

### Daily Standup Questions

1. Which test issue are you working on?
2. How many tests have you written?
3. What's your current coverage percentage?
4. Any blockers with mocking?
5. Need help with any patterns?

### Code Review Checklist

When reviewing test PRs:

- [ ] Issue number referenced in PR
- [ ] All tests pass
- [ ] Coverage meets requirements
- [ ] No real service connections
- [ ] Naming conventions followed
- [ ] Fixtures used appropriately
- [ ] Docstrings explain complex tests
- [ ] Edge cases covered
- [ ] Error paths tested

## Common Questions

### Q: Can I write tests in a different order than the issues?
**A:** Yes, but Issue 5 (Config) should be done first as many tests depend on it.

### Q: Can multiple people work on the same issue?
**A:** Yes, break it into sub-tasks and coordinate via comments on the issue.

### Q: What if I find a bug while writing tests?
**A:** Create a separate bug issue, link it to the test issue, and continue testing.

### Q: Should I mock everything?
**A:** Yes! No real Snowflake, EventHub, Azure, or Logfire connections.

### Q: How do I test async functions?
**A:** Use `@pytest.mark.asyncio` and see examples in TESTING_STANDARDS.md.

### Q: What if coverage is hard to reach?
**A:** Focus on critical paths first. Document why certain code isn't covered.

## Additional Resources

- [pytest Documentation](https://docs.pytest.org/)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)
- [pytest-mock](https://pytest-mock.readthedocs.io/)
- [Python unittest.mock](https://docs.python.org/3/library/unittest.mock.html)
- [Pydantic Testing](https://docs.pydantic.dev/latest/concepts/testing/)

## Next Actions

1. **Project Manager:** Create all 9 issues on GitHub
2. **Team Lead:** Assign issues to developers
3. **Developers:** Read TESTING_STANDARDS.md before starting
4. **Everyone:** Use tests/README.md as quick reference

---

**Let's get testing! 🚀**
