"""
Tests for smart retry logic using LLM analysis.

This module tests the smart retry functionality including:
- RetryDecision model validation
- ExceptionAnalyzer with LLM integration
- RetryManager for both smart and standard modes
- Retry decorator factory functions

Based on TESTING_STANDARDS.md - all tests mock external services.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError
from src.utils.smart_retry import (
    ExceptionAnalyzer,
    RetryDecision,
    RetryManager,
    create_exception_analyzer,
    create_smart_retry_decorator,
    create_standard_retry_decorator,
)

# ============================================================================
# Tests: RetryDecision Model
# ============================================================================


class TestRetryDecision:
    """Tests for RetryDecision Pydantic model."""

    def test_create_decision_with_all_fields_succeeds(self):
        """Test creating RetryDecision with all fields."""
        decision = RetryDecision(
            should_retry=True,
            reasoning="Transient network error",
            suggested_wait_seconds=5,
            confidence=0.9
        )

        assert decision.should_retry is True
        assert decision.reasoning == "Transient network error"
        assert decision.suggested_wait_seconds == 5
        assert decision.confidence == 0.9

    def test_create_decision_with_defaults_succeeds(self):
        """Test creating RetryDecision with default values."""
        decision = RetryDecision(
            should_retry=False,
            reasoning="Fatal error"
        )

        assert decision.should_retry is False
        assert decision.reasoning == "Fatal error"
        assert decision.suggested_wait_seconds == 2  # Default
        assert decision.confidence == 0.5  # Default

    def test_suggested_wait_seconds_minimum_constraint(self):
        """Test that suggested_wait_seconds cannot be less than 1."""
        with pytest.raises(ValidationError) as exc_info:
            RetryDecision(
                should_retry=True,
                reasoning="Test",
                suggested_wait_seconds=0
            )

        error = exc_info.value
        assert "suggested_wait_seconds" in str(error)

    def test_suggested_wait_seconds_maximum_constraint(self):
        """Test that suggested_wait_seconds cannot be greater than 60."""
        with pytest.raises(ValidationError) as exc_info:
            RetryDecision(
                should_retry=True,
                reasoning="Test",
                suggested_wait_seconds=61
            )

        error = exc_info.value
        assert "suggested_wait_seconds" in str(error)

    def test_confidence_minimum_constraint(self):
        """Test that confidence cannot be less than 0.0."""
        with pytest.raises(ValidationError) as exc_info:
            RetryDecision(
                should_retry=True,
                reasoning="Test",
                confidence=-0.1
            )

        error = exc_info.value
        assert "confidence" in str(error)

    def test_confidence_maximum_constraint(self):
        """Test that confidence cannot be greater than 1.0."""
        with pytest.raises(ValidationError) as exc_info:
            RetryDecision(
                should_retry=True,
                reasoning="Test",
                confidence=1.1
            )

        error = exc_info.value
        assert "confidence" in str(error)


# ============================================================================
# Tests: ExceptionAnalyzer
# ============================================================================


class TestExceptionAnalyzer:
    """Tests for ExceptionAnalyzer class."""

    @patch("src.utils.smart_retry.Agent")
    def test_initialize_with_openai_provider(self, mock_agent_class):
        """Test initializing analyzer with OpenAI provider."""
        analyzer = ExceptionAnalyzer(
            llm_provider="openai",
            llm_model="gpt-4o-mini",
            llm_api_key="test-key"
        )

        assert analyzer.llm_provider == "openai"
        assert analyzer.llm_model == "gpt-4o-mini"
        assert analyzer.enable_caching is True
        assert analyzer.timeout_seconds == 10
        assert os.environ.get("OPENAI_API_KEY") == "test-key"

    @patch("src.utils.smart_retry.Agent")
    def test_initialize_with_anthropic_provider(self, mock_agent_class):
        """Test initializing analyzer with Anthropic provider."""
        analyzer = ExceptionAnalyzer(
            llm_provider="anthropic",
            llm_model="claude-3-sonnet",
            llm_api_key="anthropic-key"
        )

        assert analyzer.llm_provider == "anthropic"
        assert analyzer.llm_model == "claude-3-sonnet"
        assert os.environ.get("ANTHROPIC_API_KEY") == "anthropic-key"

    @patch("src.utils.smart_retry.Agent")
    def test_initialize_with_caching_disabled(self, mock_agent_class):
        """Test initializing analyzer with caching disabled."""
        analyzer = ExceptionAnalyzer(
            enable_caching=False
        )

        assert analyzer.enable_caching is False
        assert len(analyzer._decision_cache) == 0

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_analyze_retryable_exception(self, mock_span, mock_agent_class):
        """Test analyzing an exception that should be retried."""
        # Setup mock agent
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.output = RetryDecision(
            should_retry=True,
            reasoning="Connection timeout - transient error",
            suggested_wait_seconds=5,
            confidence=0.85
        )
        mock_agent.run = AsyncMock(return_value=mock_result)
        mock_agent_class.return_value = mock_agent

        # Setup mock span context manager
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer and analyze exception
        analyzer = ExceptionAnalyzer()
        exception = ConnectionError("Connection timeout")

        decision = await analyzer.analyze_exception(exception)

        assert decision.should_retry is True
        assert decision.confidence == 0.85
        assert "timeout" in decision.reasoning.lower()
        assert mock_agent.run.called

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_analyze_fatal_exception(self, mock_span, mock_agent_class):
        """Test analyzing an exception that should not be retried."""
        # Setup mock agent
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.output = RetryDecision(
            should_retry=False,
            reasoning="Authentication error - fatal",
            suggested_wait_seconds=1,
            confidence=0.95
        )
        mock_agent.run = AsyncMock(return_value=mock_result)
        mock_agent_class.return_value = mock_agent

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer and analyze exception
        analyzer = ExceptionAnalyzer()
        exception = PermissionError("401 Unauthorized")

        decision = await analyzer.analyze_exception(exception)

        assert decision.should_retry is False
        assert decision.confidence == 0.95
        assert "fatal" in decision.reasoning.lower()

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_cache_decision_when_enabled(self, mock_span, mock_agent_class):
        """Test that decisions are cached when caching is enabled."""
        # Setup mock agent
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.output = RetryDecision(
            should_retry=True,
            reasoning="Cached decision",
            confidence=0.8
        )
        mock_agent.run = AsyncMock(return_value=mock_result)
        mock_agent_class.return_value = mock_agent

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer with caching enabled
        analyzer = ExceptionAnalyzer(enable_caching=True)
        exception = ConnectionError("Network timeout")

        # First call - should hit LLM
        decision1 = await analyzer.analyze_exception(exception)
        assert mock_agent.run.call_count == 1

        # Second call with same exception - should use cache
        decision2 = await analyzer.analyze_exception(exception)
        assert mock_agent.run.call_count == 1  # Not called again

        # Verify both decisions are the same
        assert decision1.should_retry == decision2.should_retry
        assert decision1.reasoning == decision2.reasoning

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_no_cache_when_disabled(self, mock_span, mock_agent_class):
        """Test that decisions are not cached when caching is disabled."""
        # Setup mock agent
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.output = RetryDecision(
            should_retry=True,
            reasoning="No caching",
            confidence=0.8
        )
        mock_agent.run = AsyncMock(return_value=mock_result)
        mock_agent_class.return_value = mock_agent

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer with caching disabled
        analyzer = ExceptionAnalyzer(enable_caching=False)
        exception = ConnectionError("Network timeout")

        # First call
        await analyzer.analyze_exception(exception)
        assert mock_agent.run.call_count == 1

        # Second call - should hit LLM again
        await analyzer.analyze_exception(exception)
        assert mock_agent.run.call_count == 2

    @pytest.mark.asyncio
    @patch("asyncio.wait_for")
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_handle_llm_timeout(self, mock_span, mock_agent_class, mock_wait_for):
        """Test handling LLM timeout with fallback decision."""
        # Setup mock agent
        mock_agent = MagicMock()
        mock_agent_class.return_value = mock_agent

        # Mock wait_for to raise TimeoutError immediately
        mock_wait_for.side_effect = TimeoutError("Timeout")

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer
        analyzer = ExceptionAnalyzer(timeout_seconds=1)
        exception = ConnectionError("Test error")

        decision = await analyzer.analyze_exception(exception)

        # Should return conservative fallback decision
        assert decision.should_retry is False
        assert "timed out" in decision.reasoning.lower()
        assert decision.confidence == 0.0

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_handle_llm_api_error(self, mock_span, mock_agent_class):
        """Test handling LLM API error with fallback decision."""
        # Setup mock agent to raise error
        mock_agent = MagicMock()
        mock_agent.run = AsyncMock(side_effect=Exception("API error"))
        mock_agent_class.return_value = mock_agent

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer
        analyzer = ExceptionAnalyzer()
        exception = ValueError("Test error")

        decision = await analyzer.analyze_exception(exception)

        # Should return conservative fallback decision
        assert decision.should_retry is False
        assert "failed" in decision.reasoning.lower()
        assert decision.confidence == 0.0

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_build_context_string_basic(self, mock_span, mock_agent_class):
        """Test building context string from exception."""
        # Setup mock agent
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.output = RetryDecision(
            should_retry=True,
            reasoning="Test",
            confidence=0.8
        )
        mock_agent.run = AsyncMock(return_value=mock_result)
        mock_agent_class.return_value = mock_agent

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer
        analyzer = ExceptionAnalyzer()
        exception = ConnectionError("Network timeout")

        # Build context
        context_str = analyzer._build_context_string(exception, None)

        assert "ConnectionError" in context_str
        assert "Network timeout" in context_str
        assert "Should this operation be retried?" in context_str

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_build_context_string_with_context(self, mock_span, mock_agent_class):
        """Test building context string with additional context."""
        # Setup mock agent
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.output = RetryDecision(
            should_retry=True,
            reasoning="Test",
            confidence=0.8
        )
        mock_agent.run = AsyncMock(return_value=mock_result)
        mock_agent_class.return_value = mock_agent

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer
        analyzer = ExceptionAnalyzer()
        exception = ValueError("Invalid input")
        context = {
            "attempt": 2,
            "operation": "database_write",
            "elapsed_time": 30
        }

        # Build context
        context_str = analyzer._build_context_string(exception, context)

        assert "ValueError" in context_str
        assert "Invalid input" in context_str
        assert "attempt: 2" in context_str
        assert "operation: database_write" in context_str
        assert "elapsed_time: 30" in context_str

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_build_context_string_with_cause(self, mock_span, mock_agent_class):
        """Test building context string with exception cause."""
        # Setup mock agent
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.output = RetryDecision(
            should_retry=True,
            reasoning="Test",
            confidence=0.8
        )
        mock_agent.run = AsyncMock(return_value=mock_result)
        mock_agent_class.return_value = mock_agent

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer
        analyzer = ExceptionAnalyzer()

        # Create exception with cause
        try:
            try:
                raise ConnectionError("Network error")
            except ConnectionError as e:
                raise RuntimeError("Operation failed") from e
        except RuntimeError as exc:
            context_str = analyzer._build_context_string(exc, None)

            assert "RuntimeError" in context_str
            assert "Operation failed" in context_str
            assert "Caused by: ConnectionError" in context_str
            assert "Network error" in context_str

    @patch("src.utils.smart_retry.Agent")
    def test_get_stats(self, mock_agent_class):
        """Test getting analyzer statistics."""
        analyzer = ExceptionAnalyzer()

        stats = analyzer.get_stats()

        assert "api_calls" in stats
        assert "cached_decisions" in stats
        assert "cache_enabled" in stats
        assert stats["api_calls"] == 0
        assert stats["cached_decisions"] == 0
        assert stats["cache_enabled"] is True


# ============================================================================
# Tests: RetryManager
# ============================================================================


class TestRetryManager:
    """Tests for RetryManager class."""

    @patch("src.utils.smart_retry.ExceptionAnalyzer")
    def test_create_manager_with_standard_mode(self, mock_analyzer_class):
        """Test creating RetryManager in standard mode."""
        manager = RetryManager(
            smart_enabled=False,
            max_attempts=5
        )

        assert manager.smart_enabled is False
        assert manager.max_attempts == 5
        assert manager.analyzer is None
        mock_analyzer_class.assert_not_called()

    @patch("src.utils.smart_retry.ExceptionAnalyzer")
    def test_create_manager_with_smart_mode(self, mock_analyzer_class):
        """Test creating RetryManager in smart mode."""
        manager = RetryManager(
            smart_enabled=True,
            max_attempts=3,
            llm_provider="openai",
            llm_model="gpt-4o-mini",
            llm_api_key="test-key"
        )

        assert manager.smart_enabled is True
        assert manager.max_attempts == 3
        assert manager.analyzer is not None
        mock_analyzer_class.assert_called_once()

    @patch("src.utils.smart_retry.create_standard_retry_decorator")
    @patch("src.utils.smart_retry.ExceptionAnalyzer")
    def test_get_standard_retry_decorator(self, mock_analyzer_class, mock_decorator):
        """Test getting standard retry decorator."""
        manager = RetryManager(smart_enabled=False, max_attempts=3)

        _ = manager.get_retry_decorator()

        mock_decorator.assert_called_once_with(max_attempts=3)

    @patch("src.utils.smart_retry.create_smart_retry_decorator")
    @patch("src.utils.smart_retry.ExceptionAnalyzer")
    def test_get_smart_retry_decorator(self, mock_analyzer_class, mock_decorator):
        """Test getting smart retry decorator."""
        mock_analyzer = MagicMock()
        mock_analyzer_class.return_value = mock_analyzer

        manager = RetryManager(
            smart_enabled=True,
            max_attempts=5,
            llm_api_key="test-key"
        )

        _ = manager.get_retry_decorator()

        mock_decorator.assert_called_once_with(
            analyzer=mock_analyzer,
            max_attempts=5
        )

    @patch("src.utils.smart_retry.ExceptionAnalyzer")
    def test_get_stats_standard_mode(self, mock_analyzer_class):
        """Test getting stats in standard mode."""
        manager = RetryManager(smart_enabled=False, max_attempts=3)

        stats = manager.get_stats()

        assert stats["smart_enabled"] is False
        assert stats["max_attempts"] == 3
        assert "analyzer" not in stats

    @patch("src.utils.smart_retry.ExceptionAnalyzer")
    def test_get_stats_smart_mode(self, mock_analyzer_class):
        """Test getting stats in smart mode."""
        mock_analyzer = MagicMock()
        mock_analyzer.get_stats.return_value = {
            "api_calls": 5,
            "cached_decisions": 2
        }
        mock_analyzer_class.return_value = mock_analyzer

        manager = RetryManager(
            smart_enabled=True,
            llm_api_key="test-key"
        )

        stats = manager.get_stats()

        assert stats["smart_enabled"] is True
        assert "analyzer" in stats
        assert stats["analyzer"]["api_calls"] == 5
        assert stats["analyzer"]["cached_decisions"] == 2


# ============================================================================
# Tests: Retry Decorators
# ============================================================================


class TestRetryDecorators:
    """Tests for retry decorator factory functions."""

    def test_create_standard_retry_decorator(self):
        """Test creating standard retry decorator."""
        decorator = create_standard_retry_decorator(
            max_attempts=3,
            min_wait=1,
            max_wait=10
        )

        assert decorator is not None
        assert callable(decorator)

    def test_standard_decorator_retries_fixed_attempts(self):
        """Test that standard decorator retries fixed number of attempts."""
        decorator = create_standard_retry_decorator(max_attempts=3)

        attempt_count = 0

        @decorator
        def failing_function():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ConnectionError("Network error")
            return "success"

        result = failing_function()

        assert result == "success"
        assert attempt_count == 3

    def test_standard_decorator_reraises_after_exhaustion(self):
        """Test that standard decorator reraises exception after max attempts."""
        decorator = create_standard_retry_decorator(max_attempts=2)

        @decorator
        def always_failing_function():
            raise ValueError("Always fails")

        with pytest.raises(ValueError, match="Always fails"):
            always_failing_function()

    @patch("src.utils.smart_retry.Agent")
    def test_create_smart_retry_decorator(self, mock_agent_class):
        """Test creating smart retry decorator."""
        mock_agent = MagicMock()
        mock_agent_class.return_value = mock_agent

        analyzer = ExceptionAnalyzer()
        decorator = create_smart_retry_decorator(
            analyzer=analyzer,
            max_attempts=3
        )

        assert decorator is not None
        assert callable(decorator)

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_smart_decorator_uses_llm_decision(self, mock_span, mock_agent_class):
        """Test that smart decorator uses LLM decision for retry."""
        # Setup mock agent to return "should retry" decision
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.output = RetryDecision(
            should_retry=True,
            reasoning="Retryable error",
            confidence=0.9
        )
        mock_agent.run = AsyncMock(return_value=mock_result)
        mock_agent_class.return_value = mock_agent

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer and decorator
        analyzer = ExceptionAnalyzer()
        decorator = create_smart_retry_decorator(analyzer, max_attempts=3)

        attempt_count = 0

        @decorator
        def failing_then_succeeding():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 2:
                raise ConnectionError("Network timeout")
            return "success"

        result = failing_then_succeeding()

        assert result == "success"
        assert attempt_count == 2

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_smart_decorator_stops_on_fatal_error(self, mock_span, mock_agent_class):
        """Test that smart decorator stops immediately on fatal error."""
        # Setup mock agent to return "should not retry" decision
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.output = RetryDecision(
            should_retry=False,
            reasoning="Fatal authentication error",
            confidence=0.95
        )
        mock_agent.run = AsyncMock(return_value=mock_result)
        mock_agent_class.return_value = mock_agent

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create analyzer and decorator
        analyzer = ExceptionAnalyzer()
        decorator = create_smart_retry_decorator(analyzer, max_attempts=5)

        attempt_count = 0

        @decorator
        def always_failing():
            nonlocal attempt_count
            attempt_count += 1
            raise PermissionError("401 Unauthorized")

        with pytest.raises(PermissionError):
            always_failing()

        # Should only attempt once since LLM says don't retry
        assert attempt_count == 1

    @patch("src.utils.smart_retry.Agent")
    def test_create_exception_analyzer_factory(self, mock_agent_class):
        """Test factory function for creating ExceptionAnalyzer."""
        analyzer = create_exception_analyzer(
            llm_provider="openai",
            llm_model="gpt-4",
            llm_api_key="test-key",
            timeout_seconds=15,
            enable_caching=False
        )

        assert isinstance(analyzer, ExceptionAnalyzer)
        assert analyzer.llm_provider == "openai"
        assert analyzer.llm_model == "gpt-4"
        assert analyzer.timeout_seconds == 15
        assert analyzer.enable_caching is False


# ============================================================================
# Integration Tests
# ============================================================================


class TestSmartRetryIntegration:
    """Integration tests for smart retry system."""

    @pytest.mark.asyncio
    @patch("src.utils.smart_retry.Agent")
    @patch("logfire.span")
    async def test_full_workflow_with_retry_success(self, mock_span, mock_agent_class):
        """Test complete workflow where retry eventually succeeds."""
        # Setup mock agent
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.output = RetryDecision(
            should_retry=True,
            reasoning="Transient error",
            confidence=0.8
        )
        mock_agent.run = AsyncMock(return_value=mock_result)
        mock_agent_class.return_value = mock_agent

        # Setup mock span
        mock_span_instance = MagicMock()
        mock_span_instance.__enter__ = MagicMock(return_value=mock_span_instance)
        mock_span_instance.__exit__ = MagicMock(return_value=None)
        mock_span.return_value = mock_span_instance

        # Create manager and get decorator
        manager = RetryManager(
            smart_enabled=True,
            max_attempts=3,
            llm_api_key="test-key"
        )
        decorator = manager.get_retry_decorator()

        attempt_count = 0

        @decorator
        def flaky_operation():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ConnectionError("Temporary failure")
            return f"Success after {attempt_count} attempts"

        result = flaky_operation()

        assert "Success" in result
        assert attempt_count == 3

        # Verify stats
        stats = manager.get_stats()
        assert stats["smart_enabled"] is True

    @patch("src.utils.smart_retry.Agent")
    def test_full_workflow_standard_mode(self, mock_agent_class):
        """Test complete workflow in standard retry mode."""
        # Create manager in standard mode
        manager = RetryManager(
            smart_enabled=False,
            max_attempts=4
        )
        decorator = manager.get_retry_decorator()

        attempt_count = 0

        @decorator
        def flaky_operation():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise RuntimeError("Temporary error")
            return "Success"

        result = flaky_operation()

        assert result == "Success"
        assert attempt_count == 3

        # Verify no analyzer was used
        stats = manager.get_stats()
        assert stats["smart_enabled"] is False
        assert "analyzer" not in stats
