"""
Smart retry logic using LLM analysis to determine if exceptions are retryable.

This module provides intelligent retry mechanisms using LLM-powered exception analysis.
It integrates with tenacity for retry logic and pydantic-ai for LLM interaction.
"""

import asyncio
import logging
import os
from typing import Any

import logfire
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from tenacity import (
    after_log,
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class RetryDecision(BaseModel):
    """LLM response for retry decision."""

    should_retry: bool = Field(description="Whether the error is retryable (True) or fatal (False)")
    reasoning: str = Field(description="Brief explanation of why retry is or isn't recommended")
    suggested_wait_seconds: int = Field(
        default=2,
        ge=1,
        le=60,
        description="Suggested wait time before retry in seconds",
    )
    confidence: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Confidence in the decision (0.0 to 1.0)",
    )


class ExceptionAnalyzer:
    """
    Analyzes exceptions using LLM to determine if they are retryable.

    The analyzer sends exception details and context to an LLM and asks
    for a decision on whether the operation should be retried.
    """

    def __init__(
        self,
        llm_provider: str = "openai",
        llm_model: str = "gpt-4o-mini",
        llm_api_key: str | None = None,
        llm_endpoint: str | None = None,
        timeout_seconds: int = 10,
        enable_caching: bool = True,
    ):
        self.llm_provider = llm_provider
        self.llm_model = llm_model
        self.timeout_seconds = timeout_seconds
        self.enable_caching = enable_caching
        self._decision_cache: dict[str, RetryDecision] = {}
        self._api_call_count = 0

        # Set API keys in environment BEFORE creating agent

        if llm_api_key:
            if llm_provider == "azure":
                os.environ["AZURE_OPENAI_API_KEY"] = llm_api_key
                os.environ["OPENAI_API_KEY"] = llm_api_key  # Also set this for compatibility
                os.environ["OPENAI_API_VERSION"] = (
                    "2024-08-01-preview"  # Required for Azure (must be 2024-06-01 or later for tool_choice)
                )
            elif llm_provider == "openai":
                os.environ["OPENAI_API_KEY"] = llm_api_key
            elif llm_provider == "anthropic":
                os.environ["ANTHROPIC_API_KEY"] = llm_api_key

        # Create pydantic-ai agent
        # Support both standard OpenAI and Azure OpenAI
        if llm_provider == "azure" and llm_endpoint:
            # Azure OpenAI can use the built-in 'azure' provider
            # Set environment variables that Azure provider expects
            import re

            # Extract base URL from endpoint
            base_url_match = re.match(r"(https://[^/]+)", llm_endpoint)
            base_url = base_url_match.group(1) if base_url_match else llm_endpoint

            os.environ["AZURE_OPENAI_ENDPOINT"] = base_url
            os.environ["AZURE_OPENAI_API_VERSION"] = "2024-08-01-preview"

            # Use OpenAIModel with 'azure' provider string
            from pydantic_ai.models.openai import OpenAIModel

            model = OpenAIModel(
                model_name=llm_model,
                provider="azure",  # Use built-in Azure provider
            )

            # Create agent with custom model
            self.agent = Agent(model)
        else:
            model_string = f"{llm_provider}:{llm_model}"
            self.agent = Agent(model_string)

    def _get_system_instructions(self) -> str:
        """Get system instructions for the LLM agent."""
        return """
You are an expert system reliability engineer analyzing database write failures.

Your task is to determine if a failed operation should be retried or if it's a fatal error.

RETRYABLE errors (should_retry=True):
- Network timeouts or connection errors
- Temporary service unavailability (503, 429 errors)
- Resource temporarily locked or busy
- Transient database errors
- Rate limiting errors
- Temporary DNS resolution failures
- Connection pool exhaustion

FATAL errors (should_retry=False):
- Authentication/authorization failures (401, 403)
- Schema mismatches or constraint violations
- Invalid data format or type errors
- Quota exceeded (permanent)
- Resource not found (404)
- Syntax errors in queries
- Configuration errors
- Malformed requests (400)
- Data validation failures

Consider the context provided (recent failures, time elapsed, error patterns).

Provide your reasoning clearly and concisely. Be conservative - when in doubt,
prefer NOT to retry to avoid wasting resources on hopeless operations.
"""

    async def analyze_exception(
        self,
        exception: Exception,
        context: dict[str, Any] | None = None,
    ) -> RetryDecision:
        """
        Analyze an exception and decide if it should be retried.

        Args:
            exception: The exception that occurred
            context: Optional context about the failure (attempt number, etc.)

        Returns:
            RetryDecision with should_retry flag and reasoning
        """
        with logfire.span(
            "smart_retry.analyze_exception",
            exception_type=type(exception).__name__,
            exception_message=str(exception)[:200],
            llm_provider=self.llm_provider,
            llm_model=self.llm_model,
            cache_enabled=self.enable_caching,
        ) as span:
            # Check cache first
            if self.enable_caching:
                cache_key = self._get_cache_key(exception)
                if cache_key in self._decision_cache:
                    logger.info("🎯 Using cached LLM decision")
                    cached_decision = self._decision_cache[cache_key]
                    span.set_attribute("cache_hit", True)
                    span.set_attribute("decision", cached_decision.should_retry)
                    span.set_attribute("confidence", cached_decision.confidence)
                    return cached_decision

            span.set_attribute("cache_hit", False)

            # Build context string
            context_info = self._build_context_string(exception, context)

            logger.info(f"🤖 Analyzing exception with LLM: {type(exception).__name__}")
            logger.debug(f"Context: {context_info}")

            try:
                # Run LLM agent with timeout
                decision = await asyncio.wait_for(
                    self._analyze_with_llm(context_info), timeout=self.timeout_seconds
                )

                # Cache decision
                if self.enable_caching:
                    cache_key = self._get_cache_key(exception)
                    self._decision_cache[cache_key] = decision

                # Track decision in span
                span.set_attribute("decision", decision.should_retry)
                span.set_attribute("confidence", decision.confidence)
                span.set_attribute("suggested_wait", decision.suggested_wait_seconds)
                span.set_attribute("reasoning", decision.reasoning[:200])

                logger.info(
                    f"🤖 LLM Decision: {'RETRY' if decision.should_retry else 'STOP'} "
                    f"(confidence: {decision.confidence:.2f})"
                )
                logger.info(f"🤖 Reasoning: {decision.reasoning}")

                logfire.info(
                    "LLM analysis complete",
                    decision="retry" if decision.should_retry else "stop",
                    confidence=decision.confidence,
                    exception_type=type(exception).__name__,
                )

                return decision

            except TimeoutError:
                logger.error(f"⏱️ LLM analysis timed out after {self.timeout_seconds}s")
                span.set_attribute("timeout", True)
                span.set_attribute("decision", False)
                logfire.error(
                    "LLM analysis timeout",
                    timeout_seconds=self.timeout_seconds,
                    exception_type=type(exception).__name__,
                )
                return self._fallback_decision(
                    f"LLM analysis timed out after {self.timeout_seconds}s. "
                    "Defaulting to no retry for safety."
                )
            except Exception as e:
                logger.error(f"❌ LLM analysis failed: {e}", exc_info=True)
                span.set_attribute("error", str(e))
                span.set_attribute("decision", False)
                logfire.error(
                    "LLM analysis failed",
                    error=str(e),
                    exception_type=type(exception).__name__,
                )
                return self._fallback_decision(
                    f"LLM analysis failed: {e}. Defaulting to no retry for safety."
                )

    async def _analyze_with_llm(self, context_info: str) -> RetryDecision:
        """Call the LLM agent to analyze the exception."""
        with logfire.span(
            "smart_retry.llm_call",
            llm_provider=self.llm_provider,
            llm_model=self.llm_model,
            api_call_count=self._api_call_count + 1,
        ) as span:
            self._api_call_count += 1

            # Add system instructions to the prompt
            full_prompt = f"{self._get_system_instructions()}\n\n{context_info}"

            span.set_attribute("prompt_length", len(full_prompt))

            # Run agent and request structured output as RetryDecision
            result = await self.agent.run(
                full_prompt,
                output_type=RetryDecision,  # Specify output type at runtime
            )
            # Type assertion: output_type=RetryDecision ensures this is RetryDecision
            decision: RetryDecision = result.output  # type: ignore[assignment]

            span.set_attribute("decision", decision.should_retry)
            span.set_attribute("confidence", decision.confidence)

            logfire.info(
                "LLM API call completed",
                provider=self.llm_provider,
                model=self.llm_model,
                decision="retry" if decision.should_retry else "stop",
            )

            return decision

    def _get_cache_key(self, exception: Exception) -> str:
        """Generate cache key from exception type and message."""
        return f"{type(exception).__name__}:{str(exception)[:100]}"

    def _fallback_decision(self, reason: str) -> RetryDecision:
        """Return conservative fallback decision."""
        return RetryDecision(
            should_retry=False,
            reasoning=reason,
            confidence=0.0,
            suggested_wait_seconds=1,  # Min value per Field validation
        )

    def _build_context_string(
        self,
        exception: Exception,
        context: dict[str, Any] | None = None,
    ) -> str:
        """Build a context string for the LLM."""
        lines = [
            "=== Exception Analysis Request ===",
            "",
            f"Exception Type: {type(exception).__name__}",
            f"Exception Message: {exception!s}",
            "",
        ]

        # Add exception details
        if hasattr(exception, "__cause__") and exception.__cause__:
            lines.append(f"Caused by: {type(exception.__cause__).__name__}: {exception.__cause__}")
            lines.append("")

        # Add context
        if context:
            lines.append("Context:")
            for key, value in context.items():
                lines.append(f"  - {key}: {value}")
            lines.append("")

        lines.append("Question: Should this operation be retried?")

        return "\n".join(lines)

    def get_stats(self) -> dict[str, Any]:
        """Get analyzer statistics."""
        return {
            "api_calls": self._api_call_count,
            "cached_decisions": len(self._decision_cache),
            "cache_enabled": self.enable_caching,
        }


def create_standard_retry_decorator(
    max_attempts: int = 3,
    min_wait: int = 1,
    max_wait: int = 10,
):
    """
    Create a standard retry decorator with exponential backoff.

    This is used when --smart flag is NOT set.

    Args:
        max_attempts: Maximum number of retry attempts
        min_wait: Minimum wait time in seconds
        max_wait: Maximum wait time in seconds

    Returns:
        Tenacity retry decorator
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True,  # Re-raise exception after final attempt
    )


def create_smart_retry_decorator(
    analyzer: ExceptionAnalyzer,
    max_attempts: int = 3,
):
    """
    Create a smart retry decorator that uses LLM analysis.

    This is used when --smart flag IS set.

    Args:
        analyzer: ExceptionAnalyzer instance for LLM analysis
        max_attempts: Maximum number of retry attempts

    Returns:
        Tenacity retry decorator with LLM-based decision making
    """

    def should_retry_predicate(exception: BaseException) -> bool:
        """
        Custom predicate that uses LLM to decide if we should retry.

        Args:
            exception: The exception that was raised

        Returns:
            True if should retry, False otherwise
        """
        with logfire.span(
            "smart_retry.retry_decision",
            exception_type=type(exception).__name__,
            exception_message=str(exception)[:200],
        ) as span:
            # Only analyze Exception subclasses, not BaseException (KeyboardInterrupt, etc.)
            if not isinstance(exception, Exception):
                span.set_attribute("skipped", True)
                span.set_attribute("reason", "not_exception_subclass")
                return False

            # Build context (we don't have retry_state here)
            context = {
                "operation": "retry_check",
            }

            # Call LLM analyzer (we need to run async in sync context)
            # Note: Uses asyncio.run() to execute async analyzer in a fresh event loop,
            # avoiding blocking the main event loop with busy-wait patterns.
            try:
                # Run in a new event loop to avoid conflicts
                decision = asyncio.run(analyzer.analyze_exception(exception, context))
            except RuntimeError as e:
                # If we're already in an event loop (edge case in tenacity callback)
                # Fall back to creating a new thread
                logger.warning(f"⚠️  Event loop already running, using thread-based fallback: {e}")
                span.set_attribute("fallback_mode", "thread")
                try:
                    import concurrent.futures

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(
                            asyncio.run, analyzer.analyze_exception(exception, context)
                        )
                        decision = future.result(timeout=30)
                except Exception as thread_error:
                    logger.error(
                        f"❌ Failed to analyze exception with LLM (thread fallback): {thread_error}"
                    )
                    span.set_attribute("error", str(thread_error))
                    span.set_attribute("decision", False)
                    return False
            except Exception as e:
                logger.error(f"❌ Failed to analyze exception with LLM: {e}")
                # Fallback: don't retry if analysis fails
                span.set_attribute("error", str(e))
                span.set_attribute("decision", False)
                return False

            # Log decision
            span.set_attribute("decision", decision.should_retry)
            span.set_attribute("confidence", decision.confidence)
            span.set_attribute("reasoning", decision.reasoning[:200])

            if decision.should_retry:
                logger.warning(
                    f"🔄 LLM recommends RETRY (confidence: {decision.confidence:.2f}): "
                    f"{decision.reasoning}"
                )
                logfire.warn(
                    "Retry recommended by LLM",
                    confidence=decision.confidence,
                    exception_type=type(exception).__name__,
                )
            else:
                logger.error(
                    f"🛑 LLM recommends STOP (confidence: {decision.confidence:.2f}): "
                    f"{decision.reasoning}"
                )
                logfire.error(
                    "Stop recommended by LLM",
                    confidence=decision.confidence,
                    exception_type=type(exception).__name__,
                )

            return decision.should_retry

    return retry(
        stop=stop_after_attempt(max_attempts),
        retry=retry_if_exception(should_retry_predicate),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


class RetryManager:
    """
    Manages retry logic for the application.

    Provides a unified interface for both standard and smart retry modes.
    """

    def __init__(
        self,
        smart_enabled: bool = False,
        max_attempts: int = 3,
        llm_provider: str = "openai",
        llm_model: str = "gpt-4o-mini",
        llm_api_key: str | None = None,
        llm_endpoint: str | None = None,
        timeout_seconds: int = 10,
        enable_caching: bool = True,
    ):
        self.smart_enabled = smart_enabled
        self.max_attempts = max_attempts

        if smart_enabled:
            logger.info("🤖 Smart retry mode ENABLED (using LLM analysis)")
            self.analyzer: ExceptionAnalyzer | None = ExceptionAnalyzer(
                llm_provider=llm_provider,
                llm_model=llm_model,
                llm_api_key=llm_api_key,
                llm_endpoint=llm_endpoint,
                timeout_seconds=timeout_seconds,
                enable_caching=enable_caching,
            )
        else:
            logger.info("🔧 Standard retry mode ENABLED (fixed attempts)")
            self.analyzer = None

    def get_retry_decorator(self):
        """Get the appropriate retry decorator based on configuration."""
        if self.smart_enabled and self.analyzer:
            return create_smart_retry_decorator(
                analyzer=self.analyzer,
                max_attempts=self.max_attempts,
            )
        else:
            return create_standard_retry_decorator(
                max_attempts=self.max_attempts,
            )

    def get_stats(self) -> dict[str, Any]:
        """Get retry manager statistics."""
        stats: dict[str, Any] = {
            "smart_enabled": self.smart_enabled,
            "max_attempts": self.max_attempts,
        }

        if self.analyzer:
            stats["analyzer"] = self.analyzer.get_stats()

        return stats


def create_exception_analyzer(
    llm_provider: str = "openai",
    llm_model: str = "gpt-4o-mini",
    llm_api_key: str | None = None,
    timeout_seconds: int = 10,
    enable_caching: bool = True,
) -> ExceptionAnalyzer:
    """Factory function to create an exception analyzer."""
    return ExceptionAnalyzer(
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_api_key=llm_api_key,
        timeout_seconds=timeout_seconds,
        enable_caching=enable_caching,
    )


# Example usage
if __name__ == "__main__":
    import logging

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    print("Smart retry module loaded successfully")
    print("Use RetryManager to create retry decorators")
