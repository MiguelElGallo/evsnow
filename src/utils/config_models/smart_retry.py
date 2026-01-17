"""Smart retry configuration.

Kept separate to decouple LLM settings from core pipeline config.
"""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class SmartRetryConfig(BaseSettings):
    """Smart retry configuration with LLM analysis."""

    model_config = {
        "env_prefix": "SMART_RETRY_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
    }

    enabled: bool = Field(default=False, description="Enable LLM-powered smart retry analysis")

    llm_provider: str = Field(
        default="openai",
        description="LLM provider (openai, anthropic, gemini, etc.)",
    )

    llm_model: str = Field(
        default="gpt-4o-mini",
        description="LLM model to use for exception analysis",
    )

    llm_api_key: str | None = Field(default=None, description="API key for LLM provider")

    llm_endpoint: str | None = Field(
        default=None,
        description="Custom endpoint for LLM provider (e.g., Azure OpenAI endpoint)",
    )

    max_attempts: int = Field(default=3, ge=1, le=10, description="Maximum retry attempts")

    timeout_seconds: int = Field(
        default=10,
        ge=1,
        le=60,
        description="Timeout for LLM analysis in seconds",
    )

    enable_caching: bool = Field(default=True, description="Enable caching of LLM decisions")

    @field_validator("llm_provider")
    @classmethod
    def validate_llm_provider(cls, v: str) -> str:
        """Validate LLM provider."""
        supported_providers = ["openai", "azure", "anthropic", "gemini", "groq", "cohere"]
        if v.lower() not in supported_providers:
            raise ValueError(
                f"Unsupported LLM provider: {v}. Supported providers: {', '.join(supported_providers)}"
            )
        return v.lower()

    @field_validator("llm_api_key")
    @classmethod
    def validate_api_key(cls, v: str | None) -> str | None:
        """Validate API key format."""
        if v is not None and not v.strip():
            raise ValueError("LLM API key cannot be empty if provided")
        return v.strip() if v else None
