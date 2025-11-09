"""
Tests for streaming module (base class, factory, and facade) - Isolated version.

This module tests the streaming client infrastructure using isolated imports
and comprehensive mocking to avoid dependency issues.

All tests use mocks to avoid calling real Snowflake services.
"""

import sys
import pytest
from abc import ABC, abstractmethod
from typing import Any
from unittest.mock import MagicMock, Mock, patch, call
from pathlib import Path

# Mock all heavy dependencies before any imports
sys.modules['logfire'] = MagicMock()
sys.modules['snowflake'] = MagicMock()
sys.modules['snowflake.ingest'] = MagicMock()
sys.modules['snowflake.ingest.streaming'] = MagicMock()
sys.modules['snowflake.connector'] = MagicMock()
sys.modules['snowflake.snowpark'] = MagicMock()
sys.modules['azure'] = MagicMock()
sys.modules['azure.eventhub'] = MagicMock()
sys.modules['azure.identity'] = MagicMock()
sys.modules['pydantic_ai'] = MagicMock()
sys.modules['tenacity'] = MagicMock()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))


@pytest.mark.unit
class TestSnowflakeStreamingClientBase:
    """Tests for SnowflakeStreamingClientBase abstract class."""

    def test_cannot_instantiate_abstract_class_directly(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that abstract base class cannot be instantiated directly."""
        from streaming.base import SnowflakeStreamingClientBase
        
        with pytest.raises(TypeError) as exc_info:
            SnowflakeStreamingClientBase(
                snowflake_config=sample_snowflake_config,
                connection_config=sample_snowflake_connection_config,
            )
        
        # Python error message differs by version, but should mention abstract
        error_msg = str(exc_info.value).lower()
        assert "abstract" in error_msg or "can't instantiate" in error_msg

    def test_subclass_must_implement_all_abstract_methods(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that subclass without all abstract methods cannot be instantiated."""
        from streaming.base import SnowflakeStreamingClientBase
        
        class IncompleteClient(SnowflakeStreamingClientBase):
            """Incomplete implementation missing some abstract methods."""
            
            def start(self) -> None:
                pass
            
            # Missing: stop, ingest_batch, get_stats, is_started
        
        with pytest.raises(TypeError) as exc_info:
            IncompleteClient(
                snowflake_config=sample_snowflake_config,
                connection_config=sample_snowflake_connection_config,
            )
        
        error_msg = str(exc_info.value).lower()
        assert "abstract" in error_msg or "can't instantiate" in error_msg

    def test_constructor_initializes_fields_correctly(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that constructor initializes all fields correctly."""
        from streaming.base import SnowflakeStreamingClientBase
        
        class ConcreteClient(SnowflakeStreamingClientBase):
            """Complete implementation for testing."""
            
            def start(self) -> None:
                pass
            
            def stop(self) -> None:
                pass
            
            def ingest_batch(
                self,
                channel_name: str,
                data_batch: list[dict[str, Any]],
                partition_id: str = "0",
            ) -> bool:
                return True
            
            def get_stats(self) -> dict[str, Any]:
                return {}
            
            @property
            def is_started(self) -> bool:
                return True
        
        # Create instance with all parameters
        retry_manager = Mock()
        client = ConcreteClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            client_name_suffix="test-suffix",
            retry_manager=retry_manager,
        )
        
        # Verify all fields are initialized
        assert client.snowflake_config == sample_snowflake_config
        assert client.connection_config == sample_snowflake_connection_config
        assert client.client_name_suffix == "test-suffix"
        assert client.retry_manager is retry_manager

    def test_constructor_with_minimal_parameters(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test constructor with only required parameters."""
        from streaming.base import SnowflakeStreamingClientBase
        
        class ConcreteClient(SnowflakeStreamingClientBase):
            """Complete implementation for testing."""
            
            def start(self) -> None:
                pass
            
            def stop(self) -> None:
                pass
            
            def ingest_batch(
                self,
                channel_name: str,
                data_batch: list[dict[str, Any]],
                partition_id: str = "0",
            ) -> bool:
                return True
            
            def get_stats(self) -> dict[str, Any]:
                return {}
            
            @property
            def is_started(self) -> bool:
                return False
        
        # Create with minimal parameters
        client = ConcreteClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )
        
        # Verify required fields are set
        assert client.snowflake_config == sample_snowflake_config
        assert client.connection_config == sample_snowflake_connection_config
        
        # Verify optional fields are None
        assert client.client_name_suffix is None
        assert client.retry_manager is None

    def test_concrete_subclass_implements_all_methods(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that a complete concrete subclass works correctly."""
        from streaming.base import SnowflakeStreamingClientBase
        
        class FullClient(SnowflakeStreamingClientBase):
            """Full implementation for testing."""
            
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._started = False
            
            def start(self) -> None:
                self._started = True
            
            def stop(self) -> None:
                self._started = False
            
            def ingest_batch(
                self,
                channel_name: str,
                data_batch: list[dict[str, Any]],
                partition_id: str = "0",
            ) -> bool:
                return len(data_batch) > 0
            
            def get_stats(self) -> dict[str, Any]:
                return {"started": self._started}
            
            @property
            def is_started(self) -> bool:
                return self._started
        
        # Create and test
        client = FullClient(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )
        
        # Test lifecycle
        assert not client.is_started
        client.start()
        assert client.is_started
        client.stop()
        assert not client.is_started
        
        # Test ingest_batch
        assert client.ingest_batch("channel", [{"data": 1}])
        assert not client.ingest_batch("channel", [])
        
        # Test get_stats
        client.start()
        stats = client.get_stats()
        assert stats == {"started": True}


@pytest.mark.unit
class TestStreamingFactory:
    """Tests for create_snowflake_client() factory function."""

    @patch("streaming.factory.SnowflakeHighPerformanceStreamingClient")
    def test_creates_high_performance_client_with_valid_config(
        self,
        mock_hp_client_class,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test factory creates high-performance client with valid config."""
        from streaming.factory import create_snowflake_client
        
        # Setup mock
        mock_client_instance = MagicMock()
        mock_hp_client_class.return_value = mock_client_instance
        
        # Call factory
        result = create_snowflake_client(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )
        
        # Verify high-performance client was created
        mock_hp_client_class.assert_called_once_with(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            client_name_suffix=None,
            retry_manager=None,
        )
        assert result is mock_client_instance

    def test_requires_pipe_name_for_high_performance_mode(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that pipe_name is required for high-performance mode."""
        from streaming.factory import create_snowflake_client
        
        # Remove pipe_name from config
        config_without_pipe = sample_snowflake_connection_config.model_copy(
            update={"pipe_name": None}
        )
        
        # Should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            create_snowflake_client(
                snowflake_config=sample_snowflake_config,
                connection_config=config_without_pipe,
            )
        
        error_msg = str(exc_info.value)
        assert "pipe_name" in error_msg.lower()
        assert "required" in error_msg.lower()
        assert "SNOWFLAKE_PIPE_NAME" in error_msg

    @patch("streaming.factory.SnowflakeHighPerformanceStreamingClient")
    def test_passes_retry_manager_to_client(
        self,
        mock_hp_client_class,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that retry_manager is passed to the client."""
        from streaming.factory import create_snowflake_client
        
        mock_retry_manager = Mock()
        
        create_snowflake_client(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            retry_manager=mock_retry_manager,
        )
        
        # Verify retry_manager was passed
        mock_hp_client_class.assert_called_once()
        call_kwargs = mock_hp_client_class.call_args.kwargs
        assert call_kwargs["retry_manager"] is mock_retry_manager

    @patch("streaming.factory.SnowflakeHighPerformanceStreamingClient")
    def test_passes_client_name_suffix_correctly(
        self,
        mock_hp_client_class,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that client_name_suffix is passed correctly."""
        from streaming.factory import create_snowflake_client
        
        create_snowflake_client(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            client_name_suffix="test-suffix",
        )
        
        # Verify client_name_suffix was passed
        mock_hp_client_class.assert_called_once()
        call_kwargs = mock_hp_client_class.call_args.kwargs
        assert call_kwargs["client_name_suffix"] == "test-suffix"

    @patch("streaming.factory.SnowflakeHighPerformanceStreamingClient")
    def test_creates_client_with_all_parameters(
        self,
        mock_hp_client_class,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test factory with all optional parameters provided."""
        from streaming.factory import create_snowflake_client
        
        mock_retry_manager = Mock()
        mock_client_instance = MagicMock()
        mock_hp_client_class.return_value = mock_client_instance
        
        result = create_snowflake_client(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            client_name_suffix="full-test",
            retry_manager=mock_retry_manager,
        )
        
        # Verify all parameters were passed
        mock_hp_client_class.assert_called_once_with(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
            client_name_suffix="full-test",
            retry_manager=mock_retry_manager,
        )
        assert result is mock_client_instance

    @patch("streaming.factory.SnowflakeHighPerformanceStreamingClient")
    @patch("streaming.factory.logger")
    def test_logs_creation_message(
        self,
        mock_logger,
        mock_hp_client_class,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that factory logs informational messages."""
        from streaming.factory import create_snowflake_client
        
        create_snowflake_client(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )
        
        # Check that info logging was called
        assert mock_logger.info.called
        # Get all info log messages
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        log_messages = " ".join(log_calls)
        assert "HIGH-PERFORMANCE" in log_messages or sample_snowflake_config.database in log_messages

    def test_raises_error_with_empty_pipe_name(
        self,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that empty string pipe_name is treated as missing."""
        from streaming.factory import create_snowflake_client
        
        # Create config with empty pipe_name
        config_empty_pipe = sample_snowflake_connection_config.model_copy(
            update={"pipe_name": ""}
        )
        
        # Should raise ValueError (empty string is falsy)
        with pytest.raises(ValueError) as exc_info:
            create_snowflake_client(
                snowflake_config=sample_snowflake_config,
                connection_config=config_empty_pipe,
            )
        
        assert "pipe_name" in str(exc_info.value).lower()


@pytest.mark.unit
class TestStreamingFacade:
    """Tests for streaming.snowflake facade module."""

    def test_exports_snowflake_streaming_client(self):
        """Test that facade exports SnowflakeStreamingClient."""
        import streaming.snowflake as snowflake_facade
        
        # Verify SnowflakeStreamingClient is exported
        assert hasattr(snowflake_facade, "SnowflakeStreamingClient")
        
        # Verify it's the base class
        from streaming.base import SnowflakeStreamingClientBase
        assert snowflake_facade.SnowflakeStreamingClient is SnowflakeStreamingClientBase

    def test_exports_create_snowflake_streaming_client(self):
        """Test that facade exports create_snowflake_streaming_client."""
        import streaming.snowflake as snowflake_facade
        
        # Verify function is exported
        assert hasattr(snowflake_facade, "create_snowflake_streaming_client")
        
        # Verify it's the factory function
        from streaming.factory import create_snowflake_client
        assert snowflake_facade.create_snowflake_streaming_client is create_snowflake_client

    def test_module_all_list(self):
        """Test that __all__ contains expected exports."""
        import streaming.snowflake as snowflake_facade
        
        assert hasattr(snowflake_facade, "__all__")
        assert "SnowflakeStreamingClient" in snowflake_facade.__all__
        assert "create_snowflake_streaming_client" in snowflake_facade.__all__

    def test_module_all_list_length(self):
        """Test that __all__ contains exactly 2 exports."""
        import streaming.snowflake as snowflake_facade
        
        assert len(snowflake_facade.__all__) == 2

    @patch("streaming.factory.SnowflakeHighPerformanceStreamingClient")
    def test_can_import_and_use_facade_function(
        self,
        mock_hp_client_class,
        sample_snowflake_config,
        sample_snowflake_connection_config,
    ):
        """Test that facade function can be imported and used."""
        import streaming.snowflake as snowflake_facade
        
        mock_client_instance = MagicMock()
        mock_hp_client_class.return_value = mock_client_instance
        
        # Use the facade function
        result = snowflake_facade.create_snowflake_streaming_client(
            snowflake_config=sample_snowflake_config,
            connection_config=sample_snowflake_connection_config,
        )
        
        # Verify it works like the original
        assert result is mock_client_instance
        mock_hp_client_class.assert_called_once()

    def test_facade_imports_work_correctly(self):
        """Test that imports from facade work correctly."""
        # This tests the actual import statements work
        from streaming.snowflake import (
            SnowflakeStreamingClient,
            create_snowflake_streaming_client,
        )
        
        # Verify they are the correct objects
        from streaming.base import SnowflakeStreamingClientBase
        from streaming.factory import create_snowflake_client
        
        assert SnowflakeStreamingClient is SnowflakeStreamingClientBase
        assert create_snowflake_streaming_client is create_snowflake_client

    def test_facade_maintains_backward_compatibility(self):
        """Test that facade maintains backward compatibility with old import paths."""
        # Test old import style still works
        from streaming.snowflake import SnowflakeStreamingClient
        
        # Verify it's still an abstract base class
        assert hasattr(SnowflakeStreamingClient, "__abstractmethods__")
        
        # Verify abstract methods are present
        abstract_methods = SnowflakeStreamingClient.__abstractmethods__
        assert "start" in abstract_methods
        assert "stop" in abstract_methods
        assert "ingest_batch" in abstract_methods
        assert "get_stats" in abstract_methods
        assert "is_started" in abstract_methods
