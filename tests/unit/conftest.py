"""
Minimal conftest for streaming unit tests.

This provides only the fixtures needed for testing the streaming module
without requiring all the heavy dependencies.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock


# Mock minimal config classes needed for tests
class MockSnowflakeConfig:
    """Mock SnowflakeConfig for testing."""
    
    def __init__(self, **kwargs):
        self.database = kwargs.get("database", "TEST_DB")
        self.schema_name = kwargs.get("schema_name", "TEST_SCHEMA")
        self.table_name = kwargs.get("table_name", "TEST_TABLE")
        self.batch_size = kwargs.get("batch_size", 1000)
        self.max_retry_attempts = kwargs.get("max_retry_attempts", 3)
        self.retry_delay_seconds = kwargs.get("retry_delay_seconds", 5)
        self.connection_timeout_seconds = kwargs.get("connection_timeout_seconds", 30)
    
    def model_dump(self):
        """Mock model_dump for pydantic-like behavior."""
        return {
            "database": self.database,
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "batch_size": self.batch_size,
            "max_retry_attempts": self.max_retry_attempts,
            "retry_delay_seconds": self.retry_delay_seconds,
            "connection_timeout_seconds": self.connection_timeout_seconds,
        }


class MockSnowflakeConnectionConfig:
    """Mock SnowflakeConnectionConfig for testing."""
    
    def __init__(self, **kwargs):
        self.account = kwargs.get("account", "test-account")
        self.user = kwargs.get("user", "test_user")
        self.private_key_file = kwargs.get("private_key_file", "/tmp/test_key.pem")
        self.private_key_password = kwargs.get("private_key_password", None)
        self.warehouse = kwargs.get("warehouse", "TEST_WH")
        self.database = kwargs.get("database", "TEST_DB")
        self.schema_name = kwargs.get("schema_name", "TEST_SCHEMA")
        self.role = kwargs.get("role", "TEST_ROLE")
        self.pipe_name = kwargs.get("pipe_name", "TEST_PIPE")
    
    def model_copy(self, update=None):
        """Mock model_copy for pydantic-like behavior."""
        data = {
            "account": self.account,
            "user": self.user,
            "private_key_file": self.private_key_file,
            "private_key_password": self.private_key_password,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema_name": self.schema_name,
            "role": self.role,
            "pipe_name": self.pipe_name,
        }
        if update:
            data.update(update)
        return MockSnowflakeConnectionConfig(**data)
    
    def model_dump(self):
        """Mock model_dump for pydantic-like behavior."""
        return {
            "account": self.account,
            "user": self.user,
            "private_key_file": self.private_key_file,
            "private_key_password": self.private_key_password,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema_name": self.schema_name,
            "role": self.role,
            "pipe_name": self.pipe_name,
        }


@pytest.fixture
def sample_snowflake_config():
    """Create a sample Snowflake configuration for testing."""
    return MockSnowflakeConfig(
        database="TEST_DB",
        schema_name="TEST_SCHEMA",
        table_name="TEST_TABLE",
        batch_size=1000,
        max_retry_attempts=3,
        retry_delay_seconds=5,
        connection_timeout_seconds=30,
    )


@pytest.fixture
def sample_snowflake_connection_config(tmp_path):
    """Create a sample Snowflake connection configuration for testing."""
    # Create a temporary private key file
    key_file = tmp_path / "test_key.pem"
    key_file.write_text("-----BEGIN PRIVATE KEY-----\ntest_key_content\n-----END PRIVATE KEY-----")
    
    return MockSnowflakeConnectionConfig(
        account="test-account",
        user="test_user",
        private_key_file=str(key_file),
        private_key_password=None,
        warehouse="TEST_WH",
        database="TEST_DB",
        schema_name="TEST_SCHEMA",
        role="TEST_ROLE",
        pipe_name="TEST_PIPE",
    )
