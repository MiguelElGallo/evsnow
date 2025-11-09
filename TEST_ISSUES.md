# Test Issues to Create

This document contains GitHub issue templates for writing tests for each major source file in the EvSnow project.

---

## Issue 1: Write tests for `src/main.py` (CLI Application)

**Title:** Write tests for CLI application (`src/main.py`)

**Labels:** `testing`, `cli`, `priority:high`

**Description:**

### Overview
Write comprehensive unit tests for the main CLI application that handles command-line interface operations.

### File to Test
- **File:** `src/main.py` (690 LOC)
- **Purpose:** Main CLI application using Typer framework

### Test Requirements

#### Commands to Test
1. `validate_config` - Configuration validation command
2. `run` - Main pipeline execution command
3. `status` - Pipeline status check command
4. `check_credentials` - Azure credential validation command
5. `version` - Version information command

#### Key Scenarios

**Configuration Validation:**
- [ ] Valid configuration returns success
- [ ] Invalid configuration shows errors
- [ ] Missing required environment variables raises error
- [ ] RBAC guidance is displayed when requested

**Pipeline Execution:**
- [ ] Dry run mode displays plan without execution
- [ ] Smart retry mode initializes correctly with LLM
- [ ] Standard retry mode works without LLM
- [ ] Missing LLM API key in smart mode raises error
- [ ] Graceful shutdown on KeyboardInterrupt

**Status Command:**
- [ ] Shows configuration summary
- [ ] Tests Snowflake connection
- [ ] Displays mapping information

**Credential Checking:**
- [ ] Environment credentials detected
- [ ] Azure CLI credentials detected
- [ ] Managed Identity detected
- [ ] Proper priority order shown

#### Mocking Requirements
- Mock all Typer console output
- Mock environment variables (using `monkeypatch`)
- Mock `load_config()` function
- Mock Logfire initialization
- Mock Azure credential classes
- Mock pipeline orchestrator

#### Test File
- **Location:** `tests/unit/test_cli.py`
- **Class:** `TestCLICommands`

### Acceptance Criteria
- [ ] All CLI commands have test coverage
- [ ] Error paths are tested
- [ ] No real services are called
- [ ] Tests run in <5 seconds
- [ ] Code coverage >80%

### References
- [TESTING_STANDARDS.md](./TESTING_STANDARDS.md)
- [Typer Testing Documentation](https://typer.tiangolo.com/tutorial/testing/)

---

## Issue 2: Write tests for `src/consumers/eventhub.py` (EventHub Consumer)

**Title:** Write tests for EventHub consumer (`src/consumers/eventhub.py`)

**Labels:** `testing`, `eventhub`, `priority:high`

**Description:**

### Overview
Write comprehensive unit tests for the EventHub async consumer that handles message consumption and checkpoint management.

### File to Test
- **File:** `src/consumers/eventhub.py` (1274 LOC)
- **Purpose:** Azure EventHub async consumer with Snowflake checkpoint management

### Test Requirements

#### Classes to Test
1. `EventHubMessage` - Message wrapper
2. `MessageBatch` - Batch container
3. `SnowflakeCheckpointManager` - Checkpoint management
4. `SnowflakeCheckpointStore` - Azure SDK-compatible checkpoint store
5. `EventHubAsyncConsumer` - Main consumer class

#### Key Scenarios

**EventHubMessage:**
- [ ] Message creation from EventData
- [ ] to_dict() conversion with all fields
- [ ] Handles bytes in properties correctly
- [ ] Handles None values gracefully

**MessageBatch:**
- [ ] Add messages to batch
- [ ] Batch ready when size limit reached
- [ ] Batch ready when timeout reached
- [ ] Get checkpoint data per partition
- [ ] Convert batch to dict list

**SnowflakeCheckpointManager:**
- [ ] Load last checkpoint from Snowflake
- [ ] Save checkpoint to Snowflake
- [ ] Handle missing checkpoints (return None)
- [ ] Handle multiple partitions
- [ ] Include metadata in checkpoint

**SnowflakeCheckpointStore:**
- [ ] List ownership records
- [ ] Claim ownership for partitions
- [ ] Update checkpoint via SDK
- [ ] List checkpoints from Snowflake
- [ ] Cache checkpoints correctly

**EventHubAsyncConsumer:**
- [ ] Start consumer successfully
- [ ] Initialize checkpoint manager
- [ ] Create EventHub client with credentials
- [ ] Process messages in batches
- [ ] Update checkpoints after processing
- [ ] Handle connection errors gracefully
- [ ] Stop consumer and cleanup resources
- [ ] Process remaining messages on shutdown

#### Mocking Requirements
- Mock `azure.eventhub.aio.EventHubConsumerClient`
- Mock `azure.identity.aio.DefaultAzureCredential`
- Mock Snowflake connection and queries
- Mock `logfire` spans and logging
- Mock Azure EventData objects
- Mock partition context

#### Test File
- **Location:** `tests/unit/test_eventhub.py`
- **Classes:** 
  - `TestEventHubMessage`
  - `TestMessageBatch`
  - `TestSnowflakeCheckpointManager`
  - `TestSnowflakeCheckpointStore`
  - `TestEventHubAsyncConsumer`

### Acceptance Criteria
- [ ] All classes have test coverage
- [ ] Async tests use `pytest.mark.asyncio`
- [ ] Checkpoint logic is thoroughly tested
- [ ] Error handling is verified
- [ ] No real Azure or Snowflake services called
- [ ] Tests run in <10 seconds
- [ ] Code coverage >85%

### References
- [TESTING_STANDARDS.md](./TESTING_STANDARDS.md)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)

---

## Issue 3: Write tests for `src/pipeline/orchestrator.py` (Pipeline Orchestrator)

**Title:** Write tests for pipeline orchestrator (`src/pipeline/orchestrator.py`)

**Labels:** `testing`, `pipeline`, `priority:high`

**Description:**

### Overview
Write comprehensive unit tests for the pipeline orchestrator that manages multiple EventHub-to-Snowflake mappings.

### File to Test
- **File:** `src/pipeline/orchestrator.py` (526 LOC)
- **Purpose:** Coordinates multiple EventHub consumers and Snowflake streaming clients

### Test Requirements

#### Classes to Test
1. `PipelineMapping` - Single EventHub->Snowflake mapping
2. `PipelineOrchestrator` - Main orchestrator managing all mappings
3. `run_pipeline()` - Main entry point function

#### Key Scenarios

**PipelineMapping:**
- [ ] Initialize with valid configuration
- [ ] Raise error on invalid mapping
- [ ] Start mapping components
- [ ] Process messages through Snowflake client
- [ ] Track statistics correctly
- [ ] Stop mapping gracefully
- [ ] Health check returns status

**PipelineOrchestrator:**
- [ ] Initialize all mappings from config
- [ ] Start orchestrator successfully
- [ ] Run async pipeline with multiple tasks
- [ ] Handle task failures gracefully
- [ ] Stop all mappings on shutdown
- [ ] Aggregate statistics from mappings
- [ ] Setup signal handlers (SIGINT, SIGTERM)
- [ ] Graceful shutdown on signal

**run_pipeline():**
- [ ] Create orchestrator with config
- [ ] Setup signal handlers
- [ ] Run pipeline until cancelled
- [ ] Cleanup on error
- [ ] Cleanup on keyboard interrupt

#### Mocking Requirements
- Mock `EventHubAsyncConsumer`
- Mock `SnowflakeStreamingClient`
- Mock configuration objects
- Mock `asyncio` tasks and event loops
- Mock signal handlers
- Mock `logfire` spans

#### Test File
- **Location:** `tests/unit/test_orchestrator.py`
- **Classes:**
  - `TestPipelineMapping`
  - `TestPipelineOrchestrator`
  - `TestRunPipeline`

### Acceptance Criteria
- [ ] All classes and functions tested
- [ ] Async operations tested properly
- [ ] Signal handling tested
- [ ] Error recovery tested
- [ ] Statistics tracking verified
- [ ] No real services called
- [ ] Tests run in <10 seconds
- [ ] Code coverage >85%

### References
- [TESTING_STANDARDS.md](./TESTING_STANDARDS.md)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)

---

## Issue 4: Write tests for `src/streaming/snowflake_high_performance.py` (Snowflake Client)

**Title:** Write tests for Snowflake high-performance streaming client

**Labels:** `testing`, `snowflake`, `priority:high`

**Description:**

### Overview
Write comprehensive unit tests for the Snowflake high-performance streaming client.

### File to Test
- **File:** `src/streaming/snowflake_high_performance.py` (622 LOC)
- **Purpose:** Snowflake streaming using high-performance SDK with PIPE objects

### Test Requirements

#### Class to Test
1. `SnowflakeHighPerformanceStreamingClient` - Main streaming client

#### Key Scenarios

**Client Lifecycle:**
- [ ] Initialize client with config
- [ ] Start client successfully
- [ ] Build connection profile correctly
- [ ] Create StreamingIngestClient
- [ ] Stop client and cleanup channels
- [ ] Handle start errors gracefully

**Channel Management:**
- [ ] Create channel for partition
- [ ] Reuse existing channel
- [ ] Track channel statistics
- [ ] Handle channel creation errors

**Data Ingestion:**
- [ ] Ingest batch successfully
- [ ] Handle empty batches
- [ ] Retry on transient errors (if retry_manager provided)
- [ ] Fail permanently on fatal errors
- [ ] Update statistics after ingestion
- [ ] Generate unique row IDs

**Configuration:**
- [ ] Load private key from file
- [ ] Handle encrypted private keys
- [ ] Validate required pipe_name
- [ ] Build correct connection profile

#### Mocking Requirements
- Mock `snowflake.ingest.streaming.StreamingIngestClient`
- Mock Snowflake connection and channels
- Mock private key loading
- Mock `logfire` spans
- Mock retry manager
- Mock file operations (tempfile)

#### Test File
- **Location:** `tests/unit/test_snowflake_client.py`
- **Class:** `TestSnowflakeHighPerformanceStreamingClient`

### Acceptance Criteria
- [ ] All methods tested
- [ ] Error paths verified
- [ ] Private key handling tested
- [ ] Channel management tested
- [ ] No real Snowflake connections
- [ ] Tests run in <5 seconds
- [ ] Code coverage >85%

### References
- [TESTING_STANDARDS.md](./TESTING_STANDARDS.md)
- [Snowflake SDK Documentation](https://docs.snowflake.com/en/user-guide/snowpipe-streaming)

---

## Issue 5: Write tests for `src/utils/config.py` (Configuration Management)

**Title:** Write tests for configuration management (`src/utils/config.py`)

**Labels:** `testing`, `config`, `priority:high`

**Description:**

### Overview
Write comprehensive unit tests for the Pydantic-based configuration management system.

### File to Test
- **File:** `src/utils/config.py` (728 LOC)
- **Purpose:** Configuration validation and management using Pydantic

### Test Requirements

#### Classes to Test
1. `SnowflakeConnectionConfig` - Snowflake connection settings
2. `SmartRetryConfig` - Smart retry configuration
3. `LogfireConfig` - Logfire observability config
4. `EventHubConfig` - EventHub settings
5. `SnowflakeConfig` - Snowflake destination config
6. `EventHubSnowflakeMapping` - Mapping configuration
7. `EvSnowConfig` - Main configuration class

#### Key Scenarios

**SnowflakeConnectionConfig:**
- [ ] Load from environment variables
- [ ] Validate private key file exists
- [ ] Validate account format
- [ ] Validate identifiers
- [ ] Handle missing required fields

**SmartRetryConfig:**
- [ ] Load retry settings
- [ ] Validate LLM provider
- [ ] Validate API key format
- [ ] Default values applied correctly

**LogfireConfig:**
- [ ] Load observability settings
- [ ] Validate log level
- [ ] Require token when sending to cloud
- [ ] Handle disabled state

**EventHubConfig:**
- [ ] Validate namespace format
- [ ] Validate EventHub name
- [ ] Require consumer group
- [ ] Default values for performance settings

**SnowflakeConfig:**
- [ ] Validate Snowflake identifiers
- [ ] Default batch size
- [ ] Retry settings

**EventHubSnowflakeMapping:**
- [ ] Validate mapping key format
- [ ] Create channel name pattern

**EvSnowConfig:**
- [ ] Parse dynamic EventHub configs from env
- [ ] Parse dynamic Snowflake configs from env
- [ ] Auto-create mappings
- [ ] Validate mappings reference existing configs
- [ ] Validate configuration completeness
- [ ] Generate channel names

#### Mocking Requirements
- Mock environment variables (use `monkeypatch`)
- Mock file system for private key validation
- Mock `python-dotenv` loading

#### Test File
- **Location:** `tests/unit/test_config.py`
- **Classes:**
  - `TestSnowflakeConnectionConfig`
  - `TestSmartRetryConfig`
  - `TestLogfireConfig`
  - `TestEventHubConfig`
  - `TestSnowflakeConfig`
  - `TestEventHubSnowflakeMapping`
  - `TestEvSnowConfig`

### Acceptance Criteria
- [ ] All config classes tested
- [ ] Validation logic verified
- [ ] Environment variable parsing tested
- [ ] Error cases covered
- [ ] No real files or services accessed
- [ ] Tests run in <5 seconds
- [ ] Code coverage >90%

### References
- [TESTING_STANDARDS.md](./TESTING_STANDARDS.md)
- [Pydantic Documentation](https://docs.pydantic.dev/)

---

## Issue 6: Write tests for `src/utils/smart_retry.py` (Smart Retry Logic)

**Title:** Write tests for smart retry logic (`src/utils/smart_retry.py`)

**Labels:** `testing`, `retry`, `llm`, `priority:medium`

**Description:**

### Overview
Write comprehensive unit tests for the LLM-powered smart retry mechanism.

### File to Test
- **File:** `src/utils/smart_retry.py` (559 LOC)
- **Purpose:** Intelligent retry logic using LLM analysis

### Test Requirements

#### Classes to Test
1. `RetryDecision` - LLM response model
2. `ExceptionAnalyzer` - LLM-based exception analyzer
3. `RetryManager` - Retry manager for both smart and standard modes
4. Factory functions for retry decorators

#### Key Scenarios

**RetryDecision:**
- [ ] Create decision with all fields
- [ ] Validate field constraints (confidence 0-1, wait 1-60)

**ExceptionAnalyzer:**
- [ ] Initialize with LLM config
- [ ] Analyze retryable exceptions
- [ ] Analyze fatal exceptions
- [ ] Cache decisions when enabled
- [ ] Handle LLM timeout
- [ ] Handle LLM API errors
- [ ] Build context string correctly
- [ ] Support different LLM providers (OpenAI, Azure, Anthropic)

**RetryManager:**
- [ ] Create standard retry decorator
- [ ] Create smart retry decorator
- [ ] Use correct mode based on config
- [ ] Track retry statistics

**Retry Decorators:**
- [ ] Standard decorator retries fixed attempts
- [ ] Smart decorator uses LLM decision
- [ ] Exponential backoff applied
- [ ] Reraise after exhaustion

#### Mocking Requirements
- Mock `pydantic_ai.Agent`
- Mock LLM API calls
- Mock `logfire` spans
- Mock exception analysis results
- Mock `tenacity` retry behavior

#### Test File
- **Location:** `tests/unit/test_smart_retry.py`
- **Classes:**
  - `TestRetryDecision`
  - `TestExceptionAnalyzer`
  - `TestRetryManager`
  - `TestRetryDecorators`

### Acceptance Criteria
- [ ] All classes tested
- [ ] LLM integration mocked properly
- [ ] Caching logic verified
- [ ] Timeout handling tested
- [ ] Both smart and standard modes tested
- [ ] No real LLM API calls
- [ ] Tests run in <5 seconds
- [ ] Code coverage >85%

### References
- [TESTING_STANDARDS.md](./TESTING_STANDARDS.md)
- [Pydantic AI Documentation](https://ai.pydantic.dev/)

---

## Issue 7: Write tests for `src/utils/snowflake.py` (Snowflake Utilities)

**Title:** Write tests for Snowflake utilities (`src/utils/snowflake.py`)

**Labels:** `testing`, `snowflake`, `utilities`, `priority:high`

**Description:**

### Overview
Write comprehensive unit tests for Snowflake connection and checkpoint utilities.

### File to Test
- **File:** `src/utils/snowflake.py` (682 LOC)
- **Purpose:** Snowflake connection utilities and checkpoint management

### Test Requirements

#### Functions to Test
1. `load_private_key()` - Load and decrypt private keys
2. `get_connection()` - Create/cache Snowflake connections
3. `get_snowpark_session()` - Create Snowpark sessions
4. `check_connection()` - Test connection
5. `create_control_table()` - Create checkpoint table
6. `insert_partition_checkpoint()` - Save checkpoint
7. `get_partition_checkpoints()` - Load checkpoints
8. Connection caching utilities

#### Key Scenarios

**Private Key Loading:**
- [ ] Load unencrypted private key
- [ ] Load encrypted private key with password
- [ ] Handle invalid key file
- [ ] Handle missing key file
- [ ] Convert to DER format

**Connection Management:**
- [ ] Create new connection
- [ ] Reuse cached connection
- [ ] Detect stale connections
- [ ] Handle connection errors
- [ ] Clear connection cache

**Snowpark Sessions:**
- [ ] Create session successfully
- [ ] Handle missing snowpark library
- [ ] Activate warehouse

**Connection Testing:**
- [ ] Test valid connection
- [ ] Verify database context
- [ ] Handle connection failures

**Control Table:**
- [ ] Create hybrid table with schema
- [ ] Handle existing table
- [ ] Create schema if missing
- [ ] Handle creation errors

**Checkpoint Operations:**
- [ ] Insert new checkpoint
- [ ] Update existing checkpoint (MERGE)
- [ ] Retrieve checkpoints by partition
- [ ] Handle metadata JSON
- [ ] Filter by target table

#### Mocking Requirements
- Mock `snowflake.connector.connect()`
- Mock Snowflake cursor and queries
- Mock Snowpark Session
- Mock file system for private keys
- Mock cryptography module
- Mock connection caching

#### Test File
- **Location:** `tests/unit/test_snowflake_utils.py`
- **Classes:**
  - `TestPrivateKeyLoading`
  - `TestConnectionManagement`
  - `TestSnowparkSessions`
  - `TestControlTable`
  - `TestCheckpointOperations`

### Acceptance Criteria
- [ ] All functions tested
- [ ] Connection caching verified
- [ ] Private key handling tested
- [ ] SQL operations verified with mocks
- [ ] Error handling tested
- [ ] No real Snowflake connections
- [ ] Tests run in <5 seconds
- [ ] Code coverage >85%

### References
- [TESTING_STANDARDS.md](./TESTING_STANDARDS.md)
- [Snowflake Connector Documentation](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)

---

## Issue 8: Write tests for streaming modules (`src/streaming/`)

**Title:** Write tests for streaming factory and base classes

**Labels:** `testing`, `streaming`, `priority:medium`

**Description:**

### Overview
Write comprehensive unit tests for the streaming module factory and base classes.

### Files to Test
- **File 1:** `src/streaming/base.py` (116 LOC) - Abstract base class
- **File 2:** `src/streaming/factory.py` (87 LOC) - Client factory
- **File 3:** `src/streaming/snowflake.py` (30 LOC) - Facade module

### Test Requirements

#### Classes/Functions to Test
1. `SnowflakeStreamingClientBase` - Abstract base class
2. `create_snowflake_client()` - Factory function
3. Module exports and imports

#### Key Scenarios

**Base Class:**
- [ ] Cannot instantiate abstract class directly
- [ ] Subclasses must implement all abstract methods
- [ ] Constructor initializes fields correctly

**Factory Function:**
- [ ] Create high-performance client with valid config
- [ ] Require pipe_name for high-performance mode
- [ ] Pass retry_manager to client
- [ ] Pass client_name_suffix correctly
- [ ] Handle missing config

**Facade Module:**
- [ ] Exports correct classes
- [ ] Re-exports factory function
- [ ] Imports work correctly

#### Mocking Requirements
- Mock Snowflake client implementations
- Mock configuration objects

#### Test File
- **Location:** `tests/unit/test_streaming.py`
- **Classes:**
  - `TestSnowflakeStreamingClientBase`
  - `TestStreamingFactory`
  - `TestStreamingFacade`

### Acceptance Criteria
- [ ] Abstract class behavior tested
- [ ] Factory logic verified
- [ ] Exports validated
- [ ] Error cases covered
- [ ] No real services called
- [ ] Tests run in <3 seconds
- [ ] Code coverage >80%

### References
- [TESTING_STANDARDS.md](./TESTING_STANDARDS.md)

---

## Issue 9: Write integration tests for end-to-end pipeline

**Title:** Write integration tests for end-to-end pipeline flow

**Labels:** `testing`, `integration`, `priority:medium`

**Description:**

### Overview
Write integration tests that verify the complete pipeline flow from EventHub to Snowflake using mocks.

### Test Requirements

#### Integration Scenarios
1. **Complete Pipeline Flow:**
   - [ ] EventHub receives messages
   - [ ] Messages batched correctly
   - [ ] Snowflake ingestion succeeds
   - [ ] Checkpoints saved after ingestion
   - [ ] Pipeline stats tracked

2. **Multi-Mapping Pipeline:**
   - [ ] Multiple EventHub-Snowflake mappings run concurrently
   - [ ] Each mapping operates independently
   - [ ] Mappings share no state

3. **Error Recovery:**
   - [ ] Transient Snowflake errors trigger retry
   - [ ] Fatal errors stop pipeline
   - [ ] Checkpoints preserved on error
   - [ ] Remaining messages processed on shutdown

4. **Configuration Flow:**
   - [ ] Load config from environment
   - [ ] Validate configuration
   - [ ] Create mappings from config
   - [ ] Initialize all components

#### Mocking Requirements
- Mock entire Azure EventHub SDK
- Mock entire Snowflake SDK
- Mock Logfire completely
- Mock Azure credentials
- Use real configuration parsing (with test env vars)

#### Test File
- **Location:** `tests/integration/test_pipeline_integration.py`
- **Classes:**
  - `TestEndToEndPipeline`
  - `TestMultiMappingPipeline`
  - `TestErrorRecovery`
  - `TestConfigurationFlow`

### Acceptance Criteria
- [ ] End-to-end flow tested
- [ ] All components integrated
- [ ] Error recovery verified
- [ ] Concurrent processing tested
- [ ] No real services called
- [ ] Tests run in <30 seconds
- [ ] Clear test documentation

### References
- [TESTING_STANDARDS.md](./TESTING_STANDARDS.md)

---

## Summary

### Test Coverage Goals
- **Overall:** >80% code coverage
- **Critical modules:** >90% coverage
- **Test execution time:** <60 seconds total

### Priority Order
1. **High Priority (Do First):**
   - Issue 1: CLI (`src/main.py`)
   - Issue 2: EventHub Consumer (`src/consumers/eventhub.py`)
   - Issue 3: Pipeline Orchestrator (`src/pipeline/orchestrator.py`)
   - Issue 4: Snowflake Client (`src/streaming/snowflake_high_performance.py`)
   - Issue 5: Configuration (`src/utils/config.py`)
   - Issue 7: Snowflake Utils (`src/utils/snowflake.py`)

2. **Medium Priority:**
   - Issue 6: Smart Retry (`src/utils/smart_retry.py`)
   - Issue 8: Streaming modules (`src/streaming/`)
   - Issue 9: Integration tests

### Next Steps
1. Create all GitHub issues using the templates above
2. Assign issues to team members
3. Set up CI/CD pipeline to run tests
4. Monitor test coverage as PRs are merged
5. Review and update `TESTING_STANDARDS.md` as needed
