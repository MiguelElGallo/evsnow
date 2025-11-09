# Troubleshooting Guide

## Common Issues and Solutions

### 1. "amqp:link:stolen" - Competing Consumers Error

**Symptom:**

# Troubleshooting Guide

## Common Issues and Solutions

### 1. "amqp:link:stolen" - Competing Consumers Error

**Symptom:**

```text
ConnectionLostError("New receiver 'nil' with higher epoch of '0' is created hence current receiver 'nil' with epoch '0' is getting disconnected...")
Error condition: amqp:link:stolen
```

**Cause:**
Multiple consumers are trying to read from the same EventHub partition using the same consumer group. Azure EventHub only allows **ONE consumer per partition per consumer group** at any given time.

**Common Scenarios:**

1. ✗ Running multiple instances of the application simultaneously
2. ✗ Previous instance crashed but connection hasn't timed out yet
3. ✗ Zombie process from previous run still connected
4. ✗ Testing locally while production instance is running

**Solutions:**

#### Option 1: Kill Competing Consumers

```bash
# Find all running evsnow processes
ps aux | grep evsnow

# Kill specific process
kill <PID>

# Or kill all evsnow processes
pkill -f evsnow
```

Wait 5-10 minutes for old connections to timeout, then restart.

#### Option 2: Use Different Consumer Groups

Each consumer group can have its own set of consumers. Change the consumer group in your `.env` file:

```bash
# Before
EVENTHUBNAME_1_CONSUMER_GROUP="$Default"

# After - use a unique name per environment/instance
EVENTHUBNAME_1_CONSUMER_GROUP="dev-miguel"
# or
EVENTHUBNAME_1_CONSUMER_GROUP="prod-instance-1"
```

**Note:** Each consumer group maintains its own checkpoints, so using a different consumer group means starting from scratch (or LATEST position).

#### Option 3: Check for Zombie Processes

```bash
# Check processes using EventHub ports
lsof -i -P | grep ESTABLISHED | grep eventhub

# Force cleanup of all Python processes (careful!)
pkill -9 python
```

### 2. `KeyError: 'channels_created'` - Missing Stats Key

**Symptom:**

```text
KeyError: 'channels_created'
```

**Cause:**
The `stats` dictionary in `SnowflakeHighPerformanceStreamingClient` was missing the `"channels_created"` key initialization.

**Fix:**
This has been fixed in commit XXX. Update your code or ensure the stats dictionary includes:

```python
self.stats: dict[str, Any] = {
    "client_created_at": None,
    "total_messages_sent": 0,
    "total_batches_sent": 0,
    "channels_created": 0,  # ← This was missing
    "last_ingestion": None,
    "retry_stats": {...},
}
```

### 3. Repeated "Connecting to Snowflake" Messages - Connection Pooling

**Symptom:**
You see many messages like:

```text
Connecting to Snowflake account: KPWHYJX-YU88540
Successfully connected to Snowflake account: KPWHYJX-YU88540
```

**Cause:**
The application was creating new Snowflake connections for every checkpoint save operation, which is inefficient in high-performance streaming scenarios.

**Fix:**
This has been optimized with connection pooling. The application now:

- Caches Snowflake connections and reuses them
- Only creates new connections when necessary (first use or if connection died)
- Automatically cleans up connections on shutdown

**Benefits:**

- **Faster checkpoint saves** - No authentication overhead for each save
- **Reduced load on Snowflake** - Fewer connection handshakes
- **Better performance** - Connection reuse eliminates ~100-200ms per checkpoint

**Manual Connection Management:**
If you need to force close all cached connections:

```python
from utils.snowflake import close_all_cached_connections
close_all_cached_connections()
```

### 4. Reducing Log Verbosity

If you're seeing too many detailed logs, you can adjust the log level:

**Option 1: Environment Variable**

```bash
export LOG_LEVEL=WARNING
uv run evsnow run
```

**Option 2: Edit main.py**

Change the logging level in your code:

```python
logging.basicConfig(level=logging.WARNING)  # Instead of INFO or DEBUG
```

**Option 3: Suppress Specific Loggers**

For Azure SDK logs specifically:

```python
import logging
logging.getLogger('azure.eventhub').setLevel(logging.ERROR)
logging.getLogger('azure.core').setLevel(logging.ERROR)
```

### 5. Checkpoint Issues - Not Resuming from Last Position

**Symptom:**
Consumer starts processing from LATEST instead of resuming from last checkpoint.

**Diagnosis:**

```bash
# Check if checkpoints exist in Snowflake
uv run python -c "
from utils.snowflake import get_partition_checkpoints
from utils.config import load_config

config = load_config()
checkpoints = get_partition_checkpoints(
    eventhub_namespace='YOUR_NAMESPACE',
    eventhub='YOUR_EVENTHUB',
    target_db='YOUR_DB',
    target_schema='YOUR_SCHEMA',
    target_table='YOUR_TABLE',
    config=config.snowflake_connection
)
print('Checkpoints:', checkpoints)
"
```

**Possible Causes:**

1. Different consumer group (each has separate checkpoints)
2. Checkpoints not being saved (check for save errors in logs)
3. Control table schema mismatch
4. Connection to Snowflake failing during checkpoint save

## Getting Help

If you continue experiencing issues:

1. **Check Logs:** Look for ERROR level messages with detailed context
2. **Consumer ID:** Each consumer logs its unique ID (e.g., `evsnow_a1b2c3d4`) - use this to track competing consumers
3. **Stats:** Run `uv run evsnow status` to see current statistics
4. **Health Check:** The orchestrator provides health check data in logs

## Debug Mode

Enable debug mode for maximum verbosity:

```bash
export LOG_LEVEL=DEBUG
export AZURE_LOG_LEVEL=DEBUG
uv run evsnow run
```

This will show:

- Every message received
- Checkpoint save operations
- Batch processing details
- Azure SDK internals
- Connection pooling activity


**Cause:**
Multiple consumers are trying to read from the same EventHub partition using the same consumer group. Azure EventHub only allows **ONE consumer per partition per consumer group** at any given time.

**Common Scenarios:**

1. ✗ Running multiple instances of the application simultaneously
2. ✗ Previous instance crashed but connection hasn't timed out yet
3. ✗ Zombie process from previous run still connected
4. ✗ Testing locally while production instance is running

**Solutions:**

#### Option 1: Kill Competing Consumers

```bash
# Find all running evsnow processes
ps aux | grep evsnow

# Kill specific process
kill <PID>

# Or kill all evsnow processes
pkill -f evsnow
```

Wait 5-10 minutes for old connections to timeout, then restart.

#### Option 2: Use Different Consumer Groups

Each consumer group can have its own set of consumers. Change the consumer group in your `.env` file:

```bash
# Before
EVENTHUBNAME_1_CONSUMER_GROUP="$Default"

# After - use a unique name per environment/instance
EVENTHUBNAME_1_CONSUMER_GROUP="dev-miguel"
# or
EVENTHUBNAME_1_CONSUMER_GROUP="prod-instance-1"
```

**Note:** Each consumer group maintains its own checkpoints, so using a different consumer group means starting from scratch (or LATEST position).

#### Option 3: Check for Zombie Processes

```bash
# Check processes using EventHub ports
lsof -i -P | grep ESTABLISHED | grep eventhub

# Force cleanup of all Python processes (careful!)
pkill -9 python
```

### 2. `KeyError: 'channels_created'` - Missing Stats Key

**Symptom:**

```log
KeyError: 'channels_created'
```

**Cause:**
The `stats` dictionary in `SnowflakeHighPerformanceStreamingClient` was missing the `"channels_created"` key initialization.

**Fix:**
This has been fixed in commit XXX. Update your code or ensure the stats dictionary includes:

```python
self.stats: dict[str, Any] = {
    "client_created_at": None,
    "total_messages_sent": 0,
    "total_batches_sent": 0,
    "channels_created": 0,  # ← This was missing
    "last_ingestion": None,
    "retry_stats": {...},
}
```

### 3. Reducing Log Verbosity

If you're seeing too many detailed logs, you can adjust the log level:

#### Option 1: Environment Variable

```bash
export LOG_LEVEL=WARNING
uv run evsnow run
```

**Option 2: Edit main.py**
Change the logging level in your code:

```python
logging.basicConfig(level=logging.WARNING)  # Instead of INFO or DEBUG
```

**Option 3: Suppress Specific Loggers**
For Azure SDK logs specifically:

```python
import logging
logging.getLogger('azure.eventhub').setLevel(logging.ERROR)
logging.getLogger('azure.core').setLevel(logging.ERROR)
```

### 4. Checkpoint Issues - Not Resuming from Last Position

**Symptom:**
Consumer starts processing from LATEST instead of resuming from last checkpoint.

**Diagnosis:**

```bash
# Check if checkpoints exist in Snowflake
uv run python -c "
from utils.snowflake import get_partition_checkpoints
from utils.config import load_config

config = load_config()
checkpoints = get_partition_checkpoints(
    eventhub_namespace='YOUR_NAMESPACE',
    eventhub='YOUR_EVENTHUB',
    target_db='YOUR_DB',
    target_schema='YOUR_SCHEMA',
    target_table='YOUR_TABLE',
    config=config.snowflake_connection
)
print('Checkpoints:', checkpoints)
"
```

**Possible Causes:**

1. Different consumer group (each has separate checkpoints)
2. Checkpoints not being saved (check for save errors in logs)
3. Control table schema mismatch
4. Connection to Snowflake failing during checkpoint save

## Getting Help

If you continue experiencing issues:

1. **Check Logs:** Look for ERROR level messages with detailed context
2. **Consumer ID:** Each consumer logs its unique ID (e.g., `evsnow_a1b2c3d4`) - use this to track competing consumers
3. **Stats:** Run `uv run evsnow status` to see current statistics
4. **Health Check:** The orchestrator provides health check data in logs

## Debug Mode

Enable debug mode for maximum verbosity:

```bash
export LOG_LEVEL=DEBUG
export AZURE_LOG_LEVEL=DEBUG
uv run evsnow run
```

This will show:

- Every message received
- Checkpoint save operations
- Batch processing details
- Azure SDK internals
