-- ============================================================================
-- SNOWPIPE STREAMING HIGH-PERFORMANCE ARCHITECTURE SETUP
-- Run this in Snowflake to create the required Iceberg table and PIPE
--
-- Reference: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started
--
-- This script matches the following .env.example settings:
--   SNOWFLAKE_1_DATABASE=INGESTION
--   SNOWFLAKE_1_SCHEMA=PUBLIC
--   SNOWFLAKE_1_TABLE=EVENTS_TABLE1
--   SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE
--
-- EvSnow opens one Snowpipe Streaming channel per Event Hub partition using:
--   {base_channel_name}-p{sanitized_partition}
-- For example: topic1-development-default-evsnow-prod-a-p0
-- All partition channels write through the same PIPE into the target table.
-- ============================================================================

USE DATABASE INGESTION;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- Create the target Iceberg table for Event Hub data
-- ============================================================================
CREATE OR REPLACE ICEBERG TABLE INGESTION.PUBLIC.EVENTS_TABLE1 (
    EVENT_BODY STRING,
    PARTITION_ID STRING,
    SEQUENCE_NUMBER DECIMAL(38, 0),
    OFFSET STRING,
    ENQUEUED_TIME TIMESTAMP_LTZ(6),
    PROPERTIES STRING,
    SYSTEM_PROPERTIES STRING,
    INGESTION_TIMESTAMP TIMESTAMP_LTZ(6) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_LTZ(6))
)
CLUSTER BY (ENQUEUED_TIME)
EXTERNAL_VOLUME = 'EXVOL'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'events/';

-- ============================================================================
-- Create PIPE for HIGH-PERFORMANCE Snowpipe Streaming
--
-- IMPORTANT: The high-performance SDK REQUIRES a PIPE object.
-- Without this, you'll get: ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED
--
-- The PIPE uses DATA_SOURCE(TYPE => 'STREAMING') as the source.
-- EvSnow splits mixed-partition Event Hub batches before ingest, so each
-- Snowpipe Streaming channel receives only one partition in sequence order.
-- ============================================================================
CREATE OR REPLACE PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE AS
COPY INTO INGESTION.PUBLIC.EVENTS_TABLE1 (
    EVENT_BODY,
    PARTITION_ID,
    SEQUENCE_NUMBER,
    OFFSET,
    ENQUEUED_TIME,
    PROPERTIES,
    SYSTEM_PROPERTIES
)
FROM (
    SELECT
        TO_VARCHAR($1:event_body) AS EVENT_BODY,
        TO_VARCHAR($1:partition_id) AS PARTITION_ID,
        $1:sequence_number::NUMBER(38,0) AS SEQUENCE_NUMBER,
        TO_VARCHAR($1:offset) AS OFFSET,
        $1:enqueued_time::TIMESTAMP_NTZ(6) AS ENQUEUED_TIME,
        TO_VARCHAR($1:properties) AS PROPERTIES,
        TO_VARCHAR($1:system_properties) AS SYSTEM_PROPERTIES
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

-- ============================================================================
-- Verify the PIPE was created
-- ============================================================================
SHOW PIPES LIKE 'EVENTS_TABLE_PIPE';

-- ============================================================================
-- Grant permissions to STREAM role (matches SNOWFLAKE_ROLE=STREAM in .env)
-- ============================================================================
GRANT OPERATE ON PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE TO ROLE STREAM;
GRANT MONITOR ON PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE TO ROLE STREAM;
GRANT INSERT ON TABLE INGESTION.PUBLIC.EVENTS_TABLE1 TO ROLE STREAM;
GRANT SELECT ON TABLE INGESTION.PUBLIC.EVENTS_TABLE1 TO ROLE STREAM;
GRANT USAGE ON EXTERNAL VOLUME EXVOL TO ROLE STREAM;

-- ============================================================================
-- Verify grants
-- ============================================================================
SHOW GRANTS ON PIPE INGESTION.PUBLIC.EVENTS_TABLE_PIPE;

SELECT 'Snowpipe Streaming HIGH-PERFORMANCE setup complete!' AS STATUS;
