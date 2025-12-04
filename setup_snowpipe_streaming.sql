-- ============================================================================
-- SNOWPIPE STREAMING HIGH-PERFORMANCE ARCHITECTURE SETUP
-- Run this in Snowflake to create the required PIPE for streaming ingestion
-- 
-- Reference: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started
-- 
-- This script matches the following .env.example settings:
--   SNOWFLAKE_1_DATABASE=INGESTION
--   SNOWFLAKE_1_SCHEMA=PUBLIC
--   SNOWFLAKE_1_TABLE=events_table
--   SNOWFLAKE_PIPE_NAME=EVENTS_TABLE_PIPE
-- ============================================================================

USE DATABASE INGESTION;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- Create the target table for Event Hub data
-- ============================================================================
CREATE TABLE IF NOT EXISTS EVENTS_TABLE (
    EVENT_BODY VARIANT,
    PARTITION_ID VARCHAR(50),
    SEQUENCE_NUMBER NUMBER(38,0),
    ENQUEUED_TIME TIMESTAMP_NTZ,
    PROPERTIES VARIANT,
    SYSTEM_PROPERTIES VARIANT,
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- Create PIPE for HIGH-PERFORMANCE Snowpipe Streaming
-- 
-- IMPORTANT: The high-performance SDK REQUIRES a PIPE object.
-- Without this, you'll get: ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED
-- 
-- The PIPE uses DATA_SOURCE(TYPE => 'STREAMING') as the source.
-- ============================================================================
CREATE OR REPLACE PIPE EVENTS_TABLE_PIPE
AS
    COPY INTO EVENTS_TABLE (EVENT_BODY, PARTITION_ID, SEQUENCE_NUMBER, ENQUEUED_TIME, PROPERTIES, SYSTEM_PROPERTIES)
    FROM (
        SELECT 
            $1:event_body::VARIANT,
            $1:partition_id::VARCHAR,
            $1:sequence_number::NUMBER,
            $1:enqueued_time::TIMESTAMP_NTZ,
            $1:properties::VARIANT,
            $1:system_properties::VARIANT
        FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
    );

-- ============================================================================
-- Verify the PIPE was created
-- ============================================================================
SHOW PIPES LIKE 'EVENTS_TABLE_PIPE';

-- ============================================================================
-- Grant permissions to STREAM role (matches SNOWFLAKE_ROLE=STREAM in .env)
-- ============================================================================
GRANT OPERATE ON PIPE EVENTS_TABLE_PIPE TO ROLE STREAM;
GRANT MONITOR ON PIPE EVENTS_TABLE_PIPE TO ROLE STREAM;
GRANT INSERT ON TABLE EVENTS_TABLE TO ROLE STREAM;
GRANT SELECT ON TABLE EVENTS_TABLE TO ROLE STREAM;

-- ============================================================================
-- Verify grants
-- ============================================================================
SHOW GRANTS ON PIPE EVENTS_TABLE_PIPE;

SELECT 'Snowpipe Streaming HIGH-PERFORMANCE setup complete!' AS STATUS;
