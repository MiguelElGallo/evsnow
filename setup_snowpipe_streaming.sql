-- Setup script for Snowpipe Streaming HIGH-PERFORMANCE ARCHITECTURE
-- Run this in Snowflake to create the required pipe for streaming ingestion
-- Reference: https://docs.snowflake.com/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started

USE DATABASE INGESTION;
USE SCHEMA PUBLIC;
USE WAREHOUSE compute_wh;

-- Create the events table if it doesn't exist
CREATE TABLE IF NOT EXISTS events_table (
    event_body VARIANT,
    partition_id VARCHAR(50),
    sequence_number NUMBER(38,0),
    enqueued_time TIMESTAMP_NTZ,
    properties VARIANT,
    system_properties VARIANT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create a PIPE for HIGH-PERFORMANCE Snowpipe Streaming
-- IMPORTANT: For high-performance architecture, use DATA_SOURCE with TYPE => 'STREAMING'
-- Reference SQL from documentation:
-- CREATE OR REPLACE PIPE MY_PIPE
-- AS COPY INTO MY_TABLE FROM (SELECT $1, $1:c1, $1:ts FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')));
CREATE OR REPLACE PIPE events_table_pipe
AS
    COPY INTO events_table (event_body, partition_id, sequence_number, enqueued_time, properties, system_properties)
    FROM (
        SELECT 
            $1:event_body,
            $1:partition_id,
            $1:sequence_number,
            $1:enqueued_time,
            $1:properties,
            $1:system_properties
        FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
    );

-- Verify the pipe was created
SHOW PIPES LIKE 'events_table_pipe';

-- Grant necessary permissions to the STREAM role
GRANT OPERATE ON PIPE events_table_pipe TO ROLE STREAM;
GRANT MONITOR ON PIPE events_table_pipe TO ROLE STREAM;
GRANT INSERT ON TABLE events_table TO ROLE STREAM;
GRANT SELECT ON TABLE events_table TO ROLE STREAM;

SELECT 'Snowpipe Streaming HIGH-PERFORMANCE setup complete!' AS status;
