-- Snowflake Setup SQL Script
-- Run this script in Snowflake Web UI (Worksheets) or SnowSQL
-- Make sure you're logged in as a user with appropriate permissions (ACCOUNTADMIN or similar)

-- ============================================
-- PART 1: Assign RSA Public Key to User
-- ============================================

-- Read the public key value from: snowflake/rsa_key_pub_value.txt
-- Replace <PUBLIC_KEY_VALUE> below with the actual value

USE ROLE ACCOUNTADMIN;  -- Or your admin role

ALTER USER STREAMEV SET RSA_PUBLIC_KEY='<PUBLIC_KEY_VALUE>';

-- Verify the key was set:
DESC USER STREAMEV;
-- Look for RSA_PUBLIC_KEY_FP (fingerprint) - should not be NULL


-- ============================================
-- PART 2: Create INGESTION Database and Schema
-- ============================================

CREATE ROLE IF NOT EXISTS STREAM;
GRANT ROLE STREAM TO USER STREAMEV;

CREATE DATABASE IF NOT EXISTS INGESTION;

USE DATABASE INGESTION;

CREATE SCHEMA IF NOT EXISTS PUBLIC;

-- Verify creation:
SHOW DATABASES LIKE 'INGESTION';
SHOW SCHEMAS IN DATABASE INGESTION;


-- ============================================
-- PART 3: Create CONTROL Database and Schema
-- ============================================

CREATE DATABASE IF NOT EXISTS CONTROL;

USE DATABASE CONTROL;

CREATE SCHEMA IF NOT EXISTS PUBLIC;

-- Verify creation:
SHOW DATABASES LIKE 'CONTROL';
SHOW SCHEMAS IN DATABASE CONTROL;


-- ============================================
-- PART 4: Create Control Table (INGESTION_STATUS)
-- ============================================

USE DATABASE CONTROL;
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE CONTROL.PUBLIC.INGESTION_STATUS (
    TS_INSERTED TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    EVENTHUB_NAMESPACE VARCHAR(500) NOT NULL,
    EVENTHUB VARCHAR(200) NOT NULL,
    TARGET_DB VARCHAR(200) NOT NULL,
    TARGET_SCHEMA VARCHAR(200) NOT NULL,
    TARGET_TABLE VARCHAR(200) NOT NULL,
    WATERLEVEL NUMBER(38,0),
    PARTITION_ID VARCHAR(50) NOT NULL,
    METADATA VARIANT,
    PRIMARY KEY (EVENTHUB_NAMESPACE, EVENTHUB, TARGET_DB, TARGET_SCHEMA, TARGET_TABLE, PARTITION_ID)
);

-- Verify table creation:
SHOW TABLES LIKE 'INGESTION_STATUS' IN SCHEMA CONTROL.PUBLIC;
DESC TABLE CONTROL.PUBLIC.INGESTION_STATUS;


-- ============================================
-- PART 5: Grant Permissions to STREAM Role
-- ============================================

-- Grant permissions on CONTROL database
GRANT USAGE ON DATABASE CONTROL TO ROLE STREAM;
GRANT USAGE ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM;
GRANT SELECT, INSERT, UPDATE ON TABLE CONTROL.PUBLIC.INGESTION_STATUS TO ROLE STREAM;

-- Grant permissions on INGESTION database
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE STREAM;
GRANT USAGE ON DATABASE INGESTION TO ROLE STREAM;
GRANT USAGE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT CREATE TABLE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA INGESTION.PUBLIC TO ROLE STREAM;

-- For high-performance Snowpipe Streaming target objects, also run:
--   setup_snowpipe_streaming.sql
-- That script creates the Iceberg table, PIPE, and grants EXTERNAL VOLUME usage.


-- ============================================
-- PART 6: Verify Setup
-- ============================================

-- Check RSA public key
DESC USER STREAMEV;

-- Check databases
SHOW DATABASES LIKE 'INGESTION';
SHOW DATABASES LIKE 'CONTROL';

-- Check control table
SELECT * FROM CONTROL.PUBLIC.INGESTION_STATUS LIMIT 1;

-- Test insert (should work)
-- This is just a test - the pipeline will manage actual inserts
/*
INSERT INTO CONTROL.PUBLIC.INGESTION_STATUS 
(EVENTHUB_NAMESPACE, EVENTHUB, TARGET_DB, TARGET_SCHEMA, TARGET_TABLE, WATERLEVEL, PARTITION_ID, METADATA)
VALUES 
('test.servicebus.windows.net', 'test-eventhub', 'INGESTION', 'PUBLIC', 'test_table', 0, '0', PARSE_JSON('{}'));

-- If the insert works, clean up the test record:
DELETE FROM CONTROL.PUBLIC.INGESTION_STATUS WHERE EVENTHUB = 'test-eventhub';
*/

SELECT 'Snowflake setup completed. Next: update .env, run uv run evsnow validate-config --show-rbac, then run setup_snowpipe_streaming.sql for high-performance streaming objects.' AS STATUS;
