-- Snowflake Setup SQL Script
-- Run this script in Snowflake Web UI (Worksheets) or the Snowflake CLI
-- Make sure you're logged in as a user with appropriate permissions (ACCOUNTADMIN or similar)

-- ============================================
-- PART 1: Create Runtime Role, Warehouse, And User
-- ============================================

-- Read the public key value from: snowflake/rsa_key_pub_value.txt
-- Replace <PUBLIC_KEY_VALUE> below with the actual value

USE ROLE ACCOUNTADMIN;  -- Or your admin role

CREATE ROLE IF NOT EXISTS STREAM;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE USER IF NOT EXISTS STREAMEV
    TYPE = SERVICE
    DEFAULT_ROLE = STREAM
    DEFAULT_WAREHOUSE = COMPUTE_WH
    RSA_PUBLIC_KEY = '<PUBLIC_KEY_VALUE>';

GRANT ROLE STREAM TO USER STREAMEV;
ALTER USER STREAMEV SET RSA_PUBLIC_KEY='<PUBLIC_KEY_VALUE>';
ALTER USER STREAMEV SET TYPE = SERVICE;

-- Verify the key was set:
DESC USER STREAMEV;
-- Look for RSA_PUBLIC_KEY_FP (fingerprint) - should not be NULL


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
GRANT CREATE SCHEMA ON DATABASE CONTROL TO ROLE STREAM;
GRANT USAGE ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM;
GRANT CREATE TABLE ON SCHEMA CONTROL.PUBLIC TO ROLE STREAM;
GRANT SELECT, INSERT, UPDATE ON TABLE CONTROL.PUBLIC.INGESTION_STATUS TO ROLE STREAM;

-- Grant permissions on INGESTION database
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE STREAM;
GRANT USAGE ON DATABASE INGESTION TO ROLE STREAM;
GRANT USAGE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT CREATE TABLE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT CREATE ICEBERG TABLE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT CREATE PIPE ON SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA INGESTION.PUBLIC TO ROLE STREAM;
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA INGESTION.PUBLIC TO ROLE STREAM;

-- For high-performance Snowpipe Streaming target objects, also run:
--   setup_snowpipe_streaming.sql
-- That script creates the Snowflake-managed internal Iceberg table and PIPE.


-- ============================================
-- PART 6: Verify Setup
-- ============================================

-- Check RSA public key
DESC USER STREAMEV;

-- Check runtime role grants
SHOW GRANTS TO ROLE STREAM;

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

SELECT 'Snowflake setup completed. Next: run setup_snowpipe_streaming.sql, update .env, then run uv run evsnow validate-config --show-rbac.' AS STATUS;
