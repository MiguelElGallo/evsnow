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

CREATE HYBRID TABLE IF NOT EXISTS INGESTION_STATUS (
    TS_INSERTED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    EVENTHUB_NAMESPACE VARCHAR(500),
    EVENTHUB VARCHAR(200),
    TARGET_DB VARCHAR(200),
    TARGET_SCHEMA VARCHAR(200),
    TARGET_TABLE VARCHAR(200),
    WATERLEVEL NUMBER(38, 0),
    PARTITION_ID VARCHAR(50) NOT NULL,
    METADATA VARIANT,
    PRIMARY KEY (EVENTHUB_NAMESPACE, EVENTHUB, TARGET_DB, TARGET_SCHEMA, TARGET_TABLE, PARTITION_ID)
);

-- Verify table creation:
SHOW TABLES LIKE 'INGESTION_STATUS' IN SCHEMA CONTROL.PUBLIC;
DESC TABLE CONTROL.PUBLIC.INGESTION_STATUS;


-- ============================================
-- PART 5: Grant Permissions to STREAMEV User
-- ============================================

-- Grant permissions on CONTROL database
GRANT USAGE ON DATABASE CONTROL TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA CONTROL.PUBLIC TO ROLE PUBLIC;
GRANT SELECT, INSERT, UPDATE ON TABLE CONTROL.PUBLIC.INGESTION_STATUS TO ROLE PUBLIC;

-- Grant permissions on INGESTION database
GRANT USAGE ON DATABASE INGESTION TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA INGESTION.PUBLIC TO ROLE PUBLIC;
GRANT CREATE TABLE ON SCHEMA INGESTION.PUBLIC TO ROLE PUBLIC;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA INGESTION.PUBLIC TO ROLE PUBLIC;
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA INGESTION.PUBLIC TO ROLE PUBLIC;

-- If STREAMEV has a specific role, grant to that role instead
-- Example:
-- GRANT USAGE ON DATABASE CONTROL TO ROLE DATA_ENGINEER;
-- GRANT USAGE ON SCHEMA CONTROL.PUBLIC TO ROLE DATA_ENGINEER;
-- etc.


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

PRINT;
PRINT '✅ Snowflake setup completed!';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Copy the public key from: snowflake/rsa_key_pub_value.txt';
PRINT '2. Replace <PUBLIC_KEY_VALUE> in Part 1 above';
PRINT '3. Update your .env file with Snowflake configuration';
PRINT '4. Run: ./verify_snowflake_setup.sh';
PRINT '5. Run: evsnow validate-config';
