/*--
 CelcomDigi Snowflake Intelligence - Teardown Script
 Hands-On Lab for CelcomDigi Malaysia
 
 This script removes all objects created by the setup script
--*/

USE ROLE accountadmin;

-- Assign Query Tag to Session
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"celcomdigi_intelligence_teardown","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

-- ============================================================================= 
-- DROP CORTEX SEARCH SERVICE
-- =============================================================================

USE DATABASE celcomdigi_intelligence_db;
USE SCHEMA analytics;

DROP CORTEX SEARCH SERVICE IF EXISTS celcomdigi_feedback_search;

-- ============================================================================= 
-- DROP VIEWS
-- =============================================================================

DROP VIEW IF EXISTS customer_feedback_unified;

-- ============================================================================= 
-- DROP TABLES
-- =============================================================================

DROP TABLE IF EXISTS customer_call_transcripts;
DROP TABLE IF EXISTS customer_complaint_documents;
DROP TABLE IF EXISTS customer_details;
DROP TABLE IF EXISTS customer_feedback_summary;
DROP TABLE IF EXISTS infrastructure_capacity;
DROP TABLE IF EXISTS network_performance;

-- ============================================================================= 
-- DROP STAGES
-- =============================================================================

DROP STAGE IF EXISTS semantic_models;
DROP STAGE IF EXISTS raw_files;
DROP STAGE IF EXISTS processed_data;

-- ============================================================================= 
-- DROP SCHEMA, DATABASE, WAREHOUSE, ROLE
-- =============================================================================

DROP SCHEMA IF EXISTS celcomdigi_intelligence_db.analytics;
DROP DATABASE IF EXISTS celcomdigi_intelligence_db;

DROP WAREHOUSE IF EXISTS celcomdigi_intelligence_wh;

-- Revoke privileges before dropping role
REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER FROM ROLE celcomdigi_intelligence_role;
REVOKE ROLE celcomdigi_intelligence_role FROM ROLE sysadmin;

DROP ROLE IF EXISTS celcomdigi_intelligence_role;

-- ============================================================================= 
-- OPTIONAL: REMOVE SNOWFLAKE INTELLIGENCE SCHEMA (only if created just for this lab)
-- =============================================================================

-- Uncomment the following lines only if you want to remove the Snowflake Intelligence schema
-- This may affect other agents if you have created them

-- DROP SCHEMA IF EXISTS snowflake_intelligence.agents;
-- DROP DATABASE IF EXISTS snowflake_intelligence;

-- ============================================================================= 
-- TEARDOWN COMPLETE
-- =============================================================================

SELECT 'CelcomDigi Snowflake Intelligence teardown complete!' AS status,
       'All objects have been removed' AS result;
