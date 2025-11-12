/*--
 CelcomDigi Snowflake Intelligence - Setup Script
 Hands-On Lab for CelcomDigi Malaysia
 Reference: https://github.com/Snowflake-Labs/sfguide-getting-started-with-snowflake-intelligence
--*/

USE ROLE accountadmin;

-- Assign Query Tag to Session
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"celcomdigi_intelligence","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql","region":"malaysia"}}';

-- ============================================================================= 
-- STEP 1: CREATE ROLE AND GRANT PRIVILEGES
-- =============================================================================

CREATE OR REPLACE ROLE celcomdigi_intelligence_role;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE celcomdigi_intelligence_role;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE celcomdigi_intelligence_role;

SET current_user = (SELECT CURRENT_USER());
GRANT ROLE celcomdigi_intelligence_role TO USER IDENTIFIER($current_user);

-- ============================================================================= 
-- STEP 2: SWITCH TO CUSTOM ROLE AND CREATE OBJECTS
-- =============================================================================

-- Switch to custom role (following Snowflake Labs pattern)
USE ROLE celcomdigi_intelligence_role;

CREATE OR REPLACE WAREHOUSE celcomdigi_intelligence_wh WITH WAREHOUSE_SIZE='medium';
CREATE OR REPLACE DATABASE celcomdigi_intelligence_db;
CREATE OR REPLACE SCHEMA analytics;

-- Create snowflake_intelligence database (custom role has CREATE DATABASE privilege)
CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;

GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE celcomdigi_intelligence_role;

-- Grant Cortex functions access
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE celcomdigi_intelligence_role;

-- Grant role to sysadmin
GRANT ROLE celcomdigi_intelligence_role TO ROLE sysadmin;

USE DATABASE celcomdigi_intelligence_db;
USE SCHEMA analytics;
USE WAREHOUSE celcomdigi_intelligence_wh;

-- ============================================================================= 
-- STEP 3: CREATE STAGES
-- =============================================================================

-- Use IF NOT EXISTS to preserve uploaded files on re-runs
CREATE STAGE IF NOT EXISTS semantic_models
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

CREATE STAGE IF NOT EXISTS raw_files
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

CREATE STAGE IF NOT EXISTS processed_data
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

-- =============================================================================
-- STEP 4: CREATE TABLES
-- =============================================================================

-- Network Performance Table
CREATE OR REPLACE TABLE network_performance (
    tower_id VARCHAR(50),
    tower_name VARCHAR(100),
    region VARCHAR(50),
    network_type VARCHAR(10),
    measurement_date DATE,
    measurement_hour INT,
    avg_latency_ms FLOAT,
    avg_download_speed_mbps FLOAT,
    avg_upload_speed_mbps FLOAT,
    packet_loss_pct FLOAT,
    signal_strength_dbm FLOAT,
    active_users INT,
    data_volume_gb FLOAT,
    peak_concurrent_users INT,
    call_drop_rate_pct FLOAT,
    handover_success_rate_pct FLOAT,
    availability_pct FLOAT
);

-- Infrastructure Capacity Table
CREATE OR REPLACE TABLE infrastructure_capacity (
    tower_id VARCHAR(50),
    tower_name VARCHAR(100),
    region VARCHAR(50),
    network_type VARCHAR(10),
    capacity_date DATE,
    total_bandwidth_gbps FLOAT,
    used_bandwidth_gbps FLOAT,
    available_bandwidth_gbps FLOAT,
    utilization_pct FLOAT,
    equipment_status VARCHAR(20),
    last_maintenance_date DATE,
    next_scheduled_maintenance DATE,
    expected_growth_pct FLOAT,
    upgrade_recommended BOOLEAN,
    estimated_capacity_exhaustion_date DATE
);

-- Customer Feedback Summary Table
CREATE OR REPLACE TABLE customer_feedback_summary (
    feedback_date DATE,
    region VARCHAR(50),
    feedback_type VARCHAR(50),
    total_feedback_count INT,
    complaint_count INT,
    compliment_count INT,
    inquiry_count INT,
    avg_sentiment_score FLOAT,
    negative_sentiment_count INT,
    neutral_sentiment_count INT,
    positive_sentiment_count INT,
    network_issue_count INT,
    billing_issue_count INT,
    service_issue_count INT,
    other_issue_count INT
);

-- Customer Details Table
CREATE OR REPLACE TABLE customer_details (
    customer_id VARCHAR(50),
    customer_segment VARCHAR(50),
    region VARCHAR(50),
    signup_date DATE,
    plan_type VARCHAR(50),
    monthly_revenue FLOAT,
    tenure_months INT,
    is_churned BOOLEAN,
    churn_date DATE,
    churn_reason VARCHAR(200)
);

-- Customer Call Transcripts Table
CREATE OR REPLACE TABLE customer_call_transcripts (
    call_id VARCHAR(50),
    customer_id VARCHAR(50),
    call_date TIMESTAMP,
    call_duration_seconds INT,
    audio_file_path VARCHAR(500),
    transcript_text TEXT,
    sentiment_score VARIANT,  -- AI_SENTIMENT returns OBJECT
    key_issues ARRAY,
    summary TEXT,
    resolution_status VARCHAR(50),
    agent_name VARCHAR(100),
    call_reason VARCHAR(100),
    csat_score INT
);

-- Customer Complaint Documents Table
CREATE OR REPLACE TABLE customer_complaint_documents (
    document_id VARCHAR(50),
    customer_id VARCHAR(50),
    document_date DATE,
    document_type VARCHAR(50),
    file_path VARCHAR(500),
    extracted_text TEXT,
    complaint_category VARCHAR(100),
    sentiment_score VARIANT,  -- AI_SENTIMENT returns OBJECT
    priority_level VARCHAR(20),
    summary TEXT
);

-- Customer Interaction History Table
CREATE OR REPLACE TABLE customer_interaction_history (
    customer_id VARCHAR(50),
    total_calls INT,
    total_complaints INT,
    avg_csat_score FLOAT,
    avg_sentiment_score FLOAT,
    first_contact_date DATE,
    last_contact_date DATE,
    total_network_issues INT,
    total_billing_issues INT,
    total_service_issues INT,
    escalation_count INT,
    unresolved_issues INT,
    is_at_risk BOOLEAN
);

-- CSAT Surveys Table
CREATE OR REPLACE TABLE csat_surveys (
    survey_id VARCHAR(50),
    call_id VARCHAR(50),
    customer_id VARCHAR(50),
    survey_date DATE,
    csat_score INT,
    nps_score INT,
    would_recommend BOOLEAN,
    survey_comments TEXT
);

-- =============================================================================
-- STEP 5: CREATE FILE FORMAT
-- =============================================================================

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  NULL_IF = ('NULL', 'null', '')
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- =============================================================================
-- STEP 6: CREATE ANALYTICAL VIEWS
-- =============================================================================

-- Customer 360 View
CREATE OR REPLACE VIEW customer_360_view AS
SELECT 
    cd.customer_id,
    cd.customer_segment,
    cd.region,
    cd.plan_type,
    cd.monthly_revenue,
    cd.tenure_months,
    cd.is_churned,
    cd.churn_reason,
    cih.total_calls,
    cih.total_complaints,
    cih.avg_csat_score,
    cih.avg_sentiment_score,
    cih.first_contact_date,
    cih.last_contact_date,
    cih.total_network_issues,
    cih.total_billing_issues,
    cih.total_service_issues,
    cih.escalation_count,
    cih.unresolved_issues,
    cih.is_at_risk,
    CASE 
        WHEN cih.avg_csat_score IS NULL THEN cd.tenure_months * cd.monthly_revenue
        ELSE cd.tenure_months * cd.monthly_revenue * (cih.avg_csat_score / 5.0)
    END AS customer_lifetime_value
FROM customer_details cd
LEFT JOIN customer_interaction_history cih ON cd.customer_id = cih.customer_id;

-- Call Analysis View
CREATE OR REPLACE VIEW call_analysis_view AS
SELECT 
    ct.call_id,
    ct.customer_id,
    ct.call_date,
    ct.call_duration_seconds,
    ct.sentiment_score,  -- Keep as VARIANT
    ct.sentiment_score:categories[0]:sentiment::VARCHAR AS call_sentiment,  -- Extract label
    ct.resolution_status,
    ct.agent_name,
    ct.call_reason,
    cs.csat_score,
    cs.nps_score,
    cs.would_recommend,
    cd.customer_segment,
    cd.region,
    cd.plan_type,
    cd.monthly_revenue,
    cd.tenure_months,
    cd.is_churned,
    cih.is_at_risk,
    cih.unresolved_issues,
    CASE WHEN ct.sentiment_score:categories[0]:sentiment::VARCHAR = 'negative' THEN TRUE ELSE FALSE END AS is_negative_call,
    CASE WHEN cs.csat_score <= 2 THEN TRUE ELSE FALSE END AS is_detractor
FROM customer_call_transcripts ct
LEFT JOIN csat_surveys cs ON ct.call_id = cs.call_id
LEFT JOIN customer_details cd ON ct.customer_id = cd.customer_id
LEFT JOIN customer_interaction_history cih ON ct.customer_id = cih.customer_id;

-- Network Customer Impact View
CREATE OR REPLACE VIEW network_customer_impact AS
SELECT 
    np.tower_id,
    np.tower_name,
    np.region,
    np.network_type,
    np.measurement_date,
    AVG(np.avg_latency_ms) AS avg_latency,
    AVG(np.avg_download_speed_mbps) AS avg_download_speed,
    AVG(np.packet_loss_pct) AS avg_packet_loss,
    AVG(np.call_drop_rate_pct) AS avg_call_drop_rate,
    COUNT(DISTINCT cav.customer_id) AS affected_customers,
    COUNT(DISTINCT cav.call_id) AS related_calls,
    COUNT(CASE WHEN cav.call_sentiment = 'negative' THEN 1 END) AS negative_sentiment_calls,
    SUM(CASE WHEN cav.is_at_risk THEN 1 ELSE 0 END) AS at_risk_customers,
    SUM(cd.monthly_revenue) AS total_monthly_revenue_at_risk
FROM network_performance np
LEFT JOIN call_analysis_view cav 
    ON np.region = cav.region 
    AND DATE(np.measurement_date) = DATE(cav.call_date)
    AND cav.call_reason LIKE '%Network%'
LEFT JOIN customer_details cd ON cav.customer_id = cd.customer_id
WHERE np.measurement_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY 
    np.tower_id, np.tower_name, np.region, 
    np.network_type, np.measurement_date;

-- Churn Risk Analysis View
CREATE OR REPLACE VIEW churn_risk_analysis AS
SELECT 
    cd.customer_id,
    cd.customer_segment,
    cd.region,
    cd.plan_type,
    cd.monthly_revenue,
    cd.tenure_months,
    cih.total_complaints,
    cih.avg_csat_score,
    cih.avg_sentiment_score,
    cih.unresolved_issues,
    cih.escalation_count,
    cih.total_network_issues,
    LEAST(100, 
        (cih.total_complaints * 10) + 
        ((5 - COALESCE(cih.avg_csat_score, 3)) * 15) +
        (CASE WHEN cih.avg_sentiment_score < 0 THEN ABS(cih.avg_sentiment_score) * 30 ELSE 0 END) +
        (cih.unresolved_issues * 20) +
        (cih.escalation_count * 15)
    ) AS churn_risk_score,
    CASE 
        WHEN cih.is_at_risk THEN 'High Risk'
        WHEN cih.total_complaints > 2 OR cih.avg_csat_score < 3 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_category,
    cd.monthly_revenue * 12 AS annual_revenue_at_risk
FROM customer_details cd
LEFT JOIN customer_interaction_history cih ON cd.customer_id = cih.customer_id
WHERE cd.is_churned = FALSE;

-- =============================================================================
-- STEP 7: CREATE CORTEX SEARCH SERVICE
-- =============================================================================

-- Unified view for Cortex Search
CREATE OR REPLACE VIEW customer_feedback_unified AS
SELECT 
    call_id as feedback_id,
    customer_id,
    call_date::TIMESTAMP as feedback_date,
    'Call' as feedback_type,
    transcript_text as content,
    sentiment_score:categories[0]:sentiment::VARCHAR as sentiment,  -- Extract as VARCHAR
    summary,
    call_reason as category,
    resolution_status,
    'Support Call' as source
FROM customer_call_transcripts

UNION ALL

SELECT 
    document_id as feedback_id,
    customer_id,
    document_date::TIMESTAMP as feedback_date,
    'Document' as feedback_type,
    extracted_text as content,
    'reference' as sentiment,  -- PDFs are reference docs, no sentiment
    'Reference Document' as summary,
    complaint_category as category,
    NULL as resolution_status,
    'PDF Document' as source
FROM customer_complaint_documents;

-- Create Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE celcomdigi_feedback_search
ON content
ATTRIBUTES feedback_type, customer_id, sentiment, category, resolution_status, source
WAREHOUSE = celcomdigi_intelligence_wh
TARGET_LAG = '1 minute'
AS (
    SELECT 
        feedback_id,
        content,
        feedback_type,
        customer_id,
        sentiment,  -- VARCHAR extracted from VARIANT
        category,
        resolution_status,
        source
    FROM customer_feedback_unified
);

-- =============================================================================
-- STEP 8: ENABLE CORTEX CROSS-REGION
-- =============================================================================

USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';

-- =============================================================================
-- SETUP COMPLETE
-- =============================================================================

SELECT 'CelcomDigi Snowflake Intelligence setup complete!' AS status,
       'Database: CELCOMDIGI_INTELLIGENCE_DB' AS database_name,
       'Schema: ANALYTICS' AS schema_name,
       'Warehouse: CELCOMDIGI_INTELLIGENCE_WH (Medium)' AS warehouse_name,
       'Agent Schema: SNOWFLAKE_INTELLIGENCE.AGENTS' AS agent_schema,
       'Next: Upload files and run data_processing.ipynb' AS next_step;
