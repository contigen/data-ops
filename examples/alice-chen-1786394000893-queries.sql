-- =================================================================================
-- DATA ASSET HANDOVER REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Target Successor: Data Engineering Team / Successor
-- Date: October 2023
--
-- This script serves as an operational runbook, schema documentation, and 
-- query reference for the data assets previously maintained by alice.chen.
-- =================================================================================


-- =================================================================================
-- SECTION 1: ASSET CATALOG & METADATA OVERVIEW
-- =================================================================================
/*
1. Snowflake Dataset: PROD.orders_v3_FINAL
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)
   - SLA: Strict daily update by 6:00 AM UTC.
   - Quality: dbt verified (not_null and accepted_values on 'order_status').
   - CRITICAL EDGE CASE: Canceled orders with $0 subtotal and positive shipping cost 
     will break the downstream 'daily_revenue_etl' pipeline (causing negative margin)
     if 'is_refunded' is not set to TRUE within 24 hours.

2. Looker Dashboard: revenue_dashboard_BACKUP
   - URN: urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD)
   - Status: Backup dashboard. Dependent on daily_revenue_etl output.

3. Kafka Topic/Dataset: kafka_events_raw_copy
   - URN: urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD)
   - Status: Raw event stream copy. Used for real-time ingestion auditing.

4. Snowflake Dataset: PROD.orders_v2 (Deprecated)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD)
   - Status: Legacy orders table. Do not use for new development.

5. Snowflake Dataset: PROD.orders_v1 (Deprecated)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD)
   - Status: Legacy orders table. Do not use.

6. Snowflake Dataset: PROD.quarterly_board_metrics
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD)
   - Status: High-level executive reporting table populated from orders_v3_FINAL.

7. Airflow DAG / Dataset: daily_revenue_etl
   - URN: urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD)
   - Status: Daily revenue aggregation pipeline. Highly sensitive to the refund edge case.

8. Snowflake Dataset: PROD.churn_prediction_features
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD)
   - Status: Feature store table for ML models, built on top of dim_customers and orders.

9. Snowflake Dataset: PROD.dim_customers
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD)
   - Status: Customer dimension table containing demographic and account status.
*/


-- =================================================================================
-- SECTION 2: OPERATIONAL RECOVERY PLAYBOOK (SQL & CLI REFERENCE)
-- =================================================================================

/*
RECOVERY PROCEDURE FOR THE "ZERO-DOLLAR SUBTOTAL" EDGE CASE:
If the Stripe webhook fails to sync the 'is_refunded' flag within 24 hours for canceled 
orders with positive shipping costs, the downstream 'daily_revenue_etl' will fail or 
calculate negative margins.

Follow these steps:

STEP 1: Identify the problematic Order IDs using Query 2.1 below.
STEP 2: Run the reconciliation script from the 'data-ops' repository:
        
        # For a single or small batch of order IDs:
        python stripe_reconciliation_catchup.py --order-ids "12345,67890"
        
        # For larger batches (exceeding 500 records), use the batch-size flag to prevent rate-limiting:
        python stripe_reconciliation_catchup.py --file-path "/path/to/failed_orders.csv" --batch-size 100

STEP 3: Manually trigger an Airflow backfill on the 'daily_revenue_etl' DAG for the affected dates:
        airflow dags backfill -s YYYY-MM-DD -e YYYY-MM-DD daily_revenue_etl
*/


-- =================================================================================
-- SECTION 3: MONITORING & DATA QUALITY QUERIES
-- =================================================================================

-- Query 3.1: SLA Verification Query
-- Run this query to verify if the orders_v3_FINAL table has been updated by 6:00 AM UTC daily.
SELECT 
    MAX(updated_at) AS last_pipeline_run,
    CURRENT_TIMESTAMP() AS current_system_time,
    CASE 
        WHEN MAX(updated_at) >= DATE_TRUNC('day', CURRENT_TIMESTAMP()) + INTERVAL '6 hours' THEN 'SLA Met'
        ELSE 'SLA Violated / Pending Run'
    END AS sla_status
FROM PROD.orders_v3_FINAL;


-- Query 3.2: Edge Case Detection Query (Run Daily to Pre-empt ETL Failures)
-- Identifies canceled orders with $0 subtotal and positive shipping costs where 'is_refunded' is NOT set to TRUE.
-- If this query returns rows older than 24 hours, execute the Recovery Playbook (Section 2).
SELECT 
    order_id,
    customer_id,
    order_status,
    subtotal_amount,
    shipping_amount,
    is_refunded,
    created_at,
    updated_at,
    DATEDIFF('hour', created_at, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM PROD.orders_v3_FINAL
WHERE 
    order_status = 'Canceled'
    AND subtotal_amount = 0.00
    AND shipping_amount > 0.00
    AND is_refunded = FALSE
ORDER BY created_at DESC;


-- Query 3.3: dbt Data Quality Assertion Simulation
-- Simulates the dbt tests run on orders_v3_FINAL to ensure data integrity.
SELECT 
    -- Test 1: Check for nulls in critical columns
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) AS null_order_statuses,
    
    -- Test 2: Check for accepted values in order_status
    SUM(CASE WHEN order_status NOT IN ('Pending', 'Completed', 'Canceled', 'Refunded') THEN 1 ELSE 0 END) AS invalid_status_count
FROM PROD.orders_v3_FINAL;


-- =================================================================================
-- SECTION 4: INTEGRATION & DOWNSTREAM PIPELINE QUERIES
-- =================================================================================

-- Query 4.1: Daily Revenue ETL Simulation (Downstream Pipeline Logic)
-- This query demonstrates how daily revenue and margin are calculated.
-- Note how the CASE statement handles the edge case to prevent negative margins.
CREATE OR REPLACE TABLE PROD.daily_revenue_etl_SIMULATION AS
SELECT 
    DATE(created_at) AS revenue_date,
    COUNT(DISTINCT order_id) AS total_orders,
    
    -- Standard Revenue Calculation
    SUM(CASE 
        WHEN order_status = 'Canceled' AND is_refunded = TRUE THEN 0
        ELSE subtotal_amount 
    END) AS net_subtotal_revenue,
    
    -- Shipping Revenue
    SUM(CASE 
        WHEN order_status = 'Canceled' AND is_refunded = TRUE THEN 0
        ELSE shipping_amount 
    END) AS net_shipping_revenue,
    
    -- Margin Calculation (Vulnerable to the edge case if is_refunded is FALSE for $0 subtotal / positive shipping)
    SUM(
        CASE 
            WHEN order_status = 'Canceled' AND is_refunded = FALSE AND subtotal_amount = 0.00 THEN 0 -- Safe handling
            WHEN order_status = 'Canceled' AND is_refunded = TRUE THEN 0
            ELSE (subtotal_amount + shipping_amount) - COALESCE(estimated_cost_amount, 0)
        END
    ) AS calculated_margin
FROM PROD.orders_v3_FINAL
GROUP BY 1
ORDER BY 1 DESC;


-- Query 4.2: Quarterly Board Metrics Generation
-- Aggregates orders_v3_FINAL data to populate the quarterly executive reporting table.
INSERT OVERWRITE INTO PROD.quarterly_board_metrics
SELECT 
    DATE_TRUNC('quarter', o.created_at) AS reporting_quarter,
    c.customer_segment,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders_completed,
    SUM(o.subtotal_amount) AS gross_merchandise_value,
    AVG(o.subtotal_amount) AS average_order_value
FROM PROD.orders_v3_FINAL o
JOIN PROD.dim_customers c 
    ON o.customer_id = c.customer_id
WHERE 
    o.order_status = 'Completed'
GROUP BY 1, 2;


-- Query 4.3: Churn Prediction Feature Store Pipeline
-- Generates customer behavioral features for the ML model stored in churn_prediction_features.
CREATE OR REPLACE TABLE PROD.churn_prediction_features AS
WITH customer_orders AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS total_orders_lifetime,
        SUM(subtotal_amount) AS total_spend_lifetime,
        MAX(created_at) AS last_order_timestamp,
        MIN(created_at) AS first_order_timestamp,
        SUM(CASE WHEN order_status = 'Canceled' THEN 1 ELSE 0 END) AS total_canceled_orders
    FROM PROD.orders_v3_FINAL
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.signup_date,
    c.customer_status,
    COALESCE(co.total_orders_lifetime, 0) AS total_orders_lifetime,
    COALESCE(co.total_spend_lifetime, 0.00) AS total_spend_lifetime,
    COALESCE(co.total_canceled_orders, 0) AS total_canceled_orders,
    DATEDIFF('day', co.last_order_timestamp, CURRENT_TIMESTAMP()) AS days_since_last_purchase,
    DATEDIFF('day', co.first_order_timestamp, co.last_order_timestamp) AS customer_lifetime_days
FROM PROD.dim_customers c
LEFT JOIN customer_orders co 
    ON c.customer_id = co.customer_id;


-- Query 4.4: Kafka Event Ingestion Audit
-- Reconciles raw Kafka event copies with the final Snowflake orders table to detect ingestion lag.
SELECT 
    DATE(k.event_timestamp) AS event_date,
    COUNT(DISTINCT k.payload:order_id::STRING) AS raw_kafka_event_count,
    COUNT(DISTINCT o.order_id) AS ingested_snowflake_order_count,
    (COUNT(DISTINCT k.payload:order_id::STRING) - COUNT(DISTINCT o.order_id)) AS missing_orders_count
FROM PROD.kafka_events_raw_copy k
LEFT JOIN PROD.orders_v3_FINAL o 
    ON k.payload:order_id::STRING = o.order_id
WHERE 
    k.event_type = 'ORDER_CREATED'
    AND k.event_timestamp >= CURRENT_DATE() - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1 DESC;


-- =================================================================================
-- SECTION 5: DEPRECATION AUDIT (CLEANUP REFERENCE)
-- =================================================================================

-- Query 5.1: Legacy Table Usage Check
-- Run this query to ensure no active queries or users are still hitting orders_v1 or orders_v2.
-- (Note: This uses Snowflake's Information Schema / Query History; adjust permissions as needed)
SELECT 
    query_text,
    user_name,
    role_name,
    execution_status,
    start_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(USER_NAME => CURRENT_USER(), RESULT_LIMIT => 100))
WHERE 
    (UPPER(query_text) LIKE '%ORDERS_V1%' OR UPPER(query_text) LIKE '%ORDERS_V2%')
    AND UPPER(query_text) NOT LIKE '%INFORMATION_SCHEMA%'
ORDER BY start_time DESC;