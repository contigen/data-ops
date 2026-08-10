-- =================================================================================
-- DATA ASSETS HANDOVER REFERENCE SCRIPT
-- Departing Engineer: alice.chen
--
-- This script serves as an operational runbook, lineage map, and query reference
-- for the data assets previously maintained by alice.chen.
--
-- ASSETS COVERED:
-- 1. Snowflake Dataset: orders_v3_FINAL
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)
-- 2. Looker Dashboard: revenue_dashboard_BACKUP
--    URN: urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD)
-- 3. Kafka Topic: kafka_events_raw_copy
--    URN: urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD)
-- 4. Snowflake Dataset: orders_v2 (Legacy)
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD)
-- 5. Snowflake Dataset: quarterly_board_metrics
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD)
-- 6. Airflow DAG: daily_revenue_etl
--    URN: urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD)
-- 7. Snowflake Dataset: churn_prediction_features
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD)
-- 8. Snowflake Dataset: dim_customers
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD)
-- 9. Snowflake Dataset: orders_v1 (Legacy)
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD)
-- =================================================================================


-- =================================================================================
-- SECTION 1: OPERATIONAL RUNBOOK & SLA MONITORING (orders_v3_FINAL)
-- =================================================================================
-- SLA: Daily update completed by 6:00 AM UTC.
-- Quality: Passed dbt assertions (not_null and accepted_values on order_status).
--
-- CRITICAL EDGE CASE:
-- Canceled orders with a $0 subtotal and a positive shipping cost will break the 
-- downstream `daily_revenue_etl` pipeline (causing negative margin calculations) 
-- if the `is_refunded` flag is not set to `true` within a 24-hour window.
--
-- RECOVERY PROCEDURE (If Stripe Webhook fails):
-- 1. Run reconciliation script:
--    python stripe_reconciliation_catchup.py --order-ids <comma_separated_ids>
--    (For >500 records, append `--batch-size 100` to avoid rate limits)
-- 2. Manually trigger Airflow backfill on `daily_revenue_etl` DAG.
-- =================================================================================

-- Query 1.1: SLA Verification Query
-- Run this to verify if the daily load completed before the 6:00 AM UTC SLA.
SELECT 
    MAX(created_at) AS last_order_timestamp,
    MAX(updated_at) AS last_pipeline_run,
    CURRENT_TIMESTAMP() AS current_check_time,
    CASE 
        -- Check if the table has been updated within the last 24 hours and before 06:00 UTC
        WHEN MAX(updated_at) >= DATE_TRUNC('day', CURRENT_TIMESTAMP()) + INTERVAL '6 hours' THEN 'SLA MET'
        ELSE 'SLA BREACHED / PENDING UPDATE'
    END AS sla_status
FROM PROD.orders_v3_FINAL;


-- Query 1.2: Edge Case Detection Query (Run daily to preempt ETL failures)
-- Identifies canceled orders with $0 subtotal and positive shipping cost where is_refunded is NOT true.
-- If this query returns rows, the downstream daily_revenue_etl is at risk of failing.
SELECT 
    order_id,
    customer_id,
    order_status,
    subtotal,
    shipping_cost,
    is_refunded,
    created_at,
    updated_at
FROM PROD.orders_v3_FINAL
WHERE 
    order_status = 'CANCELLED'
    AND subtotal = 0.00
    AND shipping_cost > 0.00
    AND (is_refunded IS NULL OR is_refunded = FALSE)
    AND created_at >= CURRENT_DATE() - INTERVAL '2 days';


-- =================================================================================
-- SECTION 2: DOWNSTREAM PIPELINE SIMULATION (daily_revenue_etl)
-- =================================================================================
-- This query simulates the core aggregation logic of the daily_revenue_etl DAG.
-- It demonstrates how the edge case handled in Section 1 impacts margin calculations.
-- If is_refunded is true, we override the shipping cost to 0 to prevent negative margins.
-- =================================================================================

CREATE OR REPLACE TEMPORARY TABLE temp_daily_revenue_summary AS
SELECT 
    CAST(created_at AS DATE) AS order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    
    -- Standard Revenue Calculation
    SUM(subtotal) AS gross_subtotal,
    
    -- Shipping Revenue (Adjusted for refunds to prevent negative margins)
    SUM(
        CASE 
            WHEN order_status = 'CANCELLED' AND subtotal = 0 AND is_refunded = TRUE THEN 0
            ELSE shipping_cost 
        END
    ) AS net_shipping_revenue,
    
    -- Total Revenue
    SUM(
        subtotal + 
        CASE 
            WHEN order_status = 'CANCELLED' AND subtotal = 0 AND is_refunded = TRUE THEN 0
            ELSE shipping_cost 
        END
    ) AS total_revenue,
    
    -- Margin Calculation (Simulated)
    -- If is_refunded is false on a $0 subtotal + positive shipping order, margin goes negative.
    SUM(
        (subtotal * 0.6) -- Assuming 60% product margin
        - CASE 
            WHEN order_status = 'CANCELLED' AND subtotal = 0 AND (is_refunded IS NULL OR is_refunded = FALSE) 
            THEN shipping_cost -- Shipping cost absorbed as loss
            ELSE 0 
          END
    ) AS estimated_margin
FROM PROD.orders_v3_FINAL
WHERE created_at >= CURRENT_DATE() - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1 DESC;

SELECT * FROM temp_daily_revenue_summary;


-- =================================================================================
-- SECTION 3: LEGACY VERSION RECONCILIATION (orders_v1 -> orders_v2 -> orders_v3_FINAL)
-- =================================================================================
-- Use this query to audit differences between legacy tables and the current production table.
-- Useful if historical backfills are required or if Looker dashboards point to old versions.
-- =================================================================================

SELECT 
    'orders_v1' AS source_version,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date,
    SUM(subtotal) AS total_subtotal
FROM PROD.orders_v1

UNION ALL

SELECT 
    'orders_v2' AS source_version,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date,
    SUM(subtotal) AS total_subtotal
FROM PROD.orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL' AS source_version,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date,
    SUM(subtotal) AS total_subtotal
FROM PROD.orders_v3_FINAL;


-- =================================================================================
-- SECTION 4: CUSTOMER ANALYTICS & FEATURE ENGINEERING (churn_prediction_features)
-- =================================================================================
-- This query demonstrates how dim_customers and orders_v3_FINAL are joined to 
-- generate features for the churn_prediction_features dataset.
-- =================================================================================

CREATE OR REPLACE TABLE PROD.churn_prediction_features AS
WITH customer_order_stats AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders_placed,
        SUM(subtotal) AS lifetime_spend,
        MAX(created_at) AS last_order_date,
        MIN(created_at) AS first_order_date,
        COUNT(DISTINCT CASE WHEN order_status = 'CANCELLED' THEN order_id END) AS cancelled_orders_count,
        -- Calculate average days between orders
        DATEDIFF('day', MIN(created_at), MAX(created_at)) / NULLIF(COUNT(DISTINCT order_id) - 1, 0) AS avg_days_between_orders
    FROM PROD.orders_v3_FINAL
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.customer_segment,
    c.country,
    c.signup_date,
    COALESCE(s.total_orders_placed, 0) AS total_orders_placed,
    COALESCE(s.lifetime_spend, 0.00) AS lifetime_spend,
    s.last_order_date,
    DATEDIFF('day', s.last_order_date, CURRENT_DATE()) AS days_since_last_order,
    COALESCE(s.cancelled_orders_count, 0) AS cancelled_orders_count,
    COALESCE(s.avg_days_between_orders, -1) AS avg_days_between_orders,
    -- Churn Indicator (e.g., no order in 90 days)
    CASE 
        WHEN s.last_order_date IS NULL THEN TRUE
        WHEN DATEDIFF('day', s.last_order_date, CURRENT_DATE()) > 90 THEN TRUE
        ELSE FALSE
    END AS is_churned_candidate
FROM PROD.dim_customers c
LEFT JOIN customer_order_stats s ON c.customer_id = s.customer_id;

-- Verify feature generation
SELECT * FROM PROD.churn_prediction_features LIMIT 100;


-- =================================================================================
-- SECTION 5: BOARD REPORTING (quarterly_board_metrics)
-- =================================================================================
-- Aggregates orders_v3_FINAL data into quarterly financial metrics for board review.
-- =================================================================================

CREATE OR REPLACE TABLE PROD.quarterly_board_metrics AS
SELECT 
    DATE_TRUNC('quarter', created_at) AS fiscal_quarter,
    COUNT(DISTINCT customer_id) AS active_customers,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(subtotal) AS total_product_revenue,
    SUM(shipping_cost) AS total_shipping_revenue,
    -- Average Order Value (AOV)
    SUM(subtotal) / COUNT(DISTINCT order_id) AS average_order_value,
    -- Refund Rate
    COUNT(DISTINCT CASE WHEN is_refunded = TRUE THEN order_id END) :: FLOAT / COUNT(DISTINCT order_id) AS refund_rate
FROM PROD.orders_v3_FINAL
WHERE order_status != 'FAILED'
GROUP BY 1
ORDER BY 1 DESC;

-- Verify board metrics
SELECT * FROM PROD.quarterly_board_metrics;


-- =================================================================================
-- SECTION 6: KAFKA INGESTION AUDIT (kafka_events_raw_copy)
-- =================================================================================
-- kafka_events_raw_copy acts as a staging/raw copy of the event stream.
-- Use this query to audit raw event volumes against processed orders in orders_v3_FINAL.
-- =================================================================================

SELECT 
    DATE_TRUNC('hour', k.event_timestamp) AS event_hour,
    COUNT(DISTINCT k.event_id) AS raw_kafka_events,
    COUNT(DISTINCT o.order_id) AS processed_orders
FROM PROD.kafka_events_raw_copy k
LEFT JOIN PROD.orders_v3_FINAL o 
    ON k.order_id = o.order_id 
    AND k.event_timestamp::DATE = o.created_at::DATE
WHERE k.event_timestamp >= CURRENT_DATE() - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1 DESC;