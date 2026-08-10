-- ==============================================================================
-- DATA ASSET HANDOVER REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Target Successor: Data Engineering Team
-- Date: October 2023
--
-- This script serves as an operational runbook and technical reference for the
-- data assets previously maintained by alice.chen. It contains schema layouts,
-- SLA monitoring queries, edge-case detection scripts, and downstream pipeline
-- simulation queries for Snowflake, Airflow, and Looker assets.
-- ==============================================================================


-- ==============================================================================
-- SECTION 1: OPERATIONAL OVERVIEW & SLA MONITORING
-- Asset: orders_v3_FINAL (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD))
--
-- SLA: Must be updated daily by 6:00 AM UTC.
-- Quality Assertions: dbt tests enforce `not_null` and `accepted_values` on `order_status`.
-- ==============================================================================

-- SLA Verification Query
-- Run this query to verify if the daily load completed before the 6:00 AM UTC SLA.
SELECT 
    MAX(updated_at) AS last_ingested_at,
    CURRENT_TIMESTAMP() AS current_system_time,
    CASE 
        WHEN MAX(updated_at) >= DATE_TRUNC('day', CURRENT_TIMESTAMP()) + INTERVAL '6 hours' THEN 'SLA MET'
        ELSE 'SLA VIOLATION / DELAYED'
    END AS sla_status
FROM orders_v3_FINAL;

-- dbt Data Quality Assertion Check (Manual Replication)
-- Validates that order_status contains no nulls and only accepted values.
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) AS null_status_count,
    SUM(CASE WHEN order_status NOT IN ('placed', 'processed', 'shipped', 'delivered', 'cancelled', 'refunded') THEN 1 ELSE 0 END) AS invalid_status_count
FROM orders_v3_FINAL;


-- ==============================================================================
-- SECTION 2: CRITICAL EDGE CASE DETECTION & RECOVERY
-- Asset: orders_v3_FINAL -> daily_revenue_etl
--
-- Edge Case: Canceled orders with a $0 subtotal and a positive shipping cost 
-- will break the downstream `daily_revenue_etl` pipeline (causing negative margin 
-- calculations) if the `is_refunded` flag is not set to `true` within 24 hours.
--
-- Recovery Steps:
-- 1. Run `stripe_reconciliation_catchup.py` from the `data-ops` repository.
--    - Single/Bulk: Pass target IDs via `--order-ids` or `--file-path`.
--    - Rate Limiting: For batches > 500, append `--batch-size 100`.
-- 2. Manually trigger Airflow backfill on `daily_revenue_etl` DAG for affected dates.
-- ==============================================================================

-- Edge Case Audit Query
-- Run this query to identify records that will break the downstream ETL.
-- If this query returns rows, the Stripe webhook failed and manual recovery is required.
SELECT 
    order_id,
    customer_id,
    order_date,
    subtotal,
    shipping_cost,
    is_refunded,
    order_status,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM orders_v3_FINAL
WHERE subtotal = 0 
  AND shipping_cost > 0 
  AND (is_refunded IS NULL OR is_refunded = FALSE)
  AND order_date >= CURRENT_DATE() - INTERVAL '3 days';


-- ==============================================================================
-- SECTION 3: DOWNSTREAM PIPELINE SIMULATION (daily_revenue_etl)
-- Asset: daily_revenue_etl (urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD))
--
-- This query simulates how the daily revenue ETL aggregates data from orders_v3_FINAL
-- while safely handling the zero-dollar subtotal edge case to prevent negative margins.
-- ==============================================================================

WITH safe_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        order_status,
        -- Safe margin handling: If subtotal is 0 and shipping is positive, and it's not marked refunded,
        -- we treat shipping as 0 in revenue calculations to prevent downstream pipeline crashes.
        CASE 
            WHEN subtotal = 0 AND shipping_cost > 0 AND (is_refunded IS NULL OR is_refunded = FALSE) THEN 0
            ELSE subtotal 
        END AS adjusted_subtotal,
        shipping_cost,
        is_refunded
    FROM orders_v3_FINAL
)
SELECT 
    CAST(order_date AS DATE) AS revenue_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(adjusted_subtotal) AS gross_subtotal,
    SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE shipping_cost END) AS net_shipping_revenue,
    SUM(adjusted_subtotal + CASE WHEN is_refunded = TRUE THEN 0 ELSE shipping_cost END) AS total_net_revenue
FROM safe_orders
WHERE order_status NOT IN ('cancelled')
GROUP BY 1
ORDER BY 1 DESC;


-- ==============================================================================
-- SECTION 4: CUSTOMER CHURN FEATURE GENERATION
-- Asset: churn_prediction_features (urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD))
--
-- This query demonstrates how orders_v3_FINAL joins with dim_customers to generate
-- features for the churn prediction model.
-- ==============================================================================

-- CREATE OR REPLACE TABLE churn_prediction_features AS
SELECT 
    c.customer_id,
    c.signup_date,
    c.customer_segment,
    COUNT(o.order_id) AS total_lifetime_orders,
    COALESCE(SUM(o.subtotal), 0) AS lifetime_spend,
    COALESCE(AVG(o.subtotal), 0) AS average_order_value,
    MAX(o.order_date) AS last_purchase_timestamp,
    DATEDIFF('day', MAX(o.order_date), CURRENT_TIMESTAMP()) AS days_since_last_purchase,
    COUNT(CASE WHEN o.order_status = 'refunded' THEN 1 END) AS total_refunded_orders
FROM dim_customers c
LEFT JOIN orders_v3_FINAL o 
    ON c.customer_id = o.customer_id
GROUP BY 
    c.customer_id, 
    c.signup_date, 
    c.customer_segment;


-- ==============================================================================
-- SECTION 5: QUARTERLY BOARD METRICS GENERATION
-- Asset: quarterly_board_metrics (urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD))
--
-- Aggregates high-level business performance metrics for executive reporting.
-- ==============================================================================

-- CREATE OR REPLACE TABLE quarterly_board_metrics AS
SELECT 
    DATE_TRUNC('quarter', o.order_date) AS reporting_quarter,
    COUNT(DISTINCT o.customer_id) AS unique_purchasing_customers,
    COUNT(o.order_id) AS total_orders_processed,
    SUM(o.subtotal) AS gross_merchandise_value_gmv,
    SUM(CASE WHEN o.is_refunded = TRUE THEN o.subtotal ELSE 0 END) AS total_refunded_amount,
    (SUM(CASE WHEN o.is_refunded = TRUE THEN o.subtotal ELSE 0 END) / NULLIF(SUM(o.subtotal), 0)) * 100 AS refund_rate_percentage
FROM orders_v3_FINAL o
WHERE o.order_status != 'cancelled'
GROUP BY 1
ORDER BY 1 DESC;


-- ==============================================================================
-- SECTION 6: HISTORICAL RECONCILIATION & MIGRATION PATHWAY
-- Assets: orders_v1 -> orders_v2 -> orders_v3_FINAL
--
-- Use this query to audit differences between legacy schemas and the current production table.
-- ==============================================================================

SELECT 
    'orders_v1' AS source_version,
    COUNT(*) AS record_count,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date,
    NULL AS has_refund_flag
FROM orders_v1

UNION ALL

SELECT 
    'orders_v2' AS source_version,
    COUNT(*) AS record_count,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date,
    NULL AS has_refund_flag
FROM orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL' AS source_version,
    COUNT(*) AS record_count,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date,
    'YES' AS has_refund_flag
FROM orders_v3_FINAL;


-- ==============================================================================
-- SECTION 7: REAL-TIME EVENT STREAM AUDIT
-- Asset: kafka_events_raw_copy (urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD))
--
-- Raw event stream copy used to debug order ingestion issues before they hit Snowflake.
-- ==============================================================================

SELECT 
    event_id,
    event_timestamp,
    payload:order_id::VARCHAR AS extracted_order_id,
    payload:event_type::VARCHAR AS event_type,
    payload:stripe_status::VARCHAR AS stripe_status,
    payload
FROM kafka_events_raw_copy
WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '4 hours'
ORDER BY event_timestamp DESC
LIMIT 100;