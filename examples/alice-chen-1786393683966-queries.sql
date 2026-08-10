-- ==============================================================================
-- HANDOVER REFERENCE SCRIPT: ALICE CHEN'S DATA ASSETS
-- Target Audience: Successor Data Engineer
-- Author: Ghost (on behalf of departing engineer alice.chen)
--
-- This script serves as an operational runbook and SQL reference for managing,
-- monitoring, and querying the data assets previously owned by Alice Chen.
--
-- Covered Assets:
-- 1. Snowflake: orders_v3_FINAL (Prod) - Primary Orders Source of Truth
-- 2. Snowflake: dim_customers (Prod) - Customer Dimension
-- 3. Snowflake: quarterly_board_metrics (Prod) - Executive Reporting Table
-- 4. Snowflake: churn_prediction_features (Prod) - ML Feature Store Table
-- 5. Snowflake: orders_v2 & orders_v1 (Prod) - Legacy Order Tables
-- 6. Airflow:   daily_revenue_etl (Prod) - Downstream ETL Pipeline
-- 7. Looker:    revenue_dashboard_BACKUP (Prod) - Backup Dashboard
-- 8. Kafka:     kafka_events_raw_copy (Prod) - Raw Event Stream Copy
-- ==============================================================================


-- ==============================================================================
-- SECTION 1: OPERATIONAL MONITORING & SLA COMPLIANCE (orders_v3_FINAL)
-- ==============================================================================
-- SLA: Must be updated daily by 6:00 AM UTC.
-- Quality: dbt assertions enforce 'not_null' and 'accepted_values' on 'order_status'.
--
-- CRITICAL EDGE CASE:
-- Canceled orders with a $0.00 subtotal but a positive shipping cost will break 
-- the downstream 'daily_revenue_etl' pipeline (causing negative margin calculations)
-- if the 'is_refunded' flag is not set to TRUE within a 24-hour window.
-- ==============================================================================

-- Query 1.1: SLA Verification Query
-- Run this to verify if the daily load completed before the 6:00 AM UTC SLA.
SELECT 
    MAX(created_at) AS last_order_timestamp,
    MAX(updated_at) AS last_updated_timestamp,
    CURRENT_TIMESTAMP() AS current_check_time,
    CASE 
        -- Check if the table has been updated with today's data before 06:00:00 UTC
        WHEN MAX(updated_at) >= DATE_TRUNC('day', CURRENT_TIMESTAMP()) 
             AND TO_TIME(MAX(updated_at)) <= '06:00:00'::TIME 
        THEN 'SLA MET'
        ELSE 'SLA BREACHED / PENDING'
    END AS sla_status
FROM PROD.orders_v3_FINAL;


-- Query 1.2: Critical Edge Case Detection (The "Pipeline Breaker" Query)
-- Run this query daily to identify any zero-dollar subtotal orders with positive 
-- shipping costs that have NOT been marked as refunded. 
-- If this query returns rows older than 24 hours, the downstream 'daily_revenue_etl' 
-- is at risk of failing or producing negative margins.
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
WHERE subtotal_amount = 0.00
  AND shipping_amount > 0.00
  AND (is_refunded IS NULL OR is_refunded = FALSE)
  AND order_status = 'cancelled'
ORDER BY created_at DESC;


-- ==============================================================================
-- OPERATIONAL RECOVERY PROCEDURE (If Query 1.2 returns active rows > 24h old)
-- ==============================================================================
-- The 'is_refunded' flag is populated via a Stripe webhook ingestion API.
-- If the webhook fails to sync within the 24-hour window, execute these steps:
--
-- Step 1: Run the Reconciliation Script
--   Navigate to the 'data-ops' repository and execute:
--   python stripe_reconciliation_catchup.py --order-ids <comma_separated_ids_from_query_1_2>
--
--   *Note on Rate Limiting*: If the batch size of affected orders exceeds 500 records,
--   you MUST append the '--batch-size 100' flag to prevent Stripe API rate-limiting:
--   python stripe_reconciliation_catchup.py --file-path path_to_affected_ids.csv --batch-size 100
--
-- Step 2: Downstream Backfill
--   Manually trigger an Airflow backfill on the 'daily_revenue_etl' DAG for the 
--   affected execution date(s) to recalculate downstream metrics and update the 
--   Looker 'revenue_dashboard_BACKUP'.
-- ==============================================================================


-- ==============================================================================
-- SECTION 2: DOWNSTREAM PIPELINES & ANALYTICS INTEGRATION
-- ==============================================================================
-- This section demonstrates how orders_v3_FINAL integrates with dim_customers,
-- quarterly_board_metrics, and churn_prediction_features.
-- ==============================================================================

-- Query 2.1: Daily Revenue ETL Simulation (Downstream of orders_v3_FINAL)
-- This query mimics the core logic of the 'daily_revenue_etl' Airflow DAG.
-- It calculates daily gross revenue, shipping revenue, refunds, and net margin.
CREATE OR REPLACE TEMPORARY TABLE temp_daily_revenue_summary AS
SELECT 
    DATE(o.created_at) AS revenue_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    SUM(o.subtotal_amount) AS gross_merchandise_value,
    SUM(o.shipping_amount) AS total_shipping_collected,
    -- Safe margin calculation preventing negative margins from unrefunded edge cases
    SUM(
        CASE 
            WHEN o.subtotal_amount = 0.00 AND o.shipping_amount > 0.00 AND o.is_refunded = FALSE 
            THEN 0.00 -- Force zero margin to prevent pipeline break during edge-case window
            ELSE (o.subtotal_amount + o.shipping_amount) 
        END
    ) AS net_revenue,
    SUM(CASE WHEN o.is_refunded = TRUE THEN (o.subtotal_amount + o.shipping_amount) ELSE 0.00 END) AS total_refunded_amount
FROM PROD.orders_v3_FINAL o
WHERE o.order_status IN ('completed', 'shipped', 'cancelled') -- dbt accepted_values
GROUP BY 1
ORDER BY 1 DESC;

SELECT * FROM temp_daily_revenue_summary LIMIT 10;


-- Query 2.2: Quarterly Board Metrics Generation
-- Demonstrates how quarterly_board_metrics is populated using orders_v3_FINAL and dim_customers.
-- This query aggregates customer cohorts and high-value order metrics.
SELECT 
    DATE_TRUNC('quarter', o.created_at) AS board_quarter,
    c.customer_segment,
    COUNT(DISTINCT o.customer_id) AS total_purchasing_customers,
    SUM(o.subtotal_amount) AS total_quarterly_spend,
    AVG(o.subtotal_amount) AS average_order_value,
    -- High-value customer ratio (customers spending > $1000 in the quarter)
    COUNT(DISTINCT CASE WHEN o.subtotal_amount > 1000 THEN o.customer_id END) * 100.0 / COUNT(DISTINCT o.customer_id) AS pct_high_value_customers
FROM PROD.orders_v3_FINAL o
INNER JOIN PROD.dim_customers c 
    ON o.customer_id = c.customer_id
WHERE o.order_status = 'completed'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;


-- Query 2.3: Churn Prediction Feature Generation
-- This query populates features for the 'churn_prediction_features' ML table.
-- It calculates recency, frequency, and monetary (RFM) metrics per customer.
SELECT 
    c.customer_id,
    c.signup_date,
    c.customer_segment,
    DATEDIFF('day', MAX(o.created_at), CURRENT_DATE()) AS days_since_last_purchase,
    COUNT(DISTINCT o.order_id) AS lifetime_order_count,
    SUM(o.subtotal_amount) AS lifetime_spend,
    SUM(CASE WHEN o.is_refunded = TRUE THEN 1 ELSE 0 END) AS total_refunded_orders,
    -- Ratio of refunded orders to total orders (churn risk indicator)
    DIV0(SUM(CASE WHEN o.is_refunded = TRUE THEN 1 ELSE 0 END), COUNT(DISTINCT o.order_id)) AS refund_ratio
FROM PROD.dim_customers c
LEFT JOIN PROD.orders_v3_FINAL o 
    ON c.customer_id = o.customer_id
GROUP BY 1, 2, 3;


-- ==============================================================================
-- SECTION 3: LEGACY DATA AUDITING & MIGRATION VERIFICATION
-- ==============================================================================
-- Alice Chen managed the migration from orders_v1 -> orders_v2 -> orders_v3_FINAL.
-- Use these queries to audit historical data consistency across schemas if needed.
-- ==============================================================================

-- Query 3.1: Schema Reconciliation (v1 vs v2 vs v3_FINAL)
-- Ensures that historical order counts and financial sums match across legacy tables.
WITH v1_metrics AS (
    SELECT 
        'orders_v1' AS table_version,
        COUNT(*) AS total_records,
        SUM(order_total) AS total_financials -- v1 used 'order_total'
    FROM PROD.orders_v1
),
v2_metrics AS (
    SELECT 
        'orders_v2' AS table_version,
        COUNT(*) AS total_records,
        SUM(subtotal + shipping) AS total_financials -- v2 split subtotal and shipping
    FROM PROD.orders_v2
),
v3_metrics AS (
    SELECT 
        'orders_v3_FINAL' AS table_version,
        COUNT(*) AS total_records,
        SUM(subtotal_amount + shipping_amount) AS total_financials
    FROM PROD.orders_v3_FINAL
)
SELECT * FROM v1_metrics
UNION ALL
SELECT * FROM v2_metrics
UNION ALL
SELECT * FROM v3_metrics;


-- Query 3.2: Kafka Raw Event Stream Audit (kafka_events_raw_copy)
-- Useful for debugging webhook ingestion failures or raw event mismatches.
-- This query parses raw JSON payloads from the Kafka copy table to verify 
-- if refund events are being received for the zero-dollar edge case.
SELECT 
    payload:event_id::STRING AS event_id,
    payload:event_type::STRING AS event_type,
    payload:data:object:id::STRING AS stripe_charge_id,
    payload:data:object:metadata:order_id::STRING AS order_id,
    payload:data:object:amount_refunded::DOUBLE / 100.0 AS parsed_refund_amount,
    TO_TIMESTAMP(payload:created::INT) AS event_timestamp
FROM PROD.kafka_events_raw_copy
WHERE payload:event_type::STRING = 'charge.refunded'
  AND TO_TIMESTAMP(payload:created::INT) >= DATEADD('day', -3, CURRENT_TIMESTAMP())
ORDER BY event_timestamp DESC;