-- =================================================================================
-- HANDOVER REFERENCE SQL SCRIPT
-- DEPARTING ENGINEER: alice.chen
-- TARGET ASSETS: orders_v3_FINAL, dim_customers, quarterly_board_metrics, 
--                churn_prediction_features, orders_v2, orders_v1, 
--                kafka_events_raw_copy, daily_revenue_etl, revenue_dashboard_BACKUP
-- =================================================================================

-- ---------------------------------------------------------------------------------
-- SECTION 1: SLA MONITORING & OPERATIONAL RECOVERY (orders_v3_FINAL)
-- ---------------------------------------------------------------------------------
-- Table: orders_v3_FINAL (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD))
-- SLA: Daily update by 6:00 AM UTC.
--
-- CRITICAL EDGE CASE:
-- Canceled orders with a $0 subtotal and a positive shipping cost will break the 
-- downstream 'daily_revenue_etl' pipeline (causing negative margin calculations) 
-- if the 'is_refunded' flag is not set to 'true' within a 24-hour window.
--
-- OPERATIONAL RECOVERY PROCEDURE (If webhook fails):
-- 1. Run reconciliation script from 'data-ops' repo:
--    python stripe_reconciliation_catchup.py --order-ids <comma_separated_ids>
--    * Note: If batch > 500 records, append "--batch-size 100" to avoid rate limits.
-- 2. Manually trigger Airflow backfill on 'daily_revenue_etl' DAG for affected date.
-- ---------------------------------------------------------------------------------

-- Query 1.1: Active Monitoring Query for the Zero-Dollar Subtotal Edge Case
-- Run this query to identify stuck orders that require manual Stripe reconciliation.
SELECT 
    order_id,
    customer_id,
    order_date,
    subtotal,
    shipping_cost,
    is_refunded,
    order_status,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM prod.orders_v3_FINAL
WHERE 
    subtotal = 0.00 
    AND shipping_cost > 0.00 
    AND (is_refunded IS NULL OR is_refunded = FALSE)
    AND order_date >= DATEADD('day', -2, CURRENT_DATE())
ORDER BY order_date DESC;


-- ---------------------------------------------------------------------------------
-- SECTION 2: DOWNSTREAM PIPELINE SIMULATION (daily_revenue_etl)
-- ---------------------------------------------------------------------------------
-- This query demonstrates how the daily_revenue_etl pipeline aggregates data 
-- from orders_v3_FINAL to populate the executive dashboard, incorporating 
-- protection against the zero-dollar subtotal edge case.
-- ---------------------------------------------------------------------------------

-- Query 2.1: Daily Revenue & Margin Aggregation (Core logic for daily_revenue_etl)
WITH daily_metrics AS (
    SELECT 
        CAST(order_date AS DATE) AS revenue_date,
        COUNT(DISTINCT order_id) AS total_orders,
        -- If refunded, net subtotal is 0. If not refunded, use subtotal.
        SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE subtotal END) AS net_subtotal,
        SUM(shipping_cost) AS total_shipping_collected,
        -- Margin calculation logic protecting against negative margins from unrefunded $0 subtotal orders
        SUM(
            CASE 
                WHEN subtotal = 0 AND shipping_cost > 0 AND (is_refunded IS NULL OR is_refunded = FALSE)
                THEN 0 -- Force margin to 0 to prevent pipeline breakage/negative margin alerts
                WHEN is_refunded = TRUE 
                THEN 0
                ELSE (subtotal + shipping_cost) * 0.40 -- Assuming a flat 40% margin rule for demo
            END
        ) AS estimated_margin
    FROM prod.orders_v3_FINAL
    WHERE order_status IN ('COMPLETED', 'SHIPPED', 'DELIVERED', 'CANCELLED') -- dbt accepted_values
      AND order_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY 1
)
SELECT 
    revenue_date,
    total_orders,
    net_subtotal,
    total_shipping_collected,
    estimated_margin,
    -- Flag potential data quality issues to the Looker Backup Dashboard
    CASE 
        WHEN net_subtotal = 0 AND total_shipping_collected > 0 THEN 'WARNING: Unreconciled Refunds'
        ELSE 'OK'
    END AS data_quality_status
FROM daily_metrics
ORDER BY revenue_date DESC;


-- ---------------------------------------------------------------------------------
-- SECTION 3: CUSTOMER & CHURN ANALYTICS (dim_customers & churn_prediction_features)
-- ---------------------------------------------------------------------------------
-- This query shows how orders_v3_FINAL joins with dim_customers to generate 
-- features for the churn_prediction_features table.
-- ---------------------------------------------------------------------------------

-- Query 3.1: Feature Engineering for Churn Prediction
-- Target Table: prod.churn_prediction_features
INSERT OVERWRITE INTO prod.churn_prediction_features (
    customer_id,
    signup_date,
    total_lifetime_orders,
    total_lifetime_spend,
    days_since_last_order,
    refunded_orders_count,
    has_zero_dollar_shipping_anomaly
)
SELECT 
    c.customer_id,
    c.signup_date,
    COUNT(o.order_id) AS total_lifetime_orders,
    COALESCE(SUM(CASE WHEN o.is_refunded = FALSE THEN o.subtotal ELSE 0 END), 0) AS total_lifetime_spend,
    DATEDIFF('day', MAX(o.order_date), CURRENT_TIMESTAMP()) AS days_since_last_order,
    COUNT(CASE WHEN o.is_refunded = TRUE THEN 1 END) AS refunded_orders_count,
    -- Flag customers who experienced the zero-dollar subtotal edge case
    MAX(CASE WHEN o.subtotal = 0 AND o.shipping_cost > 0 AND (o.is_refunded IS NULL OR o.is_refunded = FALSE) THEN 1 ELSE 0 END) AS has_zero_dollar_shipping_anomaly
FROM prod.dim_customers c
LEFT JOIN prod.orders_v3_FINAL o 
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.signup_date;


-- ---------------------------------------------------------------------------------
-- SECTION 4: BOARD METRICS & HISTORICAL RECONCILIATION
-- ---------------------------------------------------------------------------------
-- Validates historical consistency across orders_v1, orders_v2, and orders_v3_FINAL 
-- to ensure quarterly board metrics remain stable.
-- ---------------------------------------------------------------------------------

-- Query 4.1: Cross-Version Order Reconciliation
-- Used to verify data integrity during migrations or when auditing quarterly_board_metrics
WITH v1_summary AS (
    SELECT 
        DATE_TRUNC('quarter', order_timestamp) AS fiscal_quarter,
        COUNT(DISTINCT order_id) AS v1_order_count,
        SUM(order_amount) AS v1_total_revenue
    FROM prod.orders_v1
    GROUP BY 1
),
v2_summary AS (
    SELECT 
        DATE_TRUNC('quarter', order_datetime) AS fiscal_quarter,
        COUNT(DISTINCT id) AS v2_order_count,
        SUM(subtotal_amount) AS v2_total_revenue
    FROM prod.orders_v2
    GROUP BY 1
),
v3_summary AS (
    SELECT 
        DATE_TRUNC('quarter', order_date) AS fiscal_quarter,
        COUNT(DISTINCT order_id) AS v3_order_count,
        SUM(subtotal) AS v3_total_revenue
    FROM prod.orders_v3_FINAL
    GROUP BY 1
)
SELECT 
    COALESCE(v3.fiscal_quarter, v2.fiscal_quarter, v1.fiscal_quarter) AS fiscal_quarter,
    v1.v1_order_count,
    v2.v2_order_count,
    v3.v3_order_count,
    v1.v1_total_revenue,
    v2.v2_total_revenue,
    v3.v3_total_revenue,
    -- Discrepancy checks
    (v3.v3_order_count - v2.v2_order_count) AS v2_v3_order_diff,
    (v3.v3_total_revenue - v2.v2_total_revenue) AS v2_v3_revenue_diff
FROM v3_summary v3
FULL OUTER JOIN v2_summary v2 ON v3.fiscal_quarter = v2.fiscal_quarter
FULL OUTER JOIN v1_summary v1 ON COALESCE(v3.fiscal_quarter, v2.fiscal_quarter) = v1.fiscal_quarter
ORDER BY fiscal_quarter DESC;


-- ---------------------------------------------------------------------------------
-- SECTION 5: KAFKA EVENT INGESTION AUDITING
-- ---------------------------------------------------------------------------------
-- Audits raw events from kafka_events_raw_copy against orders_v3_FINAL to 
-- measure ingestion latency and identify dropped messages.
-- ---------------------------------------------------------------------------------

-- Query 5.1: Ingestion Latency & Completeness Audit
-- Parses JSON from raw Kafka events and matches them to the final Snowflake table.
WITH parsed_kafka_events AS (
    SELECT 
        -- Assuming Snowflake variant column 'event_data' containing JSON
        GET_PATH(event_data, 'payload.order_id')::STRING AS order_id,
        GET_PATH(event_data, 'payload.customer_id')::STRING AS customer_id,
        TO_TIMESTAMP_NTZ(GET_PATH(event_data, 'timestamp')::NUMERIC / 1000) AS kafka_event_time
    FROM raw.kafka_events_raw_copy
    WHERE GET_PATH(event_data, 'event_type')::STRING = 'ORDER_CREATED'
      AND TO_TIMESTAMP_NTZ(GET_PATH(event_data, 'timestamp')::NUMERIC / 1000) >= DATEADD('day', -7, CURRENT_DATE())
)
SELECT 
    k.order_id,
    k.kafka_event_time,
    o.order_date AS snowflake_load_time,
    DATEDIFF('minute', k.kafka_event_time, o.order_date) AS ingestion_latency_minutes,
    CASE 
        WHEN o.order_id IS NULL THEN 'MISSING_IN_SNOWFLAKE'
        ELSE 'LOADED'
    END AS ingestion_status
FROM parsed_kafka_events k
LEFT JOIN prod.orders_v3_FINAL o 
    ON k.order_id = o.order_id
ORDER BY k.kafka_event_time DESC;