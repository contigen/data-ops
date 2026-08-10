-- =================================================================================
-- DATA ENGINEERING HANDOVER REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Purpose: Operational runbooks, asset mapping, and reference queries for successor.
-- =================================================================================

-- =================================================================================
-- ASSET DIRECTORY & METADATA
-- =================================================================================
-- 1. Snowflake Dataset: orders_v3_FINAL
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)
--    SLA: Daily 6:00 AM UTC.
--    Quality: dbt tests enforced (not_null, accepted_values on order_status).
--
-- 2. Looker Dashboard: revenue_dashboard_BACKUP
--    URN: urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD)
--
-- 3. Kafka Topic: kafka_events_raw_copy
--    URN: urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD)
--
-- 4. Snowflake Dataset: orders_v2 (Deprecated/Legacy)
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD)
--
-- 5. Snowflake Dataset: orders_v1 (Deprecated/Legacy)
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD)
--
-- 6. Snowflake Dataset: quarterly_board_metrics
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD)
--
-- 7. Airflow DAG: daily_revenue_etl
--    URN: urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD)
--
-- 8. Snowflake Dataset: churn_prediction_features
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD)
--
-- 9. Snowflake Dataset: dim_customers
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD)
-- =================================================================================


-- =================================================================================
-- OPERATIONAL RUNBOOK: orders_v3_FINAL -> daily_revenue_etl BREAKAGE
-- =================================================================================
-- CRITICAL EDGE CASE:
-- Canceled orders with a $0.00 subtotal and a positive shipping cost will break 
-- the downstream `daily_revenue_etl` pipeline (causing negative margin calculations) 
-- if the `is_refunded` flag is not set to TRUE within a 24-hour window.
--
-- RECOVERY STEPS:
-- 1. Run the reconciliation script from the `data-ops` repository:
--    python stripe_reconciliation_catchup.py --order-ids <comma_separated_ids>
--    *Note: If batch size > 500, append `--batch-size 100` to avoid Stripe rate limits.
--
-- 2. Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG 
--    for the affected execution date to recalculate the downstream Looker dashboard.
-- =================================================================================


-- =================================================================================
-- MOCK SCHEMA CREATION (For Reference & Local Testing)
-- =================================================================================

-- Mocking dim_customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    signup_date DATE,
    country VARCHAR(50),
    is_active BOOLEAN
);

-- Mocking orders_v3_FINAL
CREATE TABLE IF NOT EXISTS orders_v3_FINAL (
    order_id INT PRIMARY KEY,
    customer_id INT REFERENCES dim_customers(customer_id),
    order_status VARCHAR(50) NOT NULL, -- dbt enforced: 'completed', 'pending', 'canceled', 'refunded'
    subtotal NUMBER(12, 2),
    shipping_cost NUMBER(12, 2),
    tax NUMBER(12, 2),
    total_amount NUMBER(12, 2),
    is_refunded BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_TZ,
    updated_at TIMESTAMP_TZ
);

-- Mocking quarterly_board_metrics
CREATE TABLE IF NOT EXISTS quarterly_board_metrics (
    quarter VARCHAR(10), -- e.g., '2023-Q4'
    total_revenue NUMBER(15, 2),
    active_customers INT,
    average_order_value NUMBER(10, 2),
    refund_rate NUMBER(5, 2),
    updated_at TIMESTAMP_TZ
);

-- Mocking churn_prediction_features
CREATE TABLE IF NOT EXISTS churn_prediction_features (
    customer_id INT PRIMARY KEY,
    total_orders INT,
    total_spend NUMBER(12, 2),
    days_since_last_order INT,
    refund_count INT,
    churn_risk_score NUMBER(5, 2)
);


-- =================================================================================
-- REFERENCE & MONITORING QUERIES
-- =================================================================================

-- ---------------------------------------------------------------------------------
-- QUERY 1: SLA & Data Freshness Verification
-- Checks if the daily load completed before the 6:00 AM UTC SLA.
-- ---------------------------------------------------------------------------------
SELECT 
    DATE(created_at) AS order_date,
    MAX(updated_at) AS last_updated_at,
    COUNT(*) AS total_records,
    CASE 
        WHEN MAX(updated_at) <= DATE(created_at) + TIME '06:00:00' THEN 'SLA Met'
        ELSE 'SLA Violated'
    END AS sla_status
FROM orders_v3_FINAL
GROUP BY DATE(created_at)
ORDER BY order_date DESC
LIMIT 14;


-- ---------------------------------------------------------------------------------
-- QUERY 2: Edge Case Detection (Zero-Dollar Subtotal / Positive Shipping Cost)
-- Run this query to identify orders that will break the downstream daily_revenue_etl.
-- If records are returned here and updated_at is > 24 hours old, trigger the Stripe recovery script.
-- ---------------------------------------------------------------------------------
SELECT 
    order_id,
    customer_id,
    subtotal,
    shipping_cost,
    is_refunded,
    order_status,
    created_at,
    DATEDIFF('hour', created_at, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM orders_v3_FINAL
WHERE 
    subtotal = 0.00 
    AND shipping_cost > 0.00 
    AND order_status = 'canceled'
    AND is_refunded = FALSE;


-- ---------------------------------------------------------------------------------
-- QUERY 3: Downstream Simulation (daily_revenue_etl logic)
-- Simulates the aggregation logic used to populate the Looker backup dashboard.
-- Handles the edge case by filtering out unrefunded zero-dollar anomalies to prevent negative margins.
-- ---------------------------------------------------------------------------------
WITH cleaned_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_status,
        subtotal,
        shipping_cost,
        tax,
        total_amount,
        is_refunded,
        DATE(created_at) AS order_date
    FROM orders_v3_FINAL
    WHERE NOT (
        subtotal = 0.00 
        AND shipping_cost > 0.00 
        AND is_refunded = FALSE 
        AND order_status = 'canceled'
    ) -- Exclude the pipeline-breaking edge case records
)
SELECT 
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(CASE WHEN order_status != 'refunded' THEN subtotal ELSE 0 END) AS gross_revenue,
    SUM(shipping_cost) AS total_shipping_revenue,
    SUM(CASE WHEN is_refunded = TRUE THEN (subtotal + shipping_cost) ELSE 0 END) AS total_refunded_amount,
    (SUM(CASE WHEN order_status != 'refunded' THEN subtotal ELSE 0 END) - 
     SUM(CASE WHEN is_refunded = TRUE THEN subtotal ELSE 0 END)) AS net_revenue
FROM cleaned_orders
GROUP BY order_date
ORDER BY order_date DESC;


-- ---------------------------------------------------------------------------------
-- QUERY 4: Feature Engineering Pipeline (churn_prediction_features)
-- Demonstrates how dim_customers and orders_v3_FINAL join to build ML features.
-- ---------------------------------------------------------------------------------
INSERT OVERWRITE TABLE churn_prediction_features
SELECT 
    c.customer_id,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.subtotal), 0.00) AS total_spend,
    COALESCE(DATEDIFF('day', MAX(o.created_at), CURRENT_TIMESTAMP()), 999) AS days_since_last_order,
    COUNT(CASE WHEN o.is_refunded = TRUE THEN 1 END) AS refund_count,
    -- Simple heuristic for churn risk score
    CASE 
        WHEN DATEDIFF('day', MAX(o.created_at), CURRENT_TIMESTAMP()) > 180 THEN 0.90
        WHEN DATEDIFF('day', MAX(o.created_at), CURRENT_TIMESTAMP()) > 90 THEN 0.60
        WHEN COUNT(CASE WHEN o.is_refunded = TRUE THEN 1 END) > 2 THEN 0.75
        ELSE 0.15
    END AS churn_risk_score
FROM dim_customers c
LEFT JOIN orders_v3_FINAL o 
    ON c.customer_id = o.customer_id
WHERE c.is_active = TRUE
GROUP BY c.customer_id;


-- ---------------------------------------------------------------------------------
-- QUERY 5: Quarterly Board Metrics Generation
-- Aggregates orders_v3_FINAL data to populate quarterly_board_metrics.
-- ---------------------------------------------------------------------------------
INSERT OVERWRITE TABLE quarterly_board_metrics
WITH quarterly_raw AS (
    SELECT 
        CONCAT(YEAR(created_at), '-Q', QUARTER(created_at)) AS quarter_period,
        o.order_id,
        o.customer_id,
        o.total_amount,
        o.is_refunded
    FROM orders_v3_FINAL o
    WHERE o.order_status = 'completed' OR o.is_refunded = TRUE
)
SELECT 
    quarter_period AS quarter,
    SUM(CASE WHEN is_refunded = FALSE THEN total_amount ELSE 0 END) AS total_revenue,
    COUNT(DISTINCT customer_id) AS active_customers,
    ROUND(AVG(CASE WHEN is_refunded = FALSE THEN total_amount END), 2) AS average_order_value,
    ROUND((COUNT(CASE WHEN is_refunded = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)), 2) AS refund_rate,
    CURRENT_TIMESTAMP() AS updated_at
FROM quarterly_raw
GROUP BY quarter_period
ORDER BY quarter_period DESC;


-- ---------------------------------------------------------------------------------
-- QUERY 6: Legacy Data Reconciliation (orders_v1 vs orders_v2 vs orders_v3_FINAL)
-- Useful for historical audits or backfilling older data.
-- ---------------------------------------------------------------------------------
SELECT 
    'orders_v1' AS source_version,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date
FROM orders_v1
UNION ALL
SELECT 
    'orders_v2' AS source_version,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date
FROM orders_v2
UNION ALL
SELECT 
    'orders_v3_FINAL' AS source_version,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date
FROM orders_v3_FINAL;