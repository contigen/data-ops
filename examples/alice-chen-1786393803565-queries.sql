-- =================================================================================
-- HANDOVER REFERENCE SQL SCRIPT
-- Departing Engineer: alice.chen
-- Target Audience: Successor Data Engineer / Analytics Engineer
-- Date: October 2023
-- =================================================================================

-- =================================================================================
-- ASSET DIRECTORY & LINEAGE OVERVIEW
-- =================================================================================
-- 1. Snowflake Datasets:
--    * PROD.orders_v3_FINAL (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD))
--    * PROD.orders_v2 [Legacy/Deprecated] (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD))
--    * PROD.orders_v1 [Legacy/Deprecated] (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD))
--    * PROD.dim_customers (urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD))
--    * PROD.quarterly_board_metrics (urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD))
--    * PROD.churn_prediction_features (urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD))
--
-- 2. Airflow Pipelines:
--    * daily_revenue_etl (urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD))
--
-- 3. Kafka Streams:
--    * kafka_events_raw_copy (urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD))
--
-- 4. BI Dashboards:
--    * revenue_dashboard_BACKUP [Looker] (urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD))
-- =================================================================================


-- =================================================================================
-- OPERATIONAL RUNBOOK: orders_v3_FINAL & daily_revenue_etl
-- =================================================================================
-- SLA: orders_v3_FINAL must be updated daily by 6:00 AM UTC.
-- DBT TESTS: The table has passed strict dbt data quality assertions:
--   - not_null on critical columns (order_id, customer_id, order_date)
--   - accepted_values on order_status ('completed', 'pending', 'canceled', 'returned')
--
-- CRITICAL EDGE CASE:
-- Canceled orders with a $0.00 subtotal but a positive shipping cost will break 
-- the downstream `daily_revenue_etl` pipeline (causing negative margin calculations)
-- IF the `is_refunded` flag is not set to TRUE within a 24-hour window.
--
-- RECOVERY PROCEDURE (If Stripe Webhook fails to sync is_refunded):
-- 1. Run the reconciliation script from the `data-ops` repository:
--    python stripe_reconciliation_catchup.py --order-ids <comma_separated_ids>
--    * Note: If batch size > 500, append `--batch-size 100` to prevent rate-limiting.
-- 2. Manually trigger Airflow backfill on `daily_revenue_etl` for the affected date.
-- =================================================================================


-- ---------------------------------------------------------------------------------
-- QUERY 1: SLA & Edge-Case Monitoring Query
-- Use this query to proactively detect the zero-dollar subtotal anomaly before 
-- it breaks the downstream daily_revenue_etl pipeline.
-- ---------------------------------------------------------------------------------

SELECT 
    order_id,
    customer_id,
    order_date,
    order_status,
    subtotal_amount,
    shipping_amount,
    is_refunded,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_order,
    -- Flagging the exact edge case condition:
    CASE 
        WHEN subtotal_amount = 0 
             AND shipping_amount > 0 
             AND order_status = 'canceled' 
             AND (is_refunded IS NULL OR is_refunded = FALSE)
        THEN 'CRITICAL_ANOMALY_ACTION_REQUIRED'
        ELSE 'OK'
    END AS health_status
FROM 
    PROD.orders_v3_FINAL
WHERE 
    order_date >= DATEADD('day', -3, CURRENT_DATE())
ORDER BY 
    health_status DESC, 
    order_date DESC;


-- ---------------------------------------------------------------------------------
-- QUERY 2: Downstream Simulation (daily_revenue_etl)
-- This query mimics the core aggregation logic of the daily_revenue_etl pipeline,
-- joining orders_v3_FINAL with dim_customers.
-- ---------------------------------------------------------------------------------

CREATE OR REPLACE TABLE PROD.daily_revenue_summary_STAGE AS
WITH cleaned_orders AS (
    SELECT
        order_id,
        customer_id,
        CAST(order_date AS DATE) AS order_date,
        order_status,
        subtotal_amount,
        shipping_amount,
        -- Safe margin calculation handling the zero-dollar subtotal edge case:
        CASE 
            WHEN subtotal_amount = 0 AND shipping_amount > 0 AND is_refunded = TRUE 
                THEN 0.00
            ELSE (subtotal_amount + shipping_amount) * 0.45 -- Assuming 45% standard margin
        END AS estimated_margin,
        (subtotal_amount + shipping_amount) AS gross_revenue
    FROM 
        PROD.orders_v3_FINAL
    WHERE 
        -- Ensure we only pull validated records
        order_status IN ('completed', 'pending', 'canceled', 'returned')
)
SELECT 
    o.order_date,
    c.customer_segment,
    c.country,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.gross_revenue) AS daily_gross_revenue,
    SUM(o.estimated_margin) AS daily_estimated_margin,
    SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_orders_count
FROM 
    cleaned_orders o
LEFT JOIN 
    PROD.dim_customers c ON o.customer_id = c.customer_id
GROUP BY 
    1, 2, 3;


-- ---------------------------------------------------------------------------------
-- QUERY 3: Quarterly Board Metrics Generation
-- Populates the PROD.quarterly_board_metrics table using orders_v3_FINAL.
-- ---------------------------------------------------------------------------------

INSERT OVERWRITE INTO PROD.quarterly_board_metrics (
    fiscal_quarter,
    total_active_customers,
    total_orders_processed,
    gross_merchandise_value,
    average_order_value,
    refund_rate
)
SELECT 
    CONCAT(YEAR(o.order_date), '-Q', QUARTER(o.order_date)) AS fiscal_quarter,
    COUNT(DISTINCT o.customer_id) AS total_active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders_processed,
    SUM(o.subtotal_amount + o.shipping_amount) AS gross_merchandise_value,
    AVG(o.subtotal_amount + o.shipping_amount) AS average_order_value,
    ROUND(
        SUM(CASE WHEN o.is_refunded = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT o.order_id), 
        2
    ) AS refund_rate
FROM 
    PROD.orders_v3_FINAL o
WHERE 
    o.order_date >= DATEADD('year', -2, CURRENT_DATE())
GROUP BY 
    1
ORDER BY 
    1 DESC;


-- ---------------------------------------------------------------------------------
-- QUERY 4: Churn Prediction Features Generation
-- Populates PROD.churn_prediction_features for downstream ML pipelines.
-- ---------------------------------------------------------------------------------

CREATE OR REPLACE TABLE PROD.churn_prediction_features AS
WITH customer_order_stats AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) AS lifetime_orders,
        SUM(subtotal_amount) AS lifetime_spend,
        MAX(order_date) AS last_order_timestamp,
        MIN(order_date) AS first_order_timestamp,
        SUM(CASE WHEN is_refunded = TRUE THEN 1 ELSE 0 END) AS total_refunded_orders
    FROM 
        PROD.orders_v3_FINAL
    GROUP BY 
        customer_id
)
SELECT 
    c.customer_id,
    c.customer_segment,
    c.signup_date,
    COALESCE(s.lifetime_orders, 0) AS lifetime_orders,
    COALESCE(s.lifetime_spend, 0.00) AS lifetime_spend,
    DATEDIFF('day', s.last_order_timestamp, CURRENT_TIMESTAMP()) AS days_since_last_purchase,
    DATEDIFF('day', s.first_order_timestamp, s.last_order_timestamp) AS customer_lifetime_days,
    CASE 
        WHEN s.lifetime_orders > 0 
        THEN ROUND(s.total_refunded_orders * 1.0 / s.lifetime_orders, 4)
        ELSE 0.0000
    END AS refund_ratio,
    -- Churn label definition (e.g., no order in last 90 days)
    CASE 
        WHEN DATEDIFF('day', s.last_order_timestamp, CURRENT_TIMESTAMP()) > 90 THEN 1 
        ELSE 0 
    END AS is_churned
FROM 
    PROD.dim_customers c
LEFT JOIN 
    customer_order_stats s ON c.customer_id = s.customer_id;


-- ---------------------------------------------------------------------------------
-- QUERY 5: Schema Evolution & Deprecation Audit
-- Run this to verify data consistency across legacy schemas (v1, v2) and v3_FINAL.
-- Useful for historical backfills or auditing legacy Looker dashboards.
-- ---------------------------------------------------------------------------------

SELECT 
    'orders_v1' AS source_version,
    COUNT(*) AS total_records,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date,
    SUM(subtotal_amount) AS total_subtotal
FROM PROD.orders_v1

UNION ALL

SELECT 
    'orders_v2' AS source_version,
    COUNT(*) AS total_records,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date,
    SUM(subtotal_amount) AS total_subtotal
FROM PROD.orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL' AS source_version,
    COUNT(*) AS total_records,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date,
    SUM(subtotal_amount) AS total_subtotal
FROM PROD.orders_v3_FINAL;


-- =================================================================================
-- END OF HANDOVER SCRIPT
-- For questions or emergency escalations, refer to the Data-Ops runbook or contact
-- the platform engineering team regarding the Stripe Webhook API infrastructure.
-- =================================================================================