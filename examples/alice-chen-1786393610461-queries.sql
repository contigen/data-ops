-- =================================================================================
-- DATA OPS HANDOVER REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- 
-- This script serves as a technical reference and operational runbook for the 
-- data assets previously maintained by alice.chen. It contains diagnostic 
-- queries, ETL simulations, and recovery procedures for the downstream pipelines.
-- =================================================================================

-- =================================================================================
-- SECTION 1: OPERATIONAL DOCUMENTATION & RUNBOOKS (COPIED FOR CONVENIENCE)
-- =================================================================================
--
-- ASSET: orders_v3_FINAL (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD))
-- ---------------------------------------------------------------------------------
-- * SLA: Daily update completed by 6:00 AM UTC. Populates the executive dashboard.
-- * Quality: Passed dbt assertions (not_null and accepted_values on order_status).
-- * Known Edge Case: 
--   Canceled orders with a $0.00 subtotal but a positive shipping cost will break 
--   the downstream `daily_revenue_etl` pipeline (causing negative margin calculations) 
--   if the `is_refunded` flag is not set to TRUE within a 24-hour window.
--
-- * Recovery Procedure (Stripe Webhook Failure):
--   If the Stripe webhook fails to sync `is_refunded` within 24 hours, execute:
--
--   1. Run the reconciliation script from the `data-ops` repository:
--      python stripe_reconciliation_catchup.py --order-ids <COMMA_SEPARATED_IDS>
--      
--      *Note: For batches > 500 records, append the rate-limiting flag:
--      python stripe_reconciliation_catchup.py --file-path <PATH_TO_CSV> --batch-size 100
--
--   2. Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG 
--      for the affected execution date to recalculate the downstream Looker dashboard 
--      (revenue_dashboard_BACKUP).
-- =================================================================================


-- =================================================================================
-- SECTION 2: DIAGNOSTIC & MONITORING QUERIES
-- =================================================================================

-- Query 2.1: Edge Case Detection Query (Run this to find breaking records)
-- This query identifies orders that will break the downstream daily_revenue_etl pipeline.
-- If any rows are returned by this query, execute the Stripe Reconciliation Catchup script.
SELECT 
    order_id,
    customer_id,
    order_date,
    order_status,
    subtotal_amount,
    shipping_amount,
    is_refunded,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_order
FROM PROD.snowflake.orders_v3_FINAL
WHERE 
    subtotal_amount = 0.00 
    AND shipping_amount > 0.00 
    AND is_refunded = FALSE
    AND order_status = 'cancelled'
ORDER BY order_date DESC;


-- Query 2.2: SLA Verification Query
-- Verifies if the daily batch for orders_v3_FINAL was loaded before the 6:00 AM UTC SLA.
SELECT 
    CAST(order_date AS DATE) AS run_date,
    MIN(created_at) AS first_record_timestamp,
    MAX(created_at) AS last_record_timestamp,
    COUNT(*) AS total_records_loaded,
    CASE 
        -- Check if the maximum ingestion timestamp is before 06:00:00 UTC of the next day
        WHEN MAX(created_at) <= DATEADD('hour', 6, CAST(CAST(order_date AS DATE) AS TIMESTAMP_NTZ)) THEN 'SLA Met'
        ELSE 'SLA Breached'
    END AS sla_status
FROM PROD.snowflake.orders_v3_FINAL
GROUP BY CAST(order_date AS DATE)
ORDER BY run_date DESC
LIMIT 14;


-- =================================================================================
-- SECTION 3: DOWNSTREAM PIPELINE SIMULATIONS & INTEGRATIONS
-- =================================================================================

-- Query 3.1: Simulated daily_revenue_etl Pipeline
-- This query represents the core logic of the daily_revenue_etl Airflow DAG.
-- It joins orders_v3_FINAL with dim_customers to generate daily revenue metrics.
-- Note how the edge-case protection (CASE statement) prevents negative margin calculations.
WITH daily_revenue_calculation AS (
    SELECT 
        CAST(o.order_date AS DATE) AS revenue_date,
        c.customer_segment,
        c.country,
        COUNT(DISTINCT o.order_id) AS total_orders,
        -- Edge-case protection logic: If subtotal is 0 and shipping is positive but not refunded, 
        -- we treat shipping cost as 0 to prevent downstream margin distortion.
        SUM(
            CASE 
                WHEN o.subtotal_amount = 0 AND o.shipping_amount > 0 AND o.is_refunded = FALSE 
                THEN 0 
                ELSE o.subtotal_amount 
            END
        ) AS gross_subtotal_revenue,
        SUM(o.shipping_amount) AS gross_shipping_revenue,
        SUM(
            CASE 
                WHEN o.is_refunded = TRUE THEN (o.subtotal_amount + o.shipping_amount)
                ELSE 0 
            END
        ) AS total_refunded_amount
    FROM PROD.snowflake.orders_v3_FINAL o
    INNER JOIN PROD.snowflake.dim_customers c 
        ON o.customer_id = c.customer_id
    WHERE o.order_status IN ('completed', 'shipped', 'cancelled')
    GROUP BY 1, 2, 3
)
SELECT 
    revenue_date,
    customer_segment,
    country,
    total_orders,
    gross_subtotal_revenue,
    gross_shipping_revenue,
    total_refunded_amount,
    -- Net Revenue Calculation
    (gross_subtotal_revenue + gross_shipping_revenue) - total_refunded_amount AS net_revenue
FROM daily_revenue_calculation
ORDER BY revenue_date DESC, net_revenue DESC;


-- Query 3.2: Simulated quarterly_board_metrics Generation
-- This query aggregates data from orders_v3_FINAL and dim_customers to populate 
-- the quarterly_board_metrics table.
CREATE OR REPLACE TABLE PROD.snowflake.quarterly_board_metrics_SIMULATED AS
SELECT 
    DATE_TRUNC('quarter', o.order_date) AS fiscal_quarter,
    c.customer_segment,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.subtotal_amount) AS total_gross_sales,
    ROUND(SUM(o.subtotal_amount) / COUNT(DISTINCT o.order_id), 2) AS average_order_value (AOV),
    ROUND(COUNT(DISTINCT o.order_id) / COUNT(DISTINCT o.customer_id), 2) AS purchase_frequency
FROM PROD.snowflake.orders_v3_FINAL o
INNER JOIN PROD.snowflake.dim_customers c 
    ON o.customer_id = c.customer_id
WHERE o.order_status = 'completed'
GROUP BY 1, 2
ORDER BY fiscal_quarter DESC, customer_segment;


-- Query 3.3: Simulated churn_prediction_features Generation
-- Generates behavioral features for the churn prediction model using orders_v3_FINAL.
CREATE OR REPLACE TABLE PROD.snowflake.churn_prediction_features_SIMULATED AS
WITH customer_order_history AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS total_lifetime_orders,
        SUM(subtotal_amount) AS total_lifetime_spend,
        MAX(order_date) AS most_recent_order_date,
        MIN(order_date) AS first_order_date,
        COUNT(CASE WHEN is_refunded = TRUE THEN 1 END) AS total_refunded_orders
    FROM PROD.snowflake.orders_v3_FINAL
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.customer_segment,
    c.signup_date,
    COALESCE(h.total_lifetime_orders, 0) AS total_lifetime_orders,
    COALESCE(h.total_lifetime_spend, 0.00) AS total_lifetime_spend,
    COALESCE(h.total_refunded_orders, 0) AS total_refunded_orders,
    DATEDIFF('day', h.most_recent_order_date, CURRENT_DATE()) AS days_since_last_purchase,
    DATEDIFF('day', c.signup_date, CURRENT_DATE()) AS customer_tenure_days,
    CASE 
        WHEN DATEDIFF('day', h.most_recent_order_date, CURRENT_DATE()) > 90 THEN TRUE 
        ELSE FALSE 
    END AS is_churned_90d
FROM PROD.snowflake.dim_customers c
LEFT JOIN customer_order_history h 
    ON c.customer_id = h.customer_id;


-- =================================================================================
-- SECTION 4: DEPRECATION & LINEAGE AUDITS
-- =================================================================================

-- Query 4.1: Lineage Reconciliation (orders_v1 vs orders_v2 vs orders_v3_FINAL)
-- Run this query to verify data consistency across legacy versions if backfilling historical data.
SELECT 
    'orders_v1' AS source_version,
    COUNT(*) AS total_records,
    COUNT(DISTINCT order_id) AS unique_orders,
    SUM(subtotal_amount) AS total_subtotal
FROM PROD.snowflake.orders_v1
UNION ALL
SELECT 
    'orders_v2' AS source_version,
    COUNT(*) AS total_records,
    COUNT(DISTINCT order_id) AS unique_orders,
    SUM(subtotal_amount) AS total_subtotal
FROM PROD.snowflake.orders_v2
UNION ALL
SELECT 
    'orders_v3_FINAL' AS source_version,
    COUNT(*) AS total_records,
    COUNT(DISTINCT order_id) AS unique_orders,
    SUM(subtotal_amount) AS total_subtotal
FROM PROD.snowflake.orders_v3_FINAL;