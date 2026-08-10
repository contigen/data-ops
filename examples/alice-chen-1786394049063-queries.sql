-- =================================================================================
-- DATA OPS HANDOVER REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Focus Area: Core Orders, Revenue Pipelines, and Customer Features
-- =================================================================================

-- =================================================================================
-- SECTION 1: OPERATIONAL DOCUMENTATION & SLA ALERTS
-- =================================================================================
/*
   ASSET: orders_v3_FINAL (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD))
   
   SLA: 
   * Must be fully populated and updated daily by 6:00 AM UTC.
   * Directly feeds the Executive Revenue Dashboard (revenue_dashboard_BACKUP).
   
   DATA QUALITY ASSERTIONS (dbt):
   * `order_status` must pass 'not_null' and 'accepted_values' tests.
   
   KNOWN EDGE CASE & DOWNSTREAM IMPACT:
   * Canceled orders with a $0.00 subtotal but a positive shipping cost (> $0.00) 
     will break the downstream `daily_revenue_etl` pipeline (causing negative margin calculations)
     IF the `is_refunded` flag is not set to TRUE within a 24-hour window.
     
   OPERATIONAL RECOVERY PROCEDURE (If Stripe Webhook fails to sync `is_refunded`):
   1. Run the reconciliation script from the `data-ops` repository:
      python stripe_reconciliation_catchup.py --order-ids <comma_separated_ids>
      OR
      python stripe_reconciliation_catchup.py --file-path <csv_path>
   2. For batches > 500 records, append the rate-limiting flag:
      python stripe_reconciliation_catchup.py --file-path <csv_path> --batch-size 100
   3. Trigger an Airflow backfill on the `daily_revenue_etl` DAG for the affected execution date.
*/

-- =================================================================================
-- QUERY 1: SLA & EDGE CASE MONITORING (Run Daily)
-- Identifies zero-dollar subtotal orders with positive shipping costs that lack
-- the 'is_refunded' flag, which will break the downstream daily_revenue_etl.
-- =================================================================================

-- Use this query to generate the list of Order IDs to pass to stripe_reconciliation_catchup.py
SELECT 
    order_id,
    order_date,
    order_status,
    subtotal,
    shipping_cost,
    is_refunded,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM snowflake.PROD.orders_v3_FINAL
WHERE 
    subtotal = 0.00 
    AND shipping_cost > 0.00 
    AND (is_refunded IS NULL OR is_refunded = FALSE)
    AND order_date >= CURRENT_DATE() - INTERVAL '2 days'
ORDER BY order_date DESC;


-- =================================================================================
-- QUERY 2: DOWNSTREAM SIMULATION (daily_revenue_etl)
-- Demonstrates how the daily_revenue_etl aggregates revenue and handles the edge case.
-- This query can be used to validate data before/after running the recovery script.
-- =================================================================================

WITH daily_raw_metrics AS (
    SELECT 
        CAST(order_date AS DATE) AS revenue_date,
        order_id,
        subtotal,
        shipping_cost,
        is_refunded,
        -- Defensive logic to prevent negative margin calculations during edge cases
        CASE 
            WHEN subtotal = 0.00 AND shipping_cost > 0.00 AND (is_refunded IS NULL OR is_refunded = FALSE)
            THEN 0.00 -- Force zero margin/revenue to prevent pipeline failure
            ELSE (subtotal + shipping_cost) 
        END AS gross_revenue,
        CASE 
            WHEN is_refunded = TRUE THEN 0.00
            ELSE subtotal 
        END AS net_subtotal
    FROM snowflake.PROD.orders_v3_FINAL
    WHERE order_status IN ('completed', 'shipped', 'processing') -- dbt accepted_values
)
SELECT 
    revenue_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(gross_revenue) AS total_gross_revenue,
    SUM(net_subtotal) AS total_net_subtotal,
    -- If this value is negative, the edge case has bypassed our filters
    SUM(gross_revenue) - SUM(shipping_cost) AS estimated_margin
FROM daily_raw_metrics
GROUP BY 1
ORDER BY 1 DESC;


-- =================================================================================
-- QUERY 3: CUSTOMER 360 & CHURN FEATURE VALIDATION
-- Joins dim_customers, orders_v3_FINAL, and churn_prediction_features to verify
-- feature drift or pipeline consistency.
-- =================================================================================

SELECT 
    c.customer_id,
    c.customer_segment,
    c.signup_date,
    f.predicted_churn_risk,
    f.last_active_date,
    COUNT(o.order_id) AS total_lifetime_orders,
    SUM(COALESCE(o.subtotal, 0.00)) AS lifetime_spend,
    MAX(o.order_date) AS actual_last_order_date
FROM snowflake.PROD.dim_customers c
LEFT JOIN snowflake.PROD.churn_prediction_features f 
    ON c.customer_id = f.customer_id
LEFT JOIN snowflake.PROD.orders_v3_FINAL o 
    ON c.customer_id = o.customer_id
GROUP BY 1, 2, 3, 4, 5
ORDER BY lifetime_spend DESC
LIMIT 100;


-- =================================================================================
-- QUERY 4: QUARTERLY BOARD METRICS RECONCILIATION
-- Validates quarterly_board_metrics against the source-of-truth orders_v3_FINAL.
-- Run this before board meetings to ensure no discrepancies exist.
-- =================================================================================

WITH quarterly_calc AS (
    SELECT 
        DATE_TRUNC('QUARTER', order_date) AS order_quarter,
        COUNT(DISTINCT customer_id) AS active_customers,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(subtotal + shipping_cost) AS total_revenue,
        SUM(CASE WHEN is_refunded = TRUE THEN (subtotal + shipping_cost) ELSE 0 END) AS total_refunds
    FROM snowflake.PROD.orders_v3_FINAL
    WHERE order_status = 'completed'
    GROUP BY 1
)
SELECT 
    q.order_quarter,
    q.active_customers AS source_active_customers,
    b.active_customers AS board_active_customers,
    q.total_revenue AS source_total_revenue,
    b.quarterly_revenue AS board_total_revenue,
    (q.total_revenue - b.quarterly_revenue) AS revenue_discrepancy
FROM quarterly_calc q
INNER JOIN snowflake.PROD.quarterly_board_metrics b 
    ON q.order_quarter = b.reporting_quarter
ORDER BY q.order_quarter DESC;


-- =================================================================================
-- QUERY 5: DEPRECATED ASSETS AUDIT (orders_v1 vs orders_v2 vs orders_v3_FINAL)
-- Ensures downstream users are not querying legacy tables. Run this against 
-- Snowflake's ACCESS_HISTORY to find rogue queries.
-- =================================================================================

SELECT 
    'orders_v1' AS table_version,
    COUNT(*) AS record_count,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
FROM snowflake.PROD.orders_v1

UNION ALL

SELECT 
    'orders_v2' AS table_version,
    COUNT(*) AS record_count,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
FROM snowflake.PROD.orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL' AS table_version,
    COUNT(*) AS record_count,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
FROM snowflake.PROD.orders_v3_FINAL;