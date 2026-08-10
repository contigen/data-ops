-- ==============================================================================
-- DATA ENGINEERING HANDOVER REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Target Audience: Successor Data Engineer / Data Ops Team
-- 
-- This script serves as an operational runbook, schema reference, and query 
-- catalog for the data assets previously managed by alice.chen.
-- ==============================================================================

-- ==============================================================================
-- SECTION 1: ASSET INVENTORY & METADATA
-- ==============================================================================
-- The following metadata maps the documented assets to their respective platforms:
--
-- 1. Snowflake Datasets (PROD):
--    - urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)
--      * Status: ACTIVE (Production Core)
--      * SLA: Daily 6:00 AM UTC
--      * Quality: dbt verified (not_null, accepted_values on order_status)
--    - urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD)
--      * Status: ACTIVE (Production Dimension)
--    - urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD)
--      * Status: ACTIVE (Reporting Aggregation)
--    - urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD)
--      * Status: ACTIVE (ML Feature Store)
--    - urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD)
--      * Status: DEPRECATED (Do not use for new development)
--    - urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD)
--      * Status: DEPRECATED (Do not use for new development)
--
-- 2. Orchestration & Streaming (PROD):
--    - urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD)
--      * Status: ACTIVE (Daily ETL Pipeline)
--    - urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD)
--      * Status: ACTIVE (Raw Event Stream Backup)
--
-- 3. BI / Dashboards (PROD):
--    - urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD)
--      * Status: ACTIVE (Backup Executive Dashboard)
-- ==============================================================================


-- ==============================================================================
-- SECTION 2: OPERATIONAL RUNBOOK & DIAGNOSTIC QUERIES
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- CRITICAL EDGE CASE DETECTOR: Zero-Dollar Subtotal / Positive Shipping Cost
-- ------------------------------------------------------------------------------
-- Context: Canceled orders with a $0 subtotal and a positive shipping cost 
-- will break the downstream `daily_revenue_etl` pipeline (causing negative margin 
-- calculations) if the `is_refunded` flag is not set to `true` within 24 hours.
--
-- Run this query to identify records causing or about to cause ETL failures.
-- ------------------------------------------------------------------------------

SELECT 
    order_id,
    customer_id,
    order_date,
    order_status,
    subtotal,
    shipping_cost,
    is_refunded,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_order,
    -- Flag indicating if this record will break the daily_revenue_etl pipeline
    CASE 
        WHEN subtotal = 0 
             AND shipping_cost > 0 
             AND order_status = 'CANCELED' 
             AND is_refunded = FALSE 
             AND DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) >= 24 
        THEN 'CRITICAL: Pipeline Breaker - Requires Manual Stripe Catchup'
        WHEN subtotal = 0 
             AND shipping_cost > 0 
             AND order_status = 'CANCELED' 
             AND is_refunded = FALSE 
        THEN 'WARNING: Pending Stripe Webhook Sync (<24h window)'
        ELSE 'OK'
    END AS operational_status
FROM 
    orders_v3_FINAL
WHERE 
    subtotal = 0 
    AND shipping_cost > 0
ORDER BY 
    order_date DESC;


-- ------------------------------------------------------------------------------
-- RECOVERY PROCEDURE INSTRUCTIONS (If above query returns CRITICAL records):
-- ------------------------------------------------------------------------------
-- 1. Identify the offending order_ids from the query above.
-- 2. Locate the `data-ops` repository.
-- 3. Execute the Stripe reconciliation catchup script:
--
--    # For a single or small batch of orders:
--    python stripe_reconciliation_catchup.py --order-ids "ORD-12345,ORD-67890"
--
--    # For large batches (> 500 records) to prevent Stripe API rate-limiting:
--    python stripe_reconciliation_catchup.py --file-path "/path/to/critical_orders.csv" --batch-size 100
--
-- 4. Once the script completes and `is_refunded` updates to TRUE in Snowflake,
--    manually trigger an Airflow backfill on the `daily_revenue_etl` DAG 
--    for the affected execution dates to recalculate the downstream Looker dashboard.
-- ------------------------------------------------------------------------------


-- ------------------------------------------------------------------------------
-- SLA MONITORING QUERY: orders_v3_FINAL
-- ------------------------------------------------------------------------------
-- Context: This table must be fully populated daily by 6:00 AM UTC.
-- ------------------------------------------------------------------------------

SELECT 
    MAX(order_date) AS max_order_timestamp,
    CURRENT_TIMESTAMP() AS current_system_time,
    -- Check if the latest data is within the acceptable SLA window
    CASE 
        WHEN MAX(order_date) >= DATEADD('day', -1, CURRENT_DATE()) + TO_TIME('06:00:00') 
        THEN 'SLA MET'
        ELSE 'SLA BREACHED - Check Airflow DAG daily_revenue_etl'
    END AS sla_status
FROM 
    orders_v3_FINAL;


-- ==============================================================================
-- SECTION 3: INTEGRATION & DOWNSTREAM PIPELINE SIMULATIONS
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- PIPELINE: daily_revenue_etl (Simulation)
-- ------------------------------------------------------------------------------
-- This query demonstrates how the daily revenue and margin are calculated,
-- showing how the zero-dollar subtotal edge case is handled when is_refunded is true.
-- ------------------------------------------------------------------------------

WITH daily_metrics AS (
    SELECT 
        CAST(order_date AS DATE) AS reporting_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(subtotal) AS gross_subtotal,
        SUM(shipping_cost) AS gross_shipping,
        -- Margin calculation logic:
        -- If an order is refunded, we exclude its subtotal and shipping from revenue.
        -- If a canceled order has $0 subtotal and positive shipping, but is NOT marked
        -- as refunded, it incorrectly drags down net margin calculations.
        SUM(
            CASE 
                WHEN is_refunded = TRUE THEN 0
                ELSE (subtotal + shipping_cost)
            END
        ) AS net_revenue,
        SUM(
            CASE 
                WHEN is_refunded = TRUE THEN 0
                ELSE (shipping_cost * 0.85) -- Simulated cost of shipping
            END
        ) AS estimated_shipping_cost
    FROM 
        orders_v3_FINAL
    WHERE 
        order_status IN ('COMPLETED', 'CANCELED', 'SHIPPED') -- Filter based on dbt accepted_values
    GROUP BY 
        1
)
SELECT 
    reporting_date,
    total_orders,
    gross_subtotal,
    gross_shipping,
    net_revenue,
    estimated_shipping_cost,
    (net_revenue - estimated_shipping_cost) AS net_margin
FROM 
    daily_metrics
ORDER BY 
    reporting_date DESC;


-- ------------------------------------------------------------------------------
-- PIPELINE: quarterly_board_metrics (Simulation)
-- ------------------------------------------------------------------------------
-- Demonstrates how orders_v3_FINAL joins with dim_customers to generate 
-- high-level quarterly metrics for board reporting.
-- ------------------------------------------------------------------------------

SELECT 
    DATE_TRUNC('quarter', o.order_date) AS fiscal_quarter,
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.subtotal) AS total_quarterly_revenue,
    AVG(o.subtotal) AS average_order_value,
    -- Calculate refund rate to monitor operational health
    ROUND(
        COUNT(CASE WHEN o.is_refunded = TRUE THEN 1 END) * 100.0 / COUNT(o.order_id), 
        2
    ) AS refund_rate_percentage
FROM 
    orders_v3_FINAL o
INNER JOIN 
    dim_customers c ON o.customer_id = c.customer_id
WHERE 
    o.order_status = 'COMPLETED'
GROUP BY 
    1, 2
ORDER BY 
    fiscal_quarter DESC, 
    total_quarterly_revenue DESC;


-- ------------------------------------------------------------------------------
-- PIPELINE: churn_prediction_features (Simulation)
-- ------------------------------------------------------------------------------
-- Demonstrates how features are engineered from orders_v3_FINAL and dim_customers
-- to feed the downstream machine learning churn model.
-- ------------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE temp_churn_features_draft AS
WITH customer_order_history AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS lifetime_orders,
        SUM(subtotal) AS lifetime_spend,
        MAX(order_date) AS last_purchase_date,
        MIN(order_date) AS first_purchase_date,
        COUNT(CASE WHEN is_refunded = TRUE THEN 1 END) AS total_refunded_orders
    FROM 
        orders_v3_FINAL
    GROUP BY 
        customer_id
)
SELECT 
    c.customer_id,
    c.customer_segment,
    c.signup_date,
    COALESCE(h.lifetime_orders, 0) AS f_lifetime_orders,
    COALESCE(h.lifetime_spend, 0.0) AS f_lifetime_spend,
    COALESCE(h.total_refunded_orders, 0) AS f_total_refunds,
    DATEDIFF('day', c.signup_date, CURRENT_DATE()) AS f_customer_tenure_days,
    DATEDIFF('day', h.last_purchase_date, CURRENT_DATE()) AS f_days_since_last_purchase,
    -- Feature: Refund ratio to detect dissatisfied customers
    CASE 
        WHEN COALESCE(h.lifetime_orders, 0) > 0 
        THEN ROUND(COALESCE(h.total_refunded_orders, 0) * 1.0 / h.lifetime_orders, 4)
        ELSE 0.0 
    END AS f_refund_ratio
FROM 
    dim_customers c
LEFT JOIN 
    customer_order_history h ON c.customer_id = h.customer_id;

-- Preview the engineered features
SELECT * FROM temp_churn_features_draft LIMIT 100;


-- ==============================================================================
-- SECTION 4: DEPRECATED ASSETS AUDIT
-- ==============================================================================
-- Context: orders_v1 and orders_v2 are deprecated. The query below is a safety 
-- check to ensure no active processes or users are querying these tables.
-- ------------------------------------------------------------------------------

-- Run this in Snowflake Account Usage (requires appropriate privileges) to 
-- audit if anyone is still accessing the deprecated tables.
--
-- SELECT 
--     query_text,
--     user_name,
--     role_name,
--     execution_status,
--     start_time
-- FROM 
--     snowflake.account_usage.query_history
-- WHERE 
--     (query_text ILIKE '%orders_v1%' OR query_text ILIKE '%orders_v2%')
--     AND start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
-- ORDER BY 
--     start_time DESC;