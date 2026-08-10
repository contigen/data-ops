-- ==============================================================================
-- HANDOFF REFERENCE SQL SCRIPT
-- Departing Engineer: alice.chen
-- Target Audience: Successor Data Engineer / Analytics Engineer
-- Date: October 2023
--
-- This script serves as an operational runbook and technical reference for the
-- data assets previously maintained by alice.chen. It contains production-grade
-- queries, edge-case monitoring scripts, and downstream aggregation examples.
-- ==============================================================================

-- ==============================================================================
-- SECTION 1: ASSET OVERVIEW & LINEAGE
-- ==============================================================================
/*
  The primary data flow and asset relationships are as follows:
  
  [Kafka] kafka_events_raw_copy (Ingestion)
     │
     ▼
  [Snowflake] orders_v1 -> orders_v2 (Deprecated Legacy Tables)
     │
     ▼
  [Snowflake] orders_v3_FINAL (Core Production Table - SLA: 06:00 UTC)
     │
     ├─► [Airflow] daily_revenue_etl ──► [Snowflake] quarterly_board_metrics
     │                                 └──► [Looker] revenue_dashboard_BACKUP
     │
     └─► [Snowflake] dim_customers ────► [Snowflake] churn_prediction_features
*/


-- ==============================================================================
-- SECTION 2: OPERATIONAL MONITORING & SLA COMPLIANCE (orders_v3_FINAL)
-- ==============================================================================
-- URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)
--
-- CRITICAL SLA: This table must be fully updated daily by 6:00 AM UTC.
-- DBT Assertions: The table has passed strict `not_null` and `accepted_values` 
-- tests on the `order_status` column before being promoted to `FINAL`.

-- MONITORING QUERY: Verify SLA and check for recent data loads
SELECT 
    MAX(created_at) AS last_order_timestamp,
    COUNT(CASE WHEN DATE(created_at) = CURRENT_DATE() THEN 1 END) AS today_order_count,
    COUNT(CASE WHEN DATE(created_at) = CURRENT_DATE() - 1 THEN 1 END) AS yesterday_order_count
FROM snowflake.PROD.orders_v3_FINAL;


-- ==============================================================================
-- SECTION 3: EDGE CASE DETECTION & RECOVERY (The "Zero-Dollar Subtotal" Bug)
-- ==============================================================================
-- KNOWN ISSUE: Canceled orders with a $0.00 subtotal but a positive shipping cost
-- will break the downstream `daily_revenue_etl` pipeline (causing negative margin
-- calculations) IF the `is_refunded` flag is not set to TRUE within 24 hours.
--
-- RECOVERY PROCEDURE:
-- If the query below returns any records, the Stripe webhook ingestion API failed.
-- Execute the following steps immediately:
--
--   1. Run the reconciliation script from the `data-ops` repository:
--      python stripe_reconciliation_catchup.py --order-ids <comma_separated_ids>
--
--   2. For batches > 500 records, use the batch flag to prevent rate-limiting:
--      python stripe_reconciliation_catchup.py --file-path path/to/ids.csv --batch-size 100
--
--   3. Trigger an Airflow backfill on the `daily_revenue_etl` DAG for the affected dates:
--      airflow dags backfill -s YYYY-MM-DD -e YYYY-MM-DD daily_revenue_etl

-- ALERT QUERY: Run this daily to detect unflagged zero-dollar shipping anomalies
SELECT 
    order_id,
    customer_id,
    created_at,
    subtotal,
    shipping_cost,
    order_status,
    is_refunded,
    DATEDIFF('hour', created_at, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM snowflake.PROD.orders_v3_FINAL
WHERE 
    subtotal = 0.00 
    AND shipping_cost > 0.00 
    AND order_status = 'CANCELED' 
    AND is_refunded = FALSE
ORDER BY created_at DESC;


-- ==============================================================================
-- SECTION 4: DOWNSTREAM PIPELINE SIMULATION (daily_revenue_etl)
-- ==============================================================================
-- URN: urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD)
--
-- This query demonstrates how the daily revenue ETL aggregates data from 
-- `orders_v3_FINAL` while safely handling the zero-dollar subtotal edge case
-- to prevent negative margin calculations.

WITH processed_orders AS (
    SELECT 
        order_id,
        customer_id,
        DATE(created_at) AS order_date,
        subtotal,
        shipping_cost,
        -- Safe margin calculation logic: If canceled and not refunded, force subtotal to 0
        -- to prevent downstream pipeline failures.
        CASE 
            WHEN order_status = 'CANCELED' AND is_refunded = FALSE THEN 0.00
            ELSE subtotal 
        END AS net_subtotal,
        shipping_cost AS net_shipping
    FROM snowflake.PROD.orders_v3_FINAL
)
SELECT 
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(net_subtotal) AS gross_revenue,
    SUM(net_shipping) AS shipping_revenue,
    SUM(net_subtotal + net_shipping) AS total_revenue
FROM processed_orders
GROUP BY 1
ORDER BY 1 DESC;


-- ==============================================================================
-- SECTION 5: BOARD METRICS GENERATION (quarterly_board_metrics)
-- ==============================================================================
-- URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD)
--
-- This query aggregates order data up to the quarterly level for executive reporting.
-- It joins `orders_v3_FINAL` with `dim_customers` to segment metrics by customer cohort.

SELECT 
    DATE_TRUNC('quarter', o.created_at) AS board_quarter,
    c.customer_segment,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.subtotal) AS total_quarterly_revenue,
    ROUND(SUM(o.subtotal) / COUNT(DISTINCT o.customer_id), 2) AS ARPU (Average Revenue Per User)
FROM snowflake.PROD.orders_v3_FINAL o
INNER JOIN snowflake.PROD.dim_customers c 
    ON o.customer_id = c.customer_id
WHERE 
    o.order_status NOT IN ('CANCELED', 'FAILED') -- Exclude failed transactions
GROUP BY 1, 2
ORDER BY 1 DESC, 2 ASC;


-- ==============================================================================
-- SECTION 6: MACHINE LEARNING FEATURE STORE (churn_prediction_features)
-- ==============================================================================
-- URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD)
--
-- This query generates the feature set used by the data science team's churn model.
-- It combines customer demographic data with historical order behavior.

CREATE OR REPLACE TABLE snowflake.PROD.churn_prediction_features AS
WITH customer_order_stats AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS total_lifetime_orders,
        SUM(subtotal) AS lifetime_spend,
        MAX(created_at) AS last_purchase_date,
        MIN(created_at) AS first_purchase_date,
        COUNT(CASE WHEN order_status = 'CANCELED' THEN 1 END) AS total_canceled_orders,
        COUNT(CASE WHEN is_refunded = TRUE THEN 1 END) AS total_refunded_orders
    FROM snowflake.PROD.orders_v3_FINAL
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.signup_date,
    c.country,
    c.acquisition_channel,
    COALESCE(s.total_lifetime_orders, 0) AS total_lifetime_orders,
    COALESCE(s.lifetime_spend, 0.00) AS lifetime_spend,
    DATEDIFF('day', s.last_purchase_date, CURRENT_TIMESTAMP()) AS days_since_last_purchase,
    DATEDIFF('day', s.first_purchase_date, s.last_purchase_date) AS customer_lifetime_days,
    COALESCE(s.total_canceled_orders, 0) AS total_canceled_orders,
    COALESCE(s.total_refunded_orders, 0) AS total_refunded_orders,
    -- Calculate refund rate to identify high-risk/dissatisfied customers
    CASE 
        WHEN COALESCE(s.total_lifetime_orders, 0) = 0 THEN 0.00
        ELSE ROUND(COALESCE(s.total_refunded_orders, 0) / s.total_lifetime_orders, 4)
    END AS refund_rate
FROM snowflake.PROD.dim_customers c
LEFT JOIN customer_order_stats s 
    ON c.customer_id = s.customer_id;


-- ==============================================================================
-- SECTION 7: LEGACY & BACKUP ASSETS REFERENCE
-- ==============================================================================
-- The following assets are currently undocumented but remain in production:
--
-- 1. `orders_v1` & `orders_v2` (Snowflake):
--    Legacy schemas. Do not use for new development. Kept for historical audit.
--
-- 2. `revenue_dashboard_BACKUP` (Looker):
--    A backup of the main executive dashboard. Points directly to the output of 
--    the `daily_revenue_etl` pipeline.
--
-- 3. `kafka_events_raw_copy` (Kafka):
--    Raw event stream copy. Used for debugging real-time ingestion issues before
--    data is structured into the Snowflake staging layers.

-- Example query to inspect raw Kafka event payloads if ingestion lags:
-- SELECT 
--     partition,
--     offset,
--     timestamp,
--     payload:order_id::string AS order_id,
--     payload:event_type::string AS event_type
-- FROM kafka.PROD.kafka_events_raw_copy
-- LIMIT 100;