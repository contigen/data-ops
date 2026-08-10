-- ==============================================================================
-- DATA ASSET HANDOVER REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Target Successor: Data Engineering Team / Successor
-- Date: October 2023
-- ==============================================================================
-- This script serves as an operational runbook and SQL reference for the data
-- assets previously maintained by alice.chen. It details the relationships,
-- known edge cases, recovery procedures, and downstream dependencies for:
--   1. orders_v3_FINAL (Snowflake - PROD) - *CRITICAL*
--   2. dim_customers (Snowflake - PROD)
--   3. churn_prediction_features (Snowflake - PROD)
--   4. quarterly_board_metrics (Snowflake - PROD)
--   5. daily_revenue_etl (Airflow Pipeline - Downstream)
--   6. revenue_dashboard_BACKUP (Looker - Downstream)
--   7. Legacy/Raw Assets: orders_v1, orders_v2, kafka_events_raw_copy
-- ==============================================================================


-- ==============================================================================
-- SECTION 1: CRITICAL ASSET OVERVIEW & SLA MONITORING
-- Asset: orders_v3_FINAL (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD))
-- ==============================================================================
-- DESCRIPTION:
-- Production orders dataset updated daily.
-- STRICT SLA: Must be populated and validated by 6:00 AM UTC daily.
-- This table has passed dbt data quality assertions (not_null & accepted_values on order_status).

-- SLA Verification Query:
-- Run this query to verify if today's data has loaded successfully before the 6:00 AM UTC SLA.
SELECT 
    MAX(created_at) AS last_inserted_timestamp,
    COUNT(CASE WHEN CAST(created_at AS DATE) = CURRENT_DATE() THEN 1 END) AS today_record_count,
    CASE 
        WHEN MAX(created_at) >= DATEADD(hour, 6, CAST(CURRENT_DATE() AS TIMESTAMP)) 
        THEN 'SLA MET' 
        ELSE 'SLA BREACH / RUNNING LATE' 
    END AS sla_status
FROM orders_v3_FINAL;


-- dbt Quality Assertion Simulation:
-- Run this to manually verify the constraints enforced by dbt on 'order_status'.
SELECT 
    order_status,
    COUNT(*) AS record_count,
    SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) AS null_failures,
    -- Expected statuses: 'PLACED', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'REFUNDED'
    SUM(CASE WHEN order_status NOT IN ('PLACED', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'REFUNDED') THEN 1 ELSE 0 END) AS invalid_status_failures
FROM orders_v3_FINAL
GROUP BY order_status;


-- ==============================================================================
-- SECTION 2: KNOWN EDGE CASE DETECTION & OPERATIONAL RECOVERY
-- ==============================================================================
-- EDGE CASE DESCRIPTION:
-- Canceled orders with a $0 subtotal and a positive shipping cost will break the 
-- downstream 'daily_revenue_etl' pipeline (causing negative margin calculations) 
-- if the 'is_refunded' flag is not set to 'true' within a 24-hour window.
--
-- This flag is populated via a Stripe webhook ingestion API. If the webhook fails,
-- follow the recovery steps below.

-- Detection Query:
-- Run this query daily to identify any anomalous records breaking the downstream pipeline.
SELECT 
    order_id,
    customer_id,
    order_date,
    subtotal,
    shipping_cost,
    is_refunded,
    order_status,
    created_at
FROM orders_v3_FINAL
WHERE subtotal = 0 
  AND shipping_cost > 0 
  AND is_refunded = FALSE
  AND order_status = 'CANCELLED';


-- ==============================================================================
-- OPERATIONAL RECOVERY RUNBOOK (IF ANOMALIES ARE FOUND ABOVE)
-- ==============================================================================
-- If the detection query returns rows, the Stripe webhook failed to sync. 
-- Follow these steps immediately to prevent Looker dashboard corruption:
--
-- STEP 1: Run the Reconciliation Catchup Script
--   Open your terminal and navigate to the 'data-ops' repository.
--   Execute the script using one of the following methods:
--
--   A. Single/Bulk Execution (Pass target IDs manually):
--      python stripe_reconciliation_catchup.py --order-ids "ORD-12345,ORD-67890"
--
--   B. Large Batch Execution (To prevent Stripe API rate-limiting, use --batch-size 100):
--      python stripe_reconciliation_catchup.py --file-path /path/to/failed_orders.csv --batch-size 100
--
-- STEP 2: Trigger Downstream Airflow Backfill
--   Once the script completes and 'is_refunded' updates to TRUE in Snowflake,
--   manually trigger an Airflow backfill on the 'daily_revenue_etl' DAG for the 
--   affected execution date. This recalculates downstream metrics for Looker.
--   Command:
--      airflow dags backfill -s <YYYY-MM-DD> -e <YYYY-MM-DD> daily_revenue_etl
-- ==============================================================================


-- ==============================================================================
-- SECTION 3: DOWNSTREAM PIPELINE SIMULATION (daily_revenue_etl)
-- ==============================================================================
-- This query simulates the logic inside the 'daily_revenue_etl' pipeline.
-- It demonstrates how the zero-dollar subtotal edge case impacts margin calculations.
WITH daily_metrics AS (
    SELECT 
        CAST(order_date AS DATE) AS reporting_date,
        -- Revenue is 0 for refunded orders to prevent double counting
        SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE subtotal END) AS net_subtotal_revenue,
        SUM(shipping_cost) AS total_shipping_revenue,
        -- Cost of Goods Sold (COGS) simulation
        SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE (subtotal * 0.4) END) AS estimated_cogs,
        -- If is_refunded is FALSE on a $0 subtotal with positive shipping, margin calculations can skew negative
        SUM(
            CASE 
                WHEN is_refunded = TRUE THEN 0 
                ELSE (subtotal + shipping_cost) - (subtotal * 0.4) 
            END
        ) AS gross_margin
    FROM orders_v3_FINAL
    GROUP BY 1
)
SELECT 
    reporting_date,
    net_subtotal_revenue,
    total_shipping_revenue,
    estimated_cogs,
    gross_margin,
    -- Warning flag for potential pipeline breaks
    CASE WHEN gross_margin < 0 THEN 'WARNING: Negative Margin Detected (Check Stripe Webhook)' ELSE 'OK' END AS health_status
FROM daily_metrics
ORDER BY reporting_date DESC;


-- ==============================================================================
-- SECTION 4: CUSTOMER ANALYTICS & FEATURE ENGINEERING
-- Assets: dim_customers & churn_prediction_features
-- ==============================================================================
-- This query demonstrates how 'orders_v3_FINAL' joins with 'dim_customers' 
-- to generate upstream features for the 'churn_prediction_features' table.
CREATE OR REPLACE TABLE churn_prediction_features AS
WITH customer_order_history AS (
    SELECT 
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS total_orders_placed,
        COUNT(DISTINCT CASE WHEN o.is_refunded = TRUE THEN o.order_id END) AS total_refunded_orders,
        SUM(o.subtotal) AS lifetime_spend,
        MAX(o.order_date) AS last_purchase_date,
        MIN(o.order_date) AS first_purchase_date,
        DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) AS days_since_last_purchase
    FROM orders_v3_FINAL o
    WHERE o.order_status IN ('DELIVERED', 'SHIPPED', 'PLACED')
    GROUP BY o.customer_id
)
SELECT 
    c.customer_id,
    c.email,
    c.country,
    c.signup_date,
    COALESCE(h.total_orders_placed, 0) AS total_orders,
    COALESCE(h.total_refunded_orders, 0) AS total_refunds,
    COALESCE(h.lifetime_spend, 0.0) AS lifetime_value,
    h.first_purchase_date,
    h.last_purchase_date,
    COALESCE(h.days_since_last_purchase, 9999) AS recency_days,
    -- Churn indicator: No purchase in last 90 days
    CASE WHEN COALESCE(h.days_since_last_purchase, 9999) > 90 THEN TRUE ELSE FALSE END AS is_churned_candidate
FROM dim_customers c
LEFT JOIN customer_order_history h ON c.customer_id = h.customer_id;


-- ==============================================================================
-- SECTION 5: EXECUTIVE REPORTING
-- Asset: quarterly_board_metrics
-- ==============================================================================
-- This query aggregates data from 'orders_v3_FINAL' to populate the 
-- 'quarterly_board_metrics' table, which feeds the executive dashboards.
CREATE OR REPLACE TABLE quarterly_board_metrics AS
SELECT 
    DATE_TRUNC('QUARTER', o.order_date) AS fiscal_quarter,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders_processed,
    SUM(CASE WHEN o.is_refunded = FALSE THEN o.subtotal ELSE 0 END) AS net_sales,
    SUM(CASE WHEN o.is_refunded = TRUE THEN o.subtotal ELSE 0 END) AS total_refunded_amount,
    ROUND(
        (SUM(CASE WHEN o.is_refunded = TRUE THEN o.subtotal ELSE 0 END) / 
         NULLIF(SUM(o.subtotal), 0)) * 100, 2
    ) AS refund_rate_percentage
FROM orders_v3_FINAL o
WHERE o.order_status != 'CANCELLED'
GROUP BY 1
ORDER BY fiscal_quarter DESC;


-- ==============================================================================
-- SECTION 6: LEGACY & RAW DATA AUDIT TRAIL
-- Assets: orders_v1, orders_v2, kafka_events_raw_copy
-- ==============================================================================
-- Note: orders_v1 and orders_v2 are deprecated legacy tables. 
-- Do not use them for new downstream pipelines. 
-- Below is a comparison query to audit differences between v2 and v3 if needed.

SELECT 
    'orders_v2' AS source_version,
    COUNT(*) AS total_records,
    SUM(subtotal) AS total_subtotal,
    COUNT(DISTINCT order_id) AS unique_orders
FROM orders_v2
UNION ALL
SELECT 
    'orders_v3_FINAL' AS source_version,
    COUNT(*) AS total_records,
    SUM(subtotal) AS total_subtotal,
    COUNT(DISTINCT order_id) AS unique_orders
FROM orders_v3_FINAL;

-- Kafka Raw Events Audit:
-- 'kafka_events_raw_copy' contains raw payloads before ingestion into Snowflake.
-- Use this to debug webhook payload mismatches or missing orders.
SELECT 
    payload:order_id::VARCHAR AS extracted_order_id,
    payload:stripe_status::VARCHAR AS stripe_status,
    payload:event_timestamp::TIMESTAMP AS event_time,
    payload
FROM kafka_events_raw_copy
WHERE payload:order_id::VARCHAR IS NOT NULL
ORDER BY event_time DESC
LIMIT 100;