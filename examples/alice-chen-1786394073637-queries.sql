-- ==============================================================================
-- HANDOVER REFERENCE SCRIPT: ALICE CHEN (DEPARTING ENGINEER)
-- TARGET ASSETS: orders_v3_FINAL, daily_revenue_etl, dim_customers, 
--                quarterly_board_metrics, churn_prediction_features,
--                revenue_dashboard_BACKUP, kafka_events_raw_copy, orders_v2, orders_v1
-- DATABASE PLATFORM: Snowflake / Standard SQL
-- ==============================================================================

-- ==============================================================================
-- SECTION 1: SYSTEM OVERVIEW & CRITICAL SLA METRICS
-- ==============================================================================
/*
   OPERATIONAL SUMMARY:
   - Primary Table: orders_v3_FINAL (Snowflake PROD)
   - SLA: Must be fully updated daily by 6:00 AM UTC.
   - Downstream Impact: Populates the daily executive dashboard (revenue_dashboard_BACKUP).
   - Data Quality: dbt assertions run daily to guarantee 'not_null' and 
     'accepted_values' on the `order_status` column.
   
   CRITICAL EDGE CASE (The "Zero-Dollar Subtotal" Bug):
   - Canceled orders with a $0 subtotal but a positive shipping cost (> $0) 
     will BREAK the downstream `daily_revenue_etl` pipeline (causing negative margin 
     calculations) IF the `is_refunded` flag is not set to TRUE within 24 hours.
   - This flag is populated via a Stripe webhook. If the webhook fails, manual 
     intervention is required.

   RECOVERY PROCEDURE (If Webhook Fails):
   1. Run the reconciliation script from the `data-ops` repo:
      python stripe_reconciliation_catchup.py --order-ids <comma_separated_ids>
      (For >500 records, append `--batch-size 100` to avoid Stripe rate limits).
   2. Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG 
      for the affected execution date to recalculate the Looker dashboard.
*/

-- ==============================================================================
-- SECTION 2: MONITORING & AUDIT QUERIES (RUN DAILY / ALERTING)
-- ==============================================================================

-- QUERY 2.1: SLA Verification Query
-- Run this to verify if the daily data has landed before the 6:00 AM UTC SLA.
SELECT 
    MAX(created_at) AS last_ingested_timestamp,
    CURRENT_TIMESTAMP() AS current_system_time,
    CASE 
        WHEN MAX(created_at) >= DATE_TRUNC('day', CURRENT_TIMESTAMP()) 
             + INTERVAL '6 hours' THEN 'SLA MET'
        ELSE 'SLA BREACH / DELAYED'
    END AS sla_status
FROM orders_v3_FINAL;


-- QUERY 2.2: Critical Edge Case Audit (The "Zero-Dollar Subtotal" Bug Detector)
-- Run this query to proactively identify orders that will break the downstream 
-- daily_revenue_etl pipeline. If this query returns rows, trigger the recovery script.
SELECT 
    order_id,
    customer_id,
    order_date,
    subtotal,
    shipping_cost,
    is_refunded,
    order_status,
    (subtotal + shipping_cost) AS total_charged
FROM orders_v3_FINAL
WHERE subtotal = 0 
  AND shipping_cost > 0 
  AND is_refunded = FALSE
  AND order_date >= CURRENT_DATE() - INTERVAL '2 days'
ORDER BY order_date DESC;


-- ==============================================================================
-- SECTION 3: CORE PIPELINE & DOWNSTREAM ETL SIMULATION
-- ==============================================================================

-- QUERY 3.1: Downstream ETL Simulation (daily_revenue_etl)
-- This query demonstrates how orders_v3_FINAL is aggregated to populate 
-- the revenue_dashboard_BACKUP. Note the defensive handling of the edge case.
WITH processed_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        order_status,
        -- Defensive logic to prevent negative margin calculations if webhook failed
        CASE 
            WHEN subtotal = 0 AND shipping_cost > 0 AND is_refunded = FALSE THEN TRUE
            ELSE is_refunded
        END AS safe_is_refunded,
        subtotal,
        shipping_cost,
        estimated_cost_of_goods
    FROM orders_v3_FINAL
    WHERE order_date = CURRENT_DATE() - INTERVAL '1 day'
),
daily_aggregates AS (
    SELECT 
        order_date,
        COUNT(DISTINCT order_id) AS total_order_count,
        SUM(CASE WHEN safe_is_refunded = TRUE THEN 0 ELSE subtotal END) AS gross_revenue,
        SUM(CASE WHEN safe_is_refunded = TRUE THEN 0 ELSE shipping_cost END) AS shipping_revenue,
        SUM(CASE WHEN safe_is_refunded = TRUE THEN 0 ELSE estimated_cost_of_goods END) AS total_cog,
        -- Margin calculation that would break (go negative) without the safe_is_refunded logic
        SUM(
            CASE 
                WHEN safe_is_refunded = TRUE THEN 0 
                ELSE (subtotal + shipping_cost) - estimated_cost_of_goods 
            END
        ) AS net_margin
    FROM processed_orders
    GROUP BY order_date
)
SELECT 
    order_date,
    total_order_count,
    gross_revenue,
    shipping_revenue,
    total_cog,
    net_margin,
    ROUND((net_margin / NULLIF(gross_revenue + shipping_revenue, 0)) * 100, 2) AS margin_percentage
FROM daily_aggregates;


-- ==============================================================================
-- SECTION 4: ANALYTICAL & DOWNSTREAM CONSUMPTION ASSETS
-- ==============================================================================

-- QUERY 4.1: Quarterly Board Metrics Generation (quarterly_board_metrics)
-- Demonstrates how orders_v3_FINAL joins with dim_customers to produce board-level KPIs.
SELECT 
    DATE_TRUNC('quarter', o.order_date) AS board_quarter,
    c.customer_segment,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.subtotal) AS total_subtotal_revenue,
    AVG(o.subtotal) AS average_order_value (AOV),
    -- Calculate customer lifetime value (LTV) proxy for the quarter
    SUM(o.subtotal) / COUNT(DISTINCT o.customer_id) AS quarterly_arpu
FROM orders_v3_FINAL o
INNER JOIN dim_customers c 
    ON o.customer_id = c.customer_id
WHERE o.order_status = 'COMPLETED'
  AND o.is_refunded = FALSE
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;


-- QUERY 4.2: Churn Prediction Feature Store (churn_prediction_features)
-- This query generates the features used by the ML pipeline to predict customer churn.
-- It aggregates historical order behavior from orders_v3_FINAL.
CREATE OR REPLACE TABLE churn_prediction_features AS
WITH customer_order_history AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS total_lifetime_orders,
        SUM(subtotal) AS lifetime_spend,
        MAX(order_date) AS last_order_date,
        MIN(order_date) AS first_order_date,
        DATEDIFF('day', MAX(order_date), CURRENT_DATE()) AS days_since_last_order,
        SUM(CASE WHEN is_refunded = TRUE THEN 1 ELSE 0 END) AS total_refunded_orders
    FROM orders_v3_FINAL
    WHERE order_status IN ('COMPLETED', 'DELIVERED')
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.customer_segment,
    c.signup_date,
    COALESCE(h.total_lifetime_orders, 0) AS total_lifetime_orders,
    COALESCE(h.lifetime_spend, 0.0) AS lifetime_spend,
    h.last_order_date,
    COALESCE(h.days_since_last_order, 9999) AS recency_days,
    COALESCE(h.total_refunded_orders, 0) AS total_refunded_orders,
    CASE 
        WHEN h.days_since_last_order > 90 THEN TRUE 
        ELSE FALSE 
    END AS is_churned_90d
FROM dim_customers c
LEFT JOIN customer_order_history h 
    ON c.customer_id = h.customer_id;


-- ==============================================================================
-- SECTION 5: HISTORICAL RECONCILIATION & DEPRECATED ASSETS
-- ==============================================================================

-- QUERY 5.1: Schema Evolution Audit (orders_v1 vs orders_v2 vs orders_v3_FINAL)
-- Use this query to reconcile historical data differences if migrating legacy reports.
SELECT 
    'v1' AS version, COUNT(*) AS record_count, SUM(subtotal) AS total_val, AVG(subtotal) AS avg_val FROM orders_v1
UNION ALL
SELECT 
    'v2' AS version, COUNT(*) AS record_count, SUM(subtotal) AS total_val, AVG(subtotal) AS avg_val FROM orders_v2
UNION ALL
SELECT 
    'v3_FINAL' AS version, COUNT(*) AS record_count, SUM(subtotal) AS total_val, AVG(subtotal) AS avg_val FROM orders_v3_FINAL;


-- QUERY 5.2: Kafka Raw Event Ingestion Audit (kafka_events_raw_copy)
-- Useful for debugging ingestion delays or missing orders before they hit orders_v3_FINAL.
SELECT 
    PARSE_JSON(event_payload):order_id::STRING AS raw_order_id,
    PARSE_JSON(event_payload):customer_id::STRING AS raw_customer_id,
    PARSE_JSON(event_payload):event_timestamp::TIMESTAMP AS event_time,
    PARSE_JSON(event_payload):status::STRING AS event_status,
    COUNT(*) AS duplicate_event_count
FROM kafka_events_raw_copy
WHERE event_time >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;