-- ==============================================================================
-- HANDOVER REFERENCE SQL SCRIPT
-- Departing Engineer: alice.chen
-- Target Successor: Data Engineering Team
-- Date: October 2023
-- ==============================================================================

-- ==============================================================================
-- OVERVIEW & OPERATIONAL RUNBOOKS
-- ==============================================================================
/*
This script serves as the technical documentation and operational runbook for the 
data assets previously managed by alice.chen. It details the relationships, 
known edge cases, and recovery procedures for the core order and revenue pipelines.

ASSETS COVERED:
1. PROD.orders_v3_FINAL (Snowflake Dataset)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)
   - SLA: Updated daily by 6:00 AM UTC.
   - Quality: Passed dbt tests (not_null, accepted_values on order_status).
2. PROD.dim_customers (Snowflake Dataset)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD)
3. PROD.quarterly_board_metrics (Snowflake Dataset)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD)
4. PROD.churn_prediction_features (Snowflake Dataset)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD)
5. PROD.orders_v2 & PROD.orders_v1 (Snowflake Datasets - Deprecated)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD)
6. daily_revenue_etl (Airflow DAG)
   - URN: urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD)
7. revenue_dashboard_BACKUP (Looker Dashboard)
   - URN: urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD)
8. kafka_events_raw_copy (Kafka Topic)
   - URN: urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD)

--------------------------------------------------------------------------------
CRITICAL EDGE CASE & DOWNSTREAM IMPACT:
--------------------------------------------------------------------------------
Canceled orders with a $0 subtotal and a positive shipping cost will break the 
downstream `daily_revenue_etl` pipeline (causing negative margin calculations) 
if the `is_refunded` flag is not set to `true` within a 24-hour window.

--------------------------------------------------------------------------------
OPERATIONAL RECOVERY PROCEDURE (Stripe Webhook Failures):
--------------------------------------------------------------------------------
The `is_refunded` flag is populated via a Stripe webhook ingestion API. If the 
webhook fails to sync within the 24-hour window, execute the following recovery steps:

1. Run the Reconciliation Script:
   Execute `stripe_reconciliation_catchup.py` from the `data-ops` repository.
   - Single/Bulk Execution: Pass target IDs via `--order-ids` (comma-separated) 
     or `--file-path` (CSV path).
   - Rate Limiting: For batches exceeding 500 records, append the `--batch-size 100` 
     flag to prevent Stripe API rate-limiting and script timeouts.
   
   Example CLI Command:
   $ python stripe_reconciliation_catchup.py --order-ids 12345,67890 --batch-size 100

2. Downstream Backfill:
   Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG for the 
   affected execution date to recalculate the downstream Looker dashboard 
   (`revenue_dashboard_BACKUP`).
*/

-- ==============================================================================
-- QUERY 1: SLA & CRITICAL EDGE CASE MONITORING
-- Use this query to proactively identify orders that violate the edge case rule
-- and could potentially break the downstream daily_revenue_etl pipeline.
-- ==============================================================================

SELECT
    order_id,
    customer_id,
    order_date,
    subtotal,
    shipping_cost,
    is_refunded,
    order_status,
    -- Flag indicating if this record will break the downstream ETL
    CASE 
        WHEN subtotal = 0 AND shipping_cost > 0 AND (is_refunded IS NULL OR is_refunded = FALSE) 
        THEN 'CRITICAL_ERROR: Will Break Downstream ETL'
        ELSE 'OK'
    END AS operational_status
FROM PROD.orders_v3_FINAL
WHERE order_date >= DATEADD('day', -2, CURRENT_DATE())
  AND subtotal = 0 
  AND shipping_cost > 0;


-- ==============================================================================
-- QUERY 2: DAILY REVENUE ETL SIMULATION (SAFE MARGIN CALCULATION)
-- This query demonstrates how the daily_revenue_etl pipeline aggregates data
-- for the revenue_dashboard_BACKUP, incorporating defensive logic against the 
-- zero-dollar subtotal edge case.
-- ==============================================================================

WITH daily_aggregates AS (
    SELECT
        order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        -- Exclude refunded orders from revenue metrics
        SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE subtotal END) AS net_subtotal,
        SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE shipping_cost END) AS net_shipping,
        -- Defensive margin calculation to prevent negative margin bugs
        SUM(
            CASE
                -- If the edge case occurs, force margin to 0 to prevent pipeline failure
                WHEN subtotal = 0 AND shipping_cost > 0 AND (is_refunded IS NULL OR is_refunded = FALSE) THEN 0
                WHEN is_refunded = TRUE THEN 0
                ELSE (subtotal * 0.85) - (shipping_cost * 0.10) -- Standard margin formula
            END
        ) AS calculated_margin
    FROM PROD.orders_v3_FINAL
    WHERE order_status IN ('COMPLETED', 'DELIVERED', 'SHIPPED') -- dbt accepted_values
    GROUP BY 1
)
SELECT
    order_date,
    total_orders,
    net_subtotal,
    net_shipping,
    calculated_margin,
    -- Calculate margin percentage safely
    CASE 
        WHEN net_subtotal = 0 THEN 0 
        ELSE ROUND((calculated_margin / net_subtotal) * 100, 2) 
    END AS margin_percentage
FROM daily_aggregates
ORDER BY order_date DESC;


-- ==============================================================================
-- QUERY 3: QUARTERLY BOARD METRICS GENERATION
-- Demonstrates how orders_v3_FINAL joins with dim_customers to populate the
-- quarterly_board_metrics table.
-- ==============================================================================

-- INSERT INTO PROD.quarterly_board_metrics (board_quarter, customer_segment, ...)
SELECT
    DATE_TRUNC('QUARTER', o.order_date) AS board_quarter,
    c.customer_segment,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(CASE WHEN o.is_refunded = FALSE THEN o.subtotal ELSE 0 END), 2) AS net_revenue,
    ROUND(SUM(CASE WHEN o.is_refunded = TRUE THEN o.subtotal ELSE 0 END), 2) AS refunded_revenue,
    ROUND(AVG(o.subtotal), 2) AS average_order_value
FROM PROD.orders_v3_FINAL o
INNER JOIN PROD.dim_customers c 
    ON o.customer_id = c.customer_id
WHERE o.order_status != 'CANCELLED'
GROUP BY 1, 2
ORDER BY 1 DESC, 2 ASC;


-- ==============================================================================
-- QUERY 4: CHURN PREDICTION FEATURE GENERATION
-- Demonstrates how orders_v3_FINAL and dim_customers are aggregated to build
-- features for the churn_prediction_features table.
-- ==============================================================================

-- INSERT INTO PROD.churn_prediction_features (customer_id, ...)
SELECT
    c.customer_id,
    c.customer_segment,
    c.signup_date,
    DATEDIFF('day', c.signup_date, CURRENT_DATE()) AS customer_tenure_days,
    MAX(o.order_date) AS last_purchase_date,
    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) AS days_since_last_purchase,
    COUNT(DISTINCT o.order_id) AS total_lifetime_orders,
    ROUND(SUM(CASE WHEN o.is_refunded = FALSE THEN o.subtotal ELSE 0 END), 2) AS lifetime_spend,
    ROUND(SUM(CASE WHEN o.is_refunded = TRUE THEN o.subtotal ELSE 0 END), 2) AS lifetime_refunded_amount,
    -- Refund rate feature
    ROUND(
        COUNT(DISTINCT CASE WHEN o.is_refunded = TRUE THEN o.order_id END) / 
        NULLIF(COUNT(DISTINCT o.order_id), 0) * 100, 
        2
    ) AS order_refund_rate_pct
FROM PROD.dim_customers c
LEFT JOIN PROD.orders_v3_FINAL o 
    ON c.customer_id = o.customer_id
GROUP BY 1, 2, 3;


-- ==============================================================================
-- QUERY 5: DEPRECATION & LINEAGE AUDIT
-- Use this query to verify that legacy tables (orders_v1, orders_v2) are no 
-- longer receiving updates and that all downstream processes have successfully 
-- migrated to orders_v3_FINAL.
-- ==============================================================================

WITH legacy_v1 AS (
    SELECT 'orders_v1' AS table_version, COUNT(*) AS row_count, MAX(order_date) AS max_date FROM PROD.orders_v1
),
legacy_v2 AS (
    SELECT 'orders_v2' AS table_version, COUNT(*) AS row_count, MAX(order_date) AS max_date FROM PROD.orders_v2
),
current_v3 AS (
    SELECT 'orders_v3_FINAL' AS table_version, COUNT(*) AS row_count, MAX(order_date) AS max_date FROM PROD.orders_v3_FINAL
)
SELECT * FROM legacy_v1
UNION ALL
SELECT * FROM legacy_v2
UNION ALL
SELECT * FROM current_v3;