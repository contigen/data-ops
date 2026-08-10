-- ==============================================================================
-- DATA ASSET HANDOVER RUNBOOK & REFERENCE SQL
-- Departing Engineer: alice.chen
-- Target Successor: Data Engineering Team
-- Date: October 2023
-- ==============================================================================

-- ==============================================================================
-- SECTION 1: OVERVIEW & ASSET MAP
-- ==============================================================================
/*
This script serves as the technical documentation and operational runbook for the 
data assets previously maintained by alice.chen. 

ASSET DIRECTORY & RELATIONSHIPS:
1. snowflake.PROD.orders_v3_FINAL (Dataset - Snowflake)
   - Primary production orders table. Daily SLA: 6:00 AM UTC.
   - Downstream of: Stripe Webhook Ingestion API & Kafka Raw Events (kafka_events_raw_copy)
   - Upstream of: daily_revenue_etl, churn_prediction_features, quarterly_board_metrics

2. snowflake.PROD.orders_v2 & orders_v1 (Datasets - Snowflake)
   - Deprecated legacy schemas. Retained for historical audit and migration validation.

3. snowflake.PROD.dim_customers (Dataset - Snowflake)
   - Customer dimension table used to enrich order data for downstream features.

4. airflow.daily_revenue_etl (DAG / Dataset - Airflow)
   - Daily ETL pipeline that aggregates revenue metrics. Extremely sensitive to 
     unresolved refunds in orders_v3_FINAL.

5. looker.revenue_dashboard_BACKUP (Dashboard - Looker)
   - Backup executive dashboard visualizing daily revenue metrics.

6. snowflake.PROD.churn_prediction_features (Dataset - Snowflake)
   - Feature store table containing customer-level behavioral aggregations.

7. snowflake.PROD.quarterly_board_metrics (Dataset - Snowflake)
   - Aggregated financial and operational metrics delivered to board members.
*/


-- ==============================================================================
-- SECTION 2: OPERATIONAL RECOVERY & EDGE CASE MONITORING
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- CRITICAL EDGE CASE DETECTOR: Zero-Dollar Subtotal with Positive Shipping Cost
-- ------------------------------------------------------------------------------
-- CONTEXT: Canceled orders with a $0 subtotal and a positive shipping cost will 
-- break the downstream `daily_revenue_etl` pipeline (causing negative margin 
-- calculations) if the `is_refunded` flag is not set to `true` within 24 hours.
--
-- RUN THIS QUERY to identify records causing/threatening to cause ETL failures:

SELECT 
    order_id,
    customer_id,
    order_date,
    order_status,
    subtotal,
    shipping_cost,
    is_refunded,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM snowflake.PROD.orders_v3_FINAL
WHERE subtotal = 0 
  AND shipping_cost > 0 
  AND is_refunded = FALSE
ORDER BY order_date DESC;


-- ------------------------------------------------------------------------------
-- RECOVERY PROCEDURE FOR WEBHOOK FAILURES
-- ------------------------------------------------------------------------------
/*
If the query above returns rows older than 24 hours, the Stripe webhook ingestion 
API has likely failed to sync the refund status. Follow these recovery steps:

Step 1: Extract the affected Order IDs using the query below to generate a CSV 
        or comma-separated list.

Step 2: Execute the reconciliation script from the `data-ops` repository:
        
        # For single/small batch execution:
        python stripe_reconciliation_catchup.py --order-ids "ORD-12345,ORD-67890"
        
        # For bulk execution (batches exceeding 500 records, use rate-limiting):
        python stripe_reconciliation_catchup.py --file-path /path/to/affected_orders.csv --batch-size 100

Step 3: Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG 
        for the affected execution dates to fix the Looker dashboard.
*/

-- Helper query to format Order IDs for the CLI recovery tool:
SELECT LISTAGG(DISTINCT "'" || order_id || "'", ', ') AS formatted_order_ids
FROM snowflake.PROD.orders_v3_FINAL
WHERE subtotal = 0 
  AND shipping_cost > 0 
  AND is_refunded = FALSE;


-- ==============================================================================
-- SECTION 3: CORE PIPELINE SIMULATIONS & DOWNSTREAM QUERIES
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- PIPELINE 1: daily_revenue_etl (Airflow Downstream Simulation)
-- ------------------------------------------------------------------------------
-- This query simulates the logic executed inside the `daily_revenue_etl` pipeline.
-- It aggregates daily financial metrics and handles the refund logic safely.

WITH daily_aggregated_orders AS (
    SELECT 
        CAST(order_date AS DATE) AS reporting_date,
        COUNT(DISTINCT order_id) AS total_order_count,
        -- If refunded, subtotal and shipping are excluded from net revenue calculations
        SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE subtotal END) AS net_subtotal_revenue,
        SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE shipping_cost END) AS net_shipping_revenue,
        -- Margin calculation logic (vulnerable to the $0 subtotal / positive shipping bug)
        SUM(
            CASE 
                WHEN is_refunded = TRUE THEN 0 
                ELSE (subtotal - shipping_cost) 
            END
        ) AS net_margin
    FROM snowflake.PROD.orders_v3_FINAL
    -- dbt data quality assertion equivalent: ensure status is valid
    WHERE order_status IN ('COMPLETED', 'SHIPPED', 'PROCESSING', 'CANCELLED')
    GROUP BY 1
)
SELECT 
    reporting_date,
    total_order_count,
    net_subtotal_revenue,
    net_shipping_revenue,
    (net_subtotal_revenue + net_shipping_revenue) AS gross_revenue,
    net_margin,
    -- Safe division to prevent division-by-zero errors on low-volume days
    ROUND(SAFE_DIVIDE(net_margin, (net_subtotal_revenue + net_shipping_revenue)) * 100, 2) AS margin_percentage
FROM daily_aggregated_orders
ORDER BY reporting_date DESC;


-- ------------------------------------------------------------------------------
-- PIPELINE 2: churn_prediction_features (Snowflake Feature Store)
-- ------------------------------------------------------------------------------
-- This query builds behavioral features for the churn prediction model.
-- It joins `dim_customers` with `orders_v3_FINAL`.

CREATE OR REPLACE TABLE snowflake.PROD.churn_prediction_features AS
SELECT 
    c.customer_id,
    c.signup_date,
    c.customer_segment,
    COUNT(DISTINCT o.order_id) AS total_lifetime_orders,
    SUM(COALESCE(o.subtotal, 0)) AS total_lifetime_spend,
    AVG(COALESCE(o.subtotal, 0)) AS average_order_value,
    MAX(o.order_date) AS last_purchase_timestamp,
    DATEDIFF('day', MAX(o.order_date), CURRENT_TIMESTAMP()) AS days_since_last_purchase,
    -- Refund rate calculation to identify problematic customers
    ROUND(
        COUNT(DISTINCT CASE WHEN o.is_refunded = TRUE THEN o.order_id END) / 
        NULLIF(COUNT(DISTINCT o.order_id), 0) * 100, 
        2
    ) AS refund_rate_percentage
FROM snowflake.PROD.dim_customers c
LEFT JOIN snowflake.PROD.orders_v3_FINAL o 
    ON c.customer_id = o.customer_id
GROUP BY 1, 2, 3;


-- ------------------------------------------------------------------------------
-- PIPELINE 3: quarterly_board_metrics (Snowflake Board Reporting)
-- ------------------------------------------------------------------------------
-- Aggregates high-level financial metrics by quarter for executive review.

CREATE OR REPLACE TABLE snowflake.PROD.quarterly_board_metrics AS
SELECT 
    DATE_TRUNC('quarter', o.order_date) AS fiscal_quarter,
    COUNT(DISTINCT o.customer_id) AS unique_purchasing_customers,
    COUNT(DISTINCT o.order_id) AS total_orders_processed,
    SUM(CASE WHEN o.is_refunded = FALSE THEN o.subtotal ELSE 0 END) AS net_sales,
    SUM(CASE WHEN o.is_refunded = TRUE THEN o.subtotal ELSE 0 END) AS total_refunded_amount,
    ROUND(
        SUM(CASE WHEN o.is_refunded = TRUE THEN o.subtotal ELSE 0 END) / 
        NULLIF(SUM(o.subtotal), 0) * 100, 
        2
    ) AS refund_drag_ratio
FROM snowflake.PROD.orders_v3_FINAL o
GROUP BY 1
ORDER BY fiscal_quarter DESC;


-- ==============================================================================
-- SECTION 4: LINEAGE & DEPRECATION AUDIT
-- ==============================================================================
-- Use this query to audit data consistency and volume differences across the 
-- legacy order tables (v1, v2) and the current production table (v3_FINAL).
-- This is critical for deprecating the older assets safely.

SELECT 
    'orders_v1' AS table_version,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS unique_orders,
    MIN(order_date) AS earliest_record,
    MAX(order_date) AS latest_record,
    'DEPRECATED' AS status
FROM snowflake.PROD.orders_v1

UNION ALL

SELECT 
    'orders_v2' AS table_version,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS unique_orders,
    MIN(order_date) AS earliest_record,
    MAX(order_date) AS latest_record,
    'DEPRECATED' AS status
FROM snowflake.PROD.orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL' AS table_version,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS unique_orders,
    MIN(order_date) AS earliest_record,
    MAX(order_date) AS latest_record,
    'ACTIVE_PROD' AS status
FROM snowflake.PROD.orders_v3_FINAL;