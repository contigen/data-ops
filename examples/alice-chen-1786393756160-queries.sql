-- =================================================================================
-- HANDOVER REFERENCE SQL SCRIPT: ALICE CHEN'S DATA ASSETS
-- Target Audience: Successor Engineer
-- Date: October 2023
-- 
-- This script serves as an operational runbook, schema reference, and query 
-- playbook for the data assets previously maintained by Alice Chen.
--
-- ASSET INDEX & URNS:
-- 1. Snowflake: orders_v3_FINAL (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD))
-- 2. Looker: revenue_dashboard_BACKUP (urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD))
-- 3. Kafka: kafka_events_raw_copy (urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD))
-- 4. Snowflake: orders_v2 [DEPRECATED] (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD))
-- 5. Snowflake: orders_v1 [DEPRECATED] (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD))
-- 6. Snowflake: quarterly_board_metrics (urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD))
-- 7. Airflow: daily_revenue_etl (urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD))
-- 8. Snowflake: churn_prediction_features (urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD))
-- 9. Snowflake: dim_customers (urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD))
-- =================================================================================


-- =================================================================================
-- SECTION 1: CRITICAL OPERATIONAL ALERTS & EDGE CASE MONITORING
-- =================================================================================

-- Asset: orders_v3_FINAL
-- SLA: Daily 6:00 AM UTC
--
-- CRITICAL EDGE CASE:
-- Canceled orders with a $0.00 subtotal but a positive shipping cost will break 
-- the downstream `daily_revenue_etl` pipeline (causing negative margin calculations)
-- if the `is_refunded` flag is not set to `true` within a 24-hour window.
--
-- RUN THIS QUERY TO DETECT THE ANOMALY:

SELECT 
    order_id,
    customer_id,
    order_date,
    order_status,
    subtotal_amount,
    shipping_cost_amount,
    is_refunded,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_order
FROM 
    PROD.orders_v3_FINAL
WHERE 
    subtotal_amount = 0.00 
    AND shipping_cost_amount > 0.00
    AND is_refunded = FALSE
    AND order_status = 'CANCELED';

-- =================================================================================
-- OPERATIONAL RECOVERY PROCEDURE (IF ANOMALIES ARE FOUND ABOVE):
-- =================================================================================
-- The `is_refunded` flag is populated via a Stripe webhook ingestion API. 
-- If the webhook fails to sync within the 24-hour window, execute the following:
--
-- 1. RECONCILIATION SCRIPT:
--    Run `stripe_reconciliation_catchup.py` from the `data-ops` repository.
--    * Single/Bulk Execution: 
--      python stripe_reconciliation_catchup.py --order-ids <ORDER_ID_1>,<ORDER_ID_2>
--    * Large Batches (>500 records): 
--      python stripe_reconciliation_catchup.py --file-path /path/to/ids.csv --batch-size 100
--      (Note: --batch-size 100 prevents Stripe API rate-limiting and timeouts)
--
-- 2. DOWNSTREAM BACKFILL:
--    Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG 
--    for the affected execution date to recalculate the downstream Looker dashboard.
-- =================================================================================


-- =================================================================================
-- SECTION 2: PIPELINE SIMULATION & REVENUE REPORTING
-- =================================================================================

-- This query simulates the core logic of the `daily_revenue_etl` Airflow DAG.
-- It aggregates daily financial metrics from `orders_v3_FINAL` and demonstrates
-- how the edge case (unrefunded shipping costs on $0 orders) affects margins.

CREATE OR REPLACE TABLE PROD.daily_revenue_etl_SIMULATION AS
SELECT 
    CAST(order_date AS DATE) AS revenue_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(subtotal_amount) AS gross_subtotal,
    SUM(shipping_cost_amount) AS gross_shipping,
    
    -- Margin calculation logic that breaks if the edge case is not handled:
    SUM(
        CASE 
            -- If canceled, $0 subtotal, positive shipping, and NOT marked refunded,
            -- it incorrectly registers as unrefunded cost, dragging down margins.
            WHEN order_status = 'CANCELED' AND subtotal_amount = 0 AND is_refunded = FALSE 
                THEN -shipping_cost_amount 
            WHEN is_refunded = TRUE 
                THEN 0
            ELSE (subtotal_amount + shipping_cost_amount) * 0.40 -- Assuming 40% flat margin
        END
    ) AS estimated_net_margin,
    
    -- Flag to alert downstream Looker dashboard (revenue_dashboard_BACKUP) of data quality issues
    MAX(
        CASE 
            WHEN subtotal_amount = 0 AND shipping_cost_amount > 0 AND is_refunded = FALSE AND order_status = 'CANCELED' 
                THEN 1 
            ELSE 0 
        END
    ) AS contains_unreconciled_stripe_data
FROM 
    PROD.orders_v3_FINAL
GROUP BY 
    1;


-- =================================================================================
-- SECTION 3: CUSTOMER & CHURN ANALYTICS
-- =================================================================================

-- Asset: churn_prediction_features
-- This query demonstrates how `orders_v3_FINAL` joins with `dim_customers`
-- to generate features for the downstream machine learning churn model.

CREATE OR REPLACE TABLE PROD.churn_prediction_features AS
WITH customer_order_aggregates AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) AS total_lifetime_orders,
        SUM(subtotal_amount) AS total_lifetime_spend,
        MAX(order_date) AS last_order_timestamp,
        COUNT(CASE WHEN order_status = 'CANCELED' THEN 1 END) AS total_canceled_orders,
        COUNT(CASE WHEN is_refunded = TRUE THEN 1 END) AS total_refunded_orders
    FROM 
        PROD.orders_v3_FINAL
    GROUP BY 
        customer_id
)
SELECT 
    c.customer_id,
    c.customer_segment,
    c.acquisition_channel,
    c.signup_date,
    COALESCE(a.total_lifetime_orders, 0) AS total_lifetime_orders,
    COALESCE(a.total_lifetime_spend, 0.00) AS total_lifetime_spend,
    COALESCE(a.total_canceled_orders, 0) AS total_canceled_orders,
    COALESCE(a.total_refunded_orders, 0) AS total_refunded_orders,
    DATEDIFF('day', a.last_order_timestamp, CURRENT_TIMESTAMP()) AS days_since_last_purchase,
    CASE 
        WHEN DATEDIFF('day', a.last_order_timestamp, CURRENT_TIMESTAMP()) > 90 THEN TRUE 
        ELSE FALSE 
    END AS is_churned_candidate
FROM 
    PROD.dim_customers c
LEFT JOIN 
    customer_order_aggregates a ON c.customer_id = a.customer_id;


-- =================================================================================
-- SECTION 4: BOARD LEVEL REPORTING
-- =================================================================================

-- Asset: quarterly_board_metrics
-- This query aggregates high-level KPIs for executive reporting.
-- It relies on the cleaned and validated `orders_v3_FINAL` dataset.

CREATE OR REPLACE TABLE PROD.quarterly_board_metrics AS
SELECT 
    DATE_TRUNC('QUARTER', o.order_date) AS reporting_quarter,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    COUNT(DISTINCT o.order_id) AS completed_orders,
    SUM(o.subtotal_amount) AS total_revenue,
    SUM(o.subtotal_amount) / COUNT(DISTINCT o.order_id) AS average_order_value,
    (SUM(CASE WHEN o.is_refunded = TRUE THEN o.subtotal_amount ELSE 0 END) / NULLIF(SUM(o.subtotal_amount), 0)) * 100 AS refund_rate_percentage
FROM 
    PROD.orders_v3_FINAL o
WHERE 
    o.order_status = 'COMPLETED'
GROUP BY 
    1
ORDER BY 
    1 DESC;


-- =================================================================================
-- SECTION 5: LEGACY DATA MIGRATION & AUDITING
-- =================================================================================

-- Assets: orders_v1, orders_v2, orders_v3_FINAL
-- The following query is a diagnostic tool to audit schema drift and record counts
-- across the historical iterations of the orders table. Use this if you need to 
-- debug historical data discrepancies.

SELECT 
    'orders_v1' AS table_version, 
    COUNT(*) AS total_records, 
    MIN(order_date) AS min_date, 
    MAX(order_date) AS max_date,
    'N/A - Legacy Schema' AS dbt_status
FROM PROD.orders_v1

UNION ALL

SELECT 
    'orders_v2' AS table_version, 
    COUNT(*) AS total_records, 
    MIN(order_date) AS min_date, 
    MAX(order_date) AS max_date,
    'N/A - Pre-validation Schema' AS dbt_status
FROM PROD.orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL' AS table_version, 
    COUNT(*) AS total_records, 
    MIN(order_date) AS min_date, 
    MAX(order_date) AS max_date,
    'Passed dbt not_null & accepted_values tests' AS dbt_status
FROM PROD.orders_v3_FINAL;


-- =================================================================================
-- SECTION 6: REAL-TIME INGESTION AUDIT
-- =================================================================================

-- Asset: kafka_events_raw_copy
-- This table stores raw payloads from the upstream Kafka topic. 
-- Use this query to verify if the raw events are flowing into Snowflake 
-- before they are processed into `orders_v3_FINAL`.

SELECT 
    event_id,
    event_timestamp,
    payload:order_id::VARCHAR AS parsed_order_id,
    payload:status::VARCHAR AS parsed_status,
    payload:stripe_charge_id::VARCHAR AS stripe_charge_id,
    payload
FROM 
    PROD.kafka_events_raw_copy
WHERE 
    event_timestamp >= DATEADD('day', -3, CURRENT_TIMESTAMP())
ORDER BY 
    event_timestamp DESC
LIMIT 100;