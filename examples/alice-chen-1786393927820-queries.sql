-- ==============================================================================
-- DATA ASSETS HANDOFF REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Target Audience: Successor / Data Engineering Team
-- Date: October 2023
--
-- This script serves as an operational runbook, schema reference, and query 
-- playbook for the data assets previously maintained by alice.chen.
-- ==============================================================================


-- ==============================================================================
-- SECTION 1: OPERATIONAL DOCUMENTATION & RUNBOOKS
-- ==============================================================================

/*
ASSET: orders_v3_FINAL (Dataset)
URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)

Overview:
- Production orders dataset updated daily under a strict 6:00 AM UTC SLA.
- Populates the daily executive dashboard (revenue_dashboard_BACKUP).
- The "FINAL" suffix indicates the table has passed dbt data quality assertions,
  specifically "not_null" and "accepted_values" tests on the "order_status" column.

Known Edge Cases & Downstream Impact:
- Zero-Dollar Subtotal / Positive Shipping Cost: Canceled orders with a $0 subtotal 
  and a positive shipping cost will break the downstream "daily_revenue_etl" pipeline 
  (causing negative margin calculations) if the "is_refunded" flag is not set to 
  "true" within a 24-hour window.

Operational Recovery Procedure:
The "is_refunded" flag is populated via a Stripe webhook ingestion API. If the 
webhook fails to sync within the 24-hour window, execute the following recovery steps:

1. Reconciliation Script: Run "stripe_reconciliation_catchup.py" from the "data-ops" repo.
   - Single/Bulk Execution: Pass target IDs via "--order-ids" (comma-separated) 
     or "--file-path" (CSV path).
   - Rate Limiting: For batches exceeding 500 records, append the "--batch-size 100" 
     flag to prevent Stripe API rate-limiting and script timeouts.
   
   Example CLI Command:
   $ python stripe_reconciliation_catchup.py --order-ids 12345,67890 --batch-size 100

2. Downstream Backfill: Manually trigger an Airflow backfill on the "daily_revenue_etl" 
   DAG for the affected execution date to recalculate the downstream Looker dashboard.
*/


-- ==============================================================================
-- SECTION 2: DIAGNOSTIC & MONITORING QUERIES
-- ==============================================================================

-- QUERY 2.1: SLA & Edge Case Monitoring Query
-- Run this query to identify orders violating the zero-dollar subtotal edge case.
-- If this query returns rows where is_refunded is FALSE for orders older than 24 hours,
-- you must trigger the Operational Recovery Procedure detailed above.

SELECT 
    order_id,
    customer_id,
    order_date,
    subtotal,
    shipping_cost,
    is_refunded,
    order_status,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM orders_v3_FINAL
WHERE subtotal = 0 
  AND shipping_cost > 0 
  AND is_refunded = FALSE
  AND order_date >= DATEADD('day', -3, CURRENT_DATE())
ORDER BY order_date DESC;


-- QUERY 2.2: DBT Quality Assertion Simulation
-- Simulates the dbt tests run on orders_v3_FINAL to ensure data integrity.
-- Checks for nulls in critical fields and unexpected order_status values.

WITH validation_errors AS (
    SELECT 
        order_id,
        order_status,
        CASE 
            WHEN order_id IS NULL THEN 'NULL_ORDER_ID'
            WHEN order_status IS NULL THEN 'NULL_ORDER_STATUS'
            WHEN order_status NOT IN ('placed', 'processed', 'shipped', 'delivered', 'cancelled', 'refunded') 
                THEN 'INVALID_ORDER_STATUS_VALUE'
            ELSE 'PASS'
        END AS test_result
    FROM orders_v3_FINAL
)
SELECT 
    test_result,
    COUNT(*) AS record_count
FROM validation_errors
WHERE test_result != 'PASS'
GROUP BY 1;


-- ==============================================================================
-- SECTION 3: DOWNSTREAM PIPELINE & ANALYTICS QUERIES
-- ==============================================================================

-- QUERY 3.1: Daily Revenue ETL Simulation (daily_revenue_etl)
-- This query mimics the logic used to populate the daily executive dashboard 
-- (revenue_dashboard_BACKUP). Note the handling of the refund edge case.

WITH daily_metrics AS (
    SELECT
        CAST(order_date AS DATE) AS reporting_date,
        COUNT(DISTINCT order_id) AS total_order_count,
        -- Corrected revenue logic handling the zero-dollar subtotal edge case
        SUM(
            CASE 
                WHEN subtotal = 0 AND shipping_cost > 0 AND is_refunded = TRUE THEN 0
                WHEN is_refunded = TRUE THEN 0
                ELSE (subtotal + shipping_cost)
            END
        ) AS net_revenue,
        SUM(shipping_cost) AS total_shipping_collected,
        SUM(CASE WHEN is_refunded = TRUE THEN 1 ELSE 0 END) AS refunded_order_count
    FROM orders_v3_FINAL
    WHERE order_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY 1
)
SELECT 
    reporting_date,
    total_order_count,
    net_revenue,
    total_shipping_collected,
    refunded_order_count,
    -- Alert flag if net revenue is negative (indicates the edge case broke the pipeline)
    CASE WHEN net_revenue < 0 THEN 'ALERT: Negative Revenue Detected!' ELSE 'OK' END AS pipeline_status
FROM daily_metrics
ORDER BY reporting_date DESC;


-- QUERY 3.2: Customer Churn Feature Generation (churn_prediction_features)
-- Demonstrates how dim_customers and orders_v3_FINAL are joined to generate 
-- features for the downstream machine learning model.

CREATE OR REPLACE TABLE churn_prediction_features AS
WITH customer_order_history AS (
    SELECT
        c.customer_id,
        c.signup_date,
        COUNT(o.order_id) AS total_orders_lifetime,
        SUM(COALESCE(o.subtotal, 0)) AS total_spend_lifetime,
        MAX(o.order_date) AS last_order_timestamp,
        MIN(o.order_date) AS first_order_timestamp,
        COUNT(CASE WHEN o.is_refunded = TRUE THEN 1 END) AS total_refunded_orders
    FROM dim_customers c
    LEFT JOIN orders_v3_FINAL o 
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.signup_date
)
SELECT
    customer_id,
    signup_date,
    total_orders_lifetime,
    total_spend_lifetime,
    last_order_timestamp,
    DATEDIFF('day', last_order_timestamp, CURRENT_TIMESTAMP()) AS days_since_last_purchase,
    DATEDIFF('day', signup_date, CURRENT_TIMESTAMP()) AS customer_tenure_days,
    total_refunded_orders,
    CASE 
        WHEN DATEDIFF('day', last_order_timestamp, CURRENT_TIMESTAMP()) > 90 THEN TRUE 
        ELSE FALSE 
    END AS is_churned_prediction_label
FROM customer_order_history;


-- QUERY 3.3: Quarterly Board Metrics Aggregation (quarterly_board_metrics)
-- Aggregates historical order data to populate high-level board metrics.

CREATE OR REPLACE TABLE quarterly_board_metrics AS
SELECT
    DATE_TRUNC('quarter', o.order_date) AS fiscal_quarter,
    COUNT(DISTINCT o.customer_id) AS active_purchasing_customers,
    COUNT(DISTINCT o.order_id) AS total_orders_processed,
    SUM(o.subtotal) AS gross_merchandise_value_gmv,
    SUM(CASE WHEN o.is_refunded = TRUE THEN (o.subtotal + o.shipping_cost) ELSE 0 END) AS total_refunded_amount,
    (SUM(o.subtotal) / COUNT(DISTINCT o.order_id)) AS average_order_value_aov
FROM orders_v3_FINAL o
INNER JOIN dim_customers c 
    ON o.customer_id = c.customer_id
WHERE o.order_status NOT IN ('cancelled')
GROUP BY 1
ORDER BY 1 DESC;


-- ==============================================================================
-- SECTION 4: DEPRECATION & LINEAGE AUDITING
-- ==============================================================================

-- QUERY 4.1: Schema Evolution & Migration Audit
-- Compares record counts and volume across legacy tables (orders_v1, orders_v2) 
-- and the current production table (orders_v3_FINAL) to ensure migration parity.

SELECT 
    'orders_v1 (Legacy)' AS table_version,
    COUNT(*) AS total_records,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
FROM orders_v1

UNION ALL

SELECT 
    'orders_v2 (Deprecated)' AS table_version,
    COUNT(*) AS total_records,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
FROM orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL (Production)' AS table_version,
    COUNT(*) AS total_records,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
FROM orders_v3_FINAL;


-- QUERY 4.2: Kafka Ingestion Reconciliation (kafka_events_raw_copy)
-- Audits raw events ingested from Kafka against the final Snowflake orders table 
-- to identify any ingestion loss or latency issues.

WITH raw_kafka_counts AS (
    SELECT 
        DATE_TRUNC('day', event_timestamp) AS event_date,
        COUNT(DISTINCT JSON_EXTRACT_PATH_TEXT(event_payload, 'order_id')) AS raw_event_count
    FROM kafka_events_raw_copy
    WHERE event_timestamp >= DATEADD('day', -7, CURRENT_DATE())
      AND event_type = 'ORDER_CREATED'
    GROUP BY 1
),
snowflake_order_counts AS (
    SELECT 
        DATE_TRUNC('day', order_date) AS order_date,
        COUNT(DISTINCT order_id) AS final_order_count
    FROM orders_v3_FINAL
    WHERE order_date >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY 1
)
SELECT 
    k.event_date,
    k.raw_event_count AS events_received_in_kafka,
    s.final_order_count AS orders_written_to_snowflake,
    (k.raw_event_count - COALESCE(s.final_order_count, 0)) AS ingestion_discrepancy
FROM raw_kafka_counts k
LEFT JOIN snowflake_order_counts s 
    ON k.event_date = s.order_date
ORDER BY k.event_date DESC;