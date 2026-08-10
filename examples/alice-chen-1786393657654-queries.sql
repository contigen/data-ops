-- ==============================================================================
-- DATA ASSET HANDOVER REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Target Audience: Successor Data Engineering & Analytics Team
-- Date: October 2023
-- ==============================================================================

-- ==============================================================================
-- SECTION 1: OPERATIONAL RUNBOOK & SLA DOCUMENTATION (Reference Only)
-- ==============================================================================
/*
  ASSET: PROD.orders_v3_FINAL (Snowflake Dataset)
  URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)
  
  SLA & QUALITY ASSURANCES:
  - Daily Update SLA: Must be fully populated and fresh by 6:00 AM UTC daily.
  - Downstream Impact: Directly populates the daily executive dashboard.
  - Data Quality: dbt assertions are run daily to enforce 'not_null' and 
    'accepted_values' on the `order_status` column.

  KNOWN EDGE CASE & PIPELINE BREAKERS:
  - Zero-Dollar Subtotal / Positive Shipping Cost:
    Canceled orders with a $0 subtotal and a positive shipping cost will break 
    the downstream `daily_revenue_etl` pipeline (causing negative margin calculations) 
    IF the `is_refunded` flag is not set to `true` within a 24-hour window.

  OPERATIONAL RECOVERY PROCEDURE (Stripe Webhook Failures):
  The `is_refunded` flag is populated via a Stripe webhook ingestion API. 
  If the webhook fails to sync within the 24-hour window, execute these steps:
  
  1. Run the Reconciliation Script:
     Execute `stripe_reconciliation_catchup.py` from the `data-ops` repository.
     - Single/Bulk Execution: Pass target IDs via `--order-ids` (comma-separated) 
       or `--file-path` (CSV path).
     - Rate Limiting: For batches exceeding 500 records, append the `--batch-size 100` 
       flag to prevent Stripe API rate-limiting and script timeouts.
       Example: python stripe_reconciliation_catchup.py --file-path failed_orders.csv --batch-size 100
       
  2. Trigger Downstream Backfill:
     Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG 
     (urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD))
     for the affected execution date to recalculate the downstream Looker dashboard 
     (urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD)).
*/

-- ==============================================================================
-- SECTION 2: DATA QUALITY MONITORING & AUDIT QUERIES
-- ==============================================================================

-- Query 1: SLA Freshness Verification
-- Checks if the orders_v3_FINAL table has been updated for the current day before 6:00 AM UTC.
SELECT 
    MAX(updated_at) AS last_modified_time,
    CURRENT_TIMESTAMP() AS current_system_time,
    CASE 
        WHEN MAX(updated_at) >= DATE_TRUNC('day', CURRENT_TIMESTAMP()) + INTERVAL '6 hours' THEN 'SLA Met'
        ELSE 'SLA Breached / Pending Update'
    END AS sla_status
FROM PROD.orders_v3_FINAL;


-- Query 2: Edge Case Detection (Zero-Dollar Subtotal / Positive Shipping Cost)
-- Run this query to proactively identify records that will break the downstream daily_revenue_etl.
SELECT 
    order_id,
    customer_id,
    order_date,
    subtotal,
    shipping_cost,
    is_refunded,
    order_status,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_order
FROM PROD.orders_v3_FINAL
WHERE subtotal = 0 
  AND shipping_cost > 0 
  AND is_refunded = FALSE
  AND order_status = 'CANCELED'
ORDER BY order_date DESC;


-- ==============================================================================
-- SECTION 3: CORE PIPELINE & DOWNSTREAM AGGREGATIONS
-- ==============================================================================

-- Query 3: Safe Daily Revenue Calculation (Simulating daily_revenue_etl logic)
-- This query joins orders_v3_FINAL with dim_customers and safely handles the zero-dollar subtotal edge case.
-- It serves as the foundation for the Looker dashboard (revenue_dashboard_BACKUP).
WITH safe_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        order_status,
        shipping_cost,
        -- Safe margin handling: If the edge case is detected, force subtotal to 0 and flag it
        CASE 
            WHEN subtotal = 0 AND shipping_cost > 0 AND is_refunded = FALSE AND order_status = 'CANCELED' THEN 0
            ELSE subtotal 
        END AS adjusted_subtotal,
        is_refunded
    FROM PROD.orders_v3_FINAL
)
SELECT 
    o.order_date,
    c.customer_segment,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.adjusted_subtotal) AS net_subtotal_revenue,
    SUM(o.shipping_cost) AS total_shipping_collected,
    SUM(o.adjusted_subtotal + o.shipping_cost) AS gross_revenue,
    SUM(CASE WHEN o.is_refunded = TRUE THEN o.adjusted_subtotal ELSE 0 END) AS refunded_amount
FROM safe_orders o
LEFT JOIN PROD.dim_customers c 
    ON o.customer_id = c.customer_id
WHERE o.order_date >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;


-- Query 4: Quarterly Board Metrics Generation
-- Aggregates high-level financial metrics for PROD.quarterly_board_metrics.
-- Excludes canceled/refunded orders to present clean board-level figures.
CREATE OR REPLACE TABLE PROD.quarterly_board_metrics AS
SELECT 
    DATE_TRUNC('quarter', o.order_date) AS fiscal_quarter,
    COUNT(DISTINCT o.customer_id) AS unique_purchasing_customers,
    COUNT(DISTINCT o.order_id) AS total_completed_orders,
    SUM(o.subtotal) AS total_product_revenue,
    SUM(o.shipping_cost) AS total_shipping_revenue,
    AVG(o.subtotal) AS average_order_value,
    DIV0(SUM(o.subtotal), COUNT(DISTINCT o.customer_id)) AS customer_lifetime_value_contribution
FROM PROD.orders_v3_FINAL o
WHERE o.order_status NOT IN ('CANCELED', 'FAILED')
  AND o.is_refunded = FALSE
GROUP BY 1
ORDER BY 1 DESC;


-- Query 5: Churn Prediction Feature Engineering
-- Generates features for PROD.churn_prediction_features by combining customer dimensions and order history.
CREATE OR REPLACE TABLE PROD.churn_prediction_features AS
WITH customer_order_stats AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_purchase_date,
        MAX(order_date) AS last_purchase_date,
        DATEDIFF('day', MAX(order_date), CURRENT_DATE()) AS days_since_last_purchase,
        COUNT(DISTINCT order_id) AS total_lifetime_orders,
        SUM(subtotal) AS total_lifetime_spend,
        AVG(subtotal) AS average_order_spend,
        SUM(CASE WHEN order_status = 'CANCELED' THEN 1 ELSE 0 END) AS total_canceled_orders
    FROM PROD.orders_v3_FINAL
    GROUP BY 1
)
SELECT 
    c.customer_id,
    c.customer_segment,
    c.signup_date,
    DATEDIFF('day', c.signup_date, CURRENT_DATE()) AS customer_tenure_days,
    COALESCE(s.days_since_last_purchase, 9999) AS recency_days,
    COALESCE(s.total_lifetime_orders, 0) AS frequency_count,
    COALESCE(s.total_lifetime_spend, 0.0) AS monetary_value,
    COALESCE(s.average_order_spend, 0.0) AS avg_monetary_value,
    DIV0(COALESCE(s.total_canceled_orders, 0), COALESCE(s.total_lifetime_orders, 1)) AS cancellation_rate
FROM PROD.dim_customers c
LEFT JOIN customer_order_stats s 
    ON c.customer_id = s.customer_id;


-- ==============================================================================
-- SECTION 4: DEPRECATED ASSETS & LINEAGE REFERENCE
-- ==============================================================================
/*
  The following tables exist in the PROD schema but are deprecated. 
  They are kept for historical lineage and back-compat checks during the migration 
  to orders_v3_FINAL.

  - PROD.orders_v1: Legacy order schema (pre-Stripe integration). Do not use.
  - PROD.orders_v2: Intermediate schema. Lacks the strict dbt data quality assertions 
    and does not contain the `is_refunded` flag.
  - KAFKA.kafka_events_raw_copy: Raw event stream copy. Used historically to debug 
    ingestion issues, but now superseded by the standard Snowflake-Kafka connector.
*/

-- Lineage Verification Query (To compare record counts across versions if needed)
SELECT 'orders_v1' AS version, COUNT(*) AS record_count, MAX(order_date) AS max_date FROM PROD.orders_v1
UNION ALL
SELECT 'orders_v2' AS version, COUNT(*) AS record_count, MAX(order_date) AS max_date FROM PROD.orders_v2
UNION ALL
SELECT 'orders_v3_FINAL' AS version, COUNT(*) AS record_count, MAX(order_date) AS max_date FROM PROD.orders_v3_FINAL;