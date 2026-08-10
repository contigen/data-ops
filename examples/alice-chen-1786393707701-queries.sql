-- =================================================================================
-- DATA ASSET HANDOFF REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Target Successor: Data Engineering Team
-- Date: October 2023
-- =================================================================================

-- =================================================================================
-- SECTION 1: OPERATIONAL DOCUMENTATION & RUNBOOKS (READ ME)
-- =================================================================================
/*
   ASSET: orders_v3_FINAL (Snowflake Dataset)
   URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)
   
   OVERVIEW:
   - This is the production orders dataset updated daily.
   - SLA: Must be populated and finalized by 6:00 AM UTC daily to feed the executive dashboard.
   - Quality: The "FINAL" suffix indicates successful completion of dbt data quality assertions:
     * `not_null` on critical keys (e.g., order_id, customer_id).
     * `accepted_values` on `order_status` ('PENDING', 'COMPLETED', 'CANCELED', 'REFUNDED').

   KNOWN EDGE CASES & DOWNSTREAM IMPACT:
   - Zero-Dollar Subtotal / Positive Shipping Cost:
     Canceled orders with a $0 subtotal and a positive shipping cost will break the downstream 
     `daily_revenue_etl` pipeline (causing negative margin calculations) if the `is_refunded` 
     flag is not set to `true` within a 24-hour window.

   OPERATIONAL RECOVERY PROCEDURE:
   The `is_refunded` flag is populated via a Stripe webhook ingestion API. If the webhook 
   fails to sync within the 24-hour window, execute the following recovery steps:
   
   1. Run the Reconciliation Script:
      Execute `stripe_reconciliation_catchup.py` from the `data-ops` repository.
      - Single/Bulk Execution: Pass target IDs via `--order-ids` (comma-separated) or `--file-path` (CSV path).
      - Rate Limiting: For batches exceeding 500 records, append the `--batch-size 100` flag 
        to prevent Stripe API rate-limiting and script timeouts.
        Example: `python stripe_reconciliation_catchup.py --file-path failed_orders.csv --batch-size 100`
        
   2. Downstream Backfill:
      Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG for the affected 
      execution date to recalculate the downstream Looker dashboard (`revenue_dashboard_BACKUP`).
*/

-- =================================================================================
-- SECTION 2: DIAGNOSTIC & MONITORING QUERIES
-- =================================================================================

-- Query 2.1: SLA & Freshness Verification
-- Use this query to verify if the daily load completed before the 6:00 AM UTC SLA.
SELECT 
    MAX(created_at) AS latest_order_timestamp,
    CURRENT_TIMESTAMP() AS current_system_time,
    CASE 
        WHEN MAX(created_at) >= DATEADD('hour', -6, CURRENT_TIMESTAMP()) THEN 'SLA MET'
        ELSE 'SLA BREACHED / DELAYED'
    END AS sla_status
FROM orders_v3_FINAL;


-- Query 2.2: Edge Case Detection (Zero-Dollar Subtotal with Positive Shipping)
-- Run this query to identify records that will break the downstream `daily_revenue_etl` pipeline.
-- If this query returns rows where `is_refunded` is FALSE and the order is older than 24 hours,
-- you must trigger the Stripe Reconciliation Catchup script immediately.
SELECT 
    order_id,
    customer_id,
    subtotal,
    shipping_cost,
    is_refunded,
    order_status,
    created_at,
    DATEDIFF('hour', created_at, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM orders_v3_FINAL
WHERE subtotal = 0 
  AND shipping_cost > 0 
  AND is_refunded = FALSE
  AND order_status = 'CANCELED'
ORDER BY created_at DESC;


-- Query 2.3: Data Quality Assertion Checks (Simulating dbt tests)
-- Run this to manually verify the integrity of the orders_v3_FINAL table.
SELECT
    -- Check for nulls in primary key
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    -- Check for invalid order statuses
    SUM(CASE WHEN order_status NOT IN ('PENDING', 'COMPLETED', 'CANCELED', 'REFUNDED') THEN 1 ELSE 0 END) AS invalid_status_count,
    -- Check for negative financial values
    SUM(CASE WHEN subtotal < 0 OR shipping_cost < 0 THEN 1 ELSE 0 END) AS negative_financial_records
FROM orders_v3_FINAL;


-- =================================================================================
-- SECTION 3: DOWNSTREAM PIPELINES & INTEGRATION EXAMPLES
-- =================================================================================

-- Query 3.1: Downstream Simulation - daily_revenue_etl
-- This query demonstrates how the daily revenue is calculated. 
-- Note how the logic handles the edge case by zeroing out revenue if the order is refunded.
-- If `is_refunded` is FALSE for a $0 subtotal / positive shipping order, it distorts the margin.
CREATE OR REPLACE TEMPORARY TABLE temp_daily_revenue_summary AS
SELECT
    CAST(created_at AS DATE) AS revenue_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE subtotal END) AS net_subtotal,
    SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE shipping_cost END) AS net_shipping,
    -- If is_refunded is false but subtotal is 0 and shipping is positive, this calculation is affected:
    SUM(CASE WHEN is_refunded = TRUE THEN 0 ELSE (subtotal + shipping_cost) END) AS total_net_revenue
FROM orders_v3_FINAL
WHERE order_status != 'PENDING'
GROUP BY 1
ORDER BY 1 DESC;

SELECT * FROM temp_daily_revenue_summary LIMIT 10;


-- Query 3.2: Downstream Simulation - quarterly_board_metrics
-- Demonstrates how orders_v3_FINAL joins with dim_customers to produce board-level metrics.
SELECT
    DATE_TRUNC('quarter', o.created_at) AS board_quarter,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(CASE WHEN o.is_refunded = FALSE THEN o.subtotal ELSE 0 END) AS gross_merchandise_value,
    AVG(CASE WHEN o.is_refunded = FALSE THEN o.subtotal ELSE NULL END) AS average_order_value
FROM orders_v3_FINAL o
JOIN dim_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'COMPLETED'
GROUP BY 1
ORDER BY 1 DESC;


-- Query 3.3: Downstream Simulation - churn_prediction_features
-- Demonstrates how features are engineered for the machine learning pipeline.
-- This query aggregates customer behavior using dim_customers and orders_v3_FINAL.
CREATE OR REPLACE TABLE churn_prediction_features AS
SELECT
    c.customer_id,
    c.signup_date,
    DATEDIFF('day', c.signup_date, CURRENT_DATE()) AS customer_tenure_days,
    COUNT(o.order_id) AS total_orders_placed,
    COALESCE(SUM(CASE WHEN o.is_refunded = FALSE THEN o.subtotal ELSE 0 END), 0) AS total_lifetime_spend,
    COALESCE(MAX(o.created_at), c.signup_date) AS last_order_timestamp,
    DATEDIFF('day', COALESCE(MAX(o.created_at), c.signup_date), CURRENT_DATE()) AS days_since_last_order,
    -- Flag indicating if they have ordered in the last 90 days
    CASE WHEN DATEDIFF('day', COALESCE(MAX(o.created_at), c.signup_date), CURRENT_DATE()) <= 90 THEN 0 ELSE 1 END AS is_churned_90d
FROM dim_customers c
LEFT JOIN orders_v3_FINAL o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.signup_date;

SELECT * FROM churn_prediction_features LIMIT 10;


-- =================================================================================
-- SECTION 4: LEGACY MIGRATION & LINEAGE REFERENCE
-- =================================================================================

-- Query 4.1: Lineage Reconciliation (orders_v1 vs orders_v2 vs orders_v3_FINAL)
-- Use this query to verify historical consistency across deprecated schemas if doing historical audits.
-- Note: orders_v1 and orders_v2 are deprecated but retained for historical audit purposes.
SELECT
    'orders_v1' AS source_version,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date
FROM orders_v1

UNION ALL

SELECT
    'orders_v2' AS source_version,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date
FROM orders_v2

UNION ALL

SELECT
    'orders_v3_FINAL' AS source_version,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date
FROM orders_v3_FINAL;


-- Query 4.2: Kafka Raw Event Ingestion Audit
-- If you suspect the Stripe webhook or the order creation pipeline is lagging, 
-- you can query the raw Kafka events copy to see if events are landing in the raw layer.
-- This helps isolate issues between Kafka ingestion and the Snowflake dbt pipeline.
SELECT
    -- Assuming standard Kafka metadata fields
    record_metadata:partition AS kafka_partition,
    record_metadata:offset AS kafka_offset,
    record_content:order_id::VARCHAR AS order_id,
    record_content:event_type::VARCHAR AS event_type,
    record_content:timestamp::TIMESTAMP AS event_timestamp,
    inserted_at AS snowflake_ingestion_timestamp
FROM kafka_events_raw_copy
WHERE record_content:event_type::VARCHAR IN ('order_created', 'order_refunded')
ORDER BY inserted_at DESC
LIMIT 100;