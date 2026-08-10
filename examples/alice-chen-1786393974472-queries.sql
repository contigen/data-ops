-- ==============================================================================
-- HANDOVER REFERENCE SCRIPT: ALICE CHEN'S DATA ASSETS
-- Target Audience: Successor Engineer taking over Alice Chen's portfolio
-- Date: October 2023
-- 
-- This script serves as an operational runbook, schema reference, and query 
-- playbook for the Snowflake, Airflow, Looker, and Kafka assets previously 
-- maintained by Alice Chen (alice.chen).
-- ==============================================================================

-- ==============================================================================
-- SECTION 1: ASSET DIRECTORY & METADATA
-- ==============================================================================
-- Below is the mapping of all assets covered in this handover:
-- 
-- 1. Snowflake: PROD.orders_v3_FINAL (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD))
--    - Status: ACTIVE (Production Core)
--    - SLA: Daily 6:00 AM UTC
--    - Quality: dbt verified (not_null, accepted_values on order_status)
-- 
-- 2. Snowflake: PROD.dim_customers (urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD))
--    - Status: ACTIVE (Production Core)
-- 
-- 3. Snowflake: PROD.quarterly_board_metrics (urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD))
--    - Status: ACTIVE (Reporting)
-- 
-- 4. Snowflake: PROD.churn_prediction_features (urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD))
--    - Status: ACTIVE (ML Feature Store)
-- 
-- 5. Airflow: daily_revenue_etl (urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD))
--    - Status: ACTIVE (Orchestration DAG)
-- 
-- 6. Looker: revenue_dashboard_BACKUP (urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD))
--    - Status: BACKUP (Ad-hoc / Reference)
-- 
-- 7. Kafka: kafka_events_raw_copy (urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD))
--    - Status: INGESTION (Raw Stream Copy)
-- 
-- 8. Snowflake: PROD.orders_v2 (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD))
--    - Status: DEPRECATED (Do not use for new development)
-- 
-- 9. Snowflake: PROD.orders_v1 (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD))
--    - Status: DEPRECATED (Do not use for new development)
-- ==============================================================================


-- ==============================================================================
-- SECTION 2: OPERATIONAL RUNBOOK & EDGE CASE MITIGATION
-- ==============================================================================
-- ASSET: orders_v3_FINAL
-- 
-- CRITICAL KNOWN EDGE CASE:
-- Canceled orders with a $0 subtotal and a positive shipping cost will break 
-- the downstream `daily_revenue_etl` pipeline (causing negative margin calculations) 
-- if the `is_refunded` flag is not set to `true` within a 24-hour window.
-- 
-- RECOVERY PROCEDURE:
-- If the Stripe webhook ingestion API fails to sync the `is_refunded` flag:
-- 
--   1. Run the reconciliation script from the `data-ops` repository:
--      python stripe_reconciliation_catchup.py --order-ids <comma_separated_ids>
--      
--      *Note: For batches > 500 records, append `--batch-size 100` to prevent 
--       Stripe API rate-limiting and timeouts:
--      python stripe_reconciliation_catchup.py --file-path /path/to/ids.csv --batch-size 100
-- 
--   2. Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG 
--      for the affected execution date to recalculate downstream Looker dashboards.
-- ==============================================================================

-- AUDIT QUERY: Detect the "Zero-Dollar Subtotal / Positive Shipping Cost" Edge Case
-- Run this query to identify records that will break the downstream ETL if not fixed.
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
  AND (is_refunded IS NULL OR is_refunded = FALSE)
  -- Focus on the critical 24-hour SLA window
  AND order_date >= DATEADD('day', -2, CURRENT_DATE())
ORDER BY order_date DESC;


-- ==============================================================================
-- SECTION 3: REFERENCE PIPELINE & INTEGRATION QUERIES
-- ==============================================================================

-- QUERY 1: Daily Revenue ETL Simulation (Safe Margin Calculation)
-- This query demonstrates how `orders_v3_FINAL` joins with `dim_customers` 
-- to generate clean daily revenue metrics, safely bypassing the edge case 
-- if the Stripe webhook recovery is still pending.
SELECT 
    o.order_date,
    c.customer_segment,
    c.country,
    COUNT(DISTINCT o.order_id) AS total_orders,
    
    -- Safe subtotal calculation to prevent negative margin downstream
    SUM(
        CASE 
            WHEN o.subtotal = 0 AND o.shipping_cost > 0 AND (o.is_refunded IS NULL OR o.is_refunded = FALSE) 
            THEN 0 
            ELSE o.subtotal 
        END
    ) AS clean_subtotal,
    
    SUM(o.shipping_cost) AS total_shipping_cost,
    
    -- Total Revenue
    SUM(
        CASE 
            WHEN o.subtotal = 0 AND o.shipping_cost > 0 AND (o.is_refunded IS NULL OR o.is_refunded = FALSE) 
            THEN 0 
            ELSE o.subtotal 
        END + o.shipping_cost
    ) AS gross_revenue
FROM PROD.orders_v3_FINAL o
INNER JOIN PROD.dim_customers c 
    ON o.customer_id = c.customer_id
WHERE o.order_status IN ('completed', 'shipped', 'delivered') -- dbt accepted_values
  AND o.order_date >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;


-- QUERY 2: Quarterly Board Metrics Generation
-- Demonstrates how `orders_v3_FINAL` aggregates into `quarterly_board_metrics`.
-- This query is used to populate the quarterly board report slides.
CREATE OR REPLACE TABLE PROD.quarterly_board_metrics AS
SELECT 
    DATE_TRUNC('quarter', o.order_date) AS fiscal_quarter,
    c.customer_segment,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.subtotal), 2) AS total_product_revenue,
    ROUND(SUM(o.shipping_cost), 2) AS total_shipping_revenue,
    ROUND(SUM(o.subtotal) / COUNT(DISTINCT o.order_id), 2) AS average_order_value,
    -- Refund Rate Analysis
    ROUND(
        COUNT(CASE WHEN o.is_refunded = TRUE THEN 1 END) * 100.0 / COUNT(DISTINCT o.order_id), 
        2
    ) AS refund_rate_percentage
FROM PROD.orders_v3_FINAL o
INNER JOIN PROD.dim_customers c 
    ON o.customer_id = c.customer_id
WHERE o.order_status != 'cancelled'
GROUP BY 1, 2
ORDER BY 1 DESC, 2 ASC;


-- QUERY 3: Churn Prediction Feature Engineering
-- Demonstrates how `orders_v3_FINAL` and `dim_customers` feed into 
-- `churn_prediction_features` for downstream ML models.
CREATE OR REPLACE TABLE PROD.churn_prediction_features AS
WITH customer_order_stats AS (
    SELECT 
        customer_id,
        MAX(order_date) AS last_order_date,
        MIN(order_date) AS first_order_date,
        COUNT(DISTINCT order_id) AS lifetime_orders,
        SUM(subtotal) AS lifetime_spend,
        AVG(subtotal) AS average_order_spend,
        DATEDIFF('day', MAX(order_date), CURRENT_DATE()) AS days_since_last_order
    FROM PROD.orders_v3_FINAL
    WHERE order_status = 'completed'
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.customer_segment,
    c.signup_date,
    DATEDIFF('day', c.signup_date, CURRENT_DATE()) AS customer_tenure_days,
    COALESCE(s.lifetime_orders, 0) AS lifetime_orders,
    COALESCE(s.lifetime_spend, 0.0) AS lifetime_spend,
    COALESCE(s.average_order_spend, 0.0) AS average_order_spend,
    COALESCE(s.days_since_last_order, 9999) AS recency_days,
    -- Churn Label (e.g., no order in last 90 days)
    CASE 
        WHEN COALESCE(s.days_since_last_order, 9999) > 90 THEN 1 
        ELSE 0 
    END AS is_churned
FROM PROD.dim_customers c
LEFT JOIN customer_order_stats s 
    ON c.customer_id = s.customer_id;


-- ==============================================================================
-- SECTION 4: DEPRECATION & HISTORICAL AUDIT
-- ==============================================================================
-- The following query is a sanity check to compare the record counts and schema 
-- drift across orders_v1, orders_v2, and orders_v3_FINAL. 
-- Use this to verify historical data consistency if migrating legacy systems.

SELECT 
    'orders_v1' AS table_version, 
    COUNT(*) AS total_records, 
    MIN(order_date) AS min_date, 
    MAX(order_date) AS max_date,
    'N/A' AS has_refund_flag
FROM PROD.orders_v1

UNION ALL

SELECT 
    'orders_v2' AS table_version, 
    COUNT(*) AS total_records, 
    MIN(order_date) AS min_date, 
    MAX(order_date) AS max_date,
    'N/A' AS has_refund_flag
FROM PROD.orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL' AS table_version, 
    COUNT(*) AS total_records, 
    MIN(order_date) AS min_date, 
    MAX(order_date) AS max_date,
    'YES' AS has_refund_flag
FROM PROD.orders_v3_FINAL;


-- ==============================================================================
-- SECTION 5: KAFKA INGESTION AUDIT
-- ==============================================================================
-- ASSET: kafka_events_raw_copy
-- 
-- This query checks the ingestion rate and latency of the raw Kafka event copy 
-- table to ensure the streaming pipeline is healthy.
SELECT 
    DATE_TRUNC('hour', record_metadata:CreateTime::timestamp) AS event_hour,
    COUNT(*) AS event_count,
    AVG(DATEDIFF('second', record_metadata:CreateTime::timestamp, ingestion_time)) AS avg_ingestion_latency_seconds
FROM PROD.kafka_events_raw_copy
WHERE ingestion_time >= DATEADD('day', -3, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;