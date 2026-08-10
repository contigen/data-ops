-- ==============================================================================
-- HANDOFF REFERENCE SQL SCRIPT
-- DEPARTING ENGINEER: alice.chen
-- PREPARED BY: Ghost
-- SUBJECT: Core Data Assets & Pipeline Recovery Runbook
-- ==============================================================================

-- ==============================================================================
-- SECTION 1: ASSET OVERVIEW & OPERATIONAL RUNBOOKS (READ-ME)
-- ==============================================================================
/*
This script serves as the technical handoff and operational runbook for the data 
assets previously maintained by Alice Chen. It contains schema layouts, diagnostic 
queries, ETL logic, and recovery procedures.

ASSET DIRECTORY & STATUS:
1. orders_v3_FINAL (Snowflake - PROD) - ACTIVE (Core Production Orders Dataset)
   - SLA: Daily update by 6:00 AM UTC for Executive Dashboard.
   - Quality: Passed dbt assertions (not_null, accepted_values on order_status).
2. dim_customers (Snowflake - PROD) - ACTIVE (Customer Dimension)
3. quarterly_board_metrics (Snowflake - PROD) - ACTIVE (Board Reporting)
4. churn_prediction_features (Snowflake - PROD) - ACTIVE (ML Feature Store)
5. daily_revenue_etl (Airflow - PROD) - ACTIVE (Downstream ETL Pipeline)
6. revenue_dashboard_BACKUP (Looker - PROD) - ACTIVE (Backup Dashboard)
7. kafka_events_raw_copy (Kafka - PROD) - ACTIVE (Raw Event Stream Copy)
8. orders_v2 (Snowflake - PROD) - DEPRECATED (Do not use for new development)
9. orders_v1 (Snowflake - PROD) - DEPRECATED (Do not use for new development)

--------------------------------------------------------------------------------
CRITICAL OPERATIONAL ALERT: THE "ZERO-DOLLAR" EDGE CASE
--------------------------------------------------------------------------------
* Symptom: 
  Canceled orders with a $0.00 subtotal but a positive shipping cost (> $0.00) 
  will break the downstream `daily_revenue_etl` pipeline, causing negative margin 
  calculations and dashboard failures.
* Root Cause: 
  This occurs if the `is_refunded` flag is NOT set to 'true' within a 24-hour window 
  for these specific canceled orders.
* Recovery Procedure:
  If the Stripe webhook ingestion API fails to sync this flag within 24 hours:
  
  1. Run the reconciliation script from the `data-ops` repository:
     python stripe_reconciliation_catchup.py --order-ids <comma_separated_ids>
     
     *Note: For batches > 500 records, append the rate-limiting flag:
     python stripe_reconciliation_catchup.py --file-path <path_to_csv> --batch-size 100
     
  2. Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG 
     for the affected execution date to recalculate downstream metrics.
*/

-- ==============================================================================
-- SECTION 2: DIAGNOSTIC & MONITORING QUERIES
-- ==============================================================================

-- QUERY 2.1: Edge Case Detection Query
-- Run this query to identify orders that will break the downstream ETL pipeline.
-- If any rows are returned, initiate the Stripe Reconciliation Recovery Procedure.
SELECT 
    order_id,
    customer_id,
    order_date,
    order_status,
    subtotal,
    shipping_cost,
    is_refunded,
    (subtotal + shipping_cost) AS total_charged
FROM PROD.orders_v3_FINAL
WHERE 
    subtotal = 0.00 
    AND shipping_cost > 0.00 
    AND order_status = 'CANCELED'
    AND is_refunded = FALSE
    AND order_date >= CURRENT_DATE() - INTERVAL '2 DAY';


-- QUERY 2.2: dbt Data Quality Assertion Emulation
-- Emulates the dbt tests run on orders_v3_FINAL to ensure data integrity.
SELECT
    -- Test 1: Not Null Assertions
    COUNT(CASE WHEN order_id IS NULL THEN 1 END) AS null_order_ids,
    COUNT(CASE WHEN customer_id IS NULL THEN 1 END) AS null_customer_ids,
    
    -- Test 2: Accepted Values Assertion on order_status
    COUNT(CASE WHEN order_status NOT IN ('PLACED', 'SHIPPED', 'DELIVERED', 'CANCELED', 'RETURNED') THEN 1 END) AS invalid_status_count,
    
    -- Test 3: Primary Key Uniqueness
    COUNT(order_id) - COUNT(DISTINCT order_id) AS duplicate_order_ids
FROM PROD.orders_v3_FINAL;


-- ==============================================================================
-- SECTION 3: PIPELINE SIMULATION & DOWNSTREAM INTEGRATION
-- ==============================================================================

-- QUERY 3.1: daily_revenue_etl (Airflow Pipeline Logic)
-- This query simulates the core logic of the daily_revenue_etl pipeline.
-- It aggregates daily financial metrics and handles the zero-dollar edge case safely.
WITH daily_metrics AS (
    SELECT 
        order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN order_status = 'CANCELED' THEN order_id END) AS canceled_orders,
        
        -- Safe Revenue Calculation: Excludes refunded/canceled edge cases to prevent negative margins
        SUM(
            CASE 
                WHEN is_refunded = TRUE THEN 0.00
                WHEN order_status = 'CANCELED' AND subtotal = 0.00 THEN 0.00
                ELSE subtotal 
            END
        ) AS gross_revenue,
        
        SUM(
            CASE 
                WHEN is_refunded = TRUE THEN 0.00
                WHEN order_status = 'CANCELED' AND subtotal = 0.00 THEN 0.00
                ELSE shipping_cost 
            END
        ) AS shipping_revenue,
        
        -- Estimated Cost of Goods Sold (COGS)
        SUM(CASE WHEN is_refunded = TRUE THEN 0.00 ELSE estimated_cost END) AS total_cogs
    FROM PROD.orders_v3_FINAL
    WHERE order_date >= CURRENT_DATE() - INTERVAL '30 DAY'
    GROUP BY order_date
)
SELECT 
    order_date,
    total_orders,
    canceled_orders,
    gross_revenue,
    shipping_revenue,
    (gross_revenue + shipping_revenue) AS total_revenue,
    total_cogs,
    -- Margin calculation that would break (go negative/skewed) without the safe revenue logic above
    ((gross_revenue + shipping_revenue) - total_cogs) AS net_margin,
    ROUND(((gross_revenue + shipping_revenue - total_cogs) / NULLIF(gross_revenue + shipping_revenue, 0)) * 100, 2) AS margin_percentage
FROM daily_metrics
ORDER BY order_date DESC;


-- QUERY 3.2: quarterly_board_metrics Generation
-- Demonstrates how orders_v3_FINAL rolls up into the quarterly board reporting table.
-- Joins with dim_customers to segment performance by customer cohort.
INSERT INTO PROD.quarterly_board_metrics (
    fiscal_quarter,
    customer_segment,
    total_active_customers,
    total_orders,
    total_revenue,
    average_order_value
)
SELECT 
    'Q' || EXTRACT(QUARTER FROM o.order_date) || '-' || EXTRACT(YEAR FROM o.order_date) AS fiscal_quarter,
    c.customer_segment,
    COUNT(DISTINCT o.customer_id) AS total_active_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(CASE WHEN o.is_refunded = TRUE THEN 0.00 ELSE o.subtotal END) AS total_revenue,
    ROUND(AVG(CASE WHEN o.is_refunded = TRUE THEN NULL ELSE o.subtotal END), 2) AS average_order_value
FROM PROD.orders_v3_FINAL o
INNER JOIN PROD.dim_customers c 
    ON o.customer_id = c.customer_id
WHERE o.order_date >= DATE_TRUNC('YEAR', CURRENT_DATE()) -- Current Fiscal Year
GROUP BY 1, 2;


-- QUERY 3.3: churn_prediction_features Generation
-- Demonstrates how orders_v3_FINAL and dim_customers feed the ML feature store.
-- This query calculates behavioral features used by the churn prediction model.
CREATE OR REPLACE TABLE PROD.churn_prediction_features AS
SELECT 
    c.customer_id,
    c.customer_segment,
    c.signup_date,
    DATEDIFF('day', c.signup_date, CURRENT_DATE()) AS customer_tenure_days,
    
    -- Order Frequency & Recency
    COUNT(DISTINCT o.order_id) AS lifetime_orders,
    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) AS days_since_last_order,
    
    -- Financial Metrics
    SUM(CASE WHEN o.is_refunded = TRUE THEN 0.00 ELSE o.subtotal END) AS lifetime_spend,
    ROUND(AVG(CASE WHEN o.is_refunded = TRUE THEN NULL ELSE o.subtotal END), 2) AS avg_order_value,
    
    -- Refund & Cancellation Behavior (Risk Indicators)
    COUNT(DISTINCT CASE WHEN o.is_refunded = TRUE THEN o.order_id END) AS total_refunded_orders,
    COUNT(DISTINCT CASE WHEN o.order_status = 'CANCELED' THEN o.order_id END) AS total_canceled_orders,
    
    -- Ratio of refunds to total orders
    ROUND(
        COUNT(DISTINCT CASE WHEN o.is_refunded = TRUE THEN o.order_id END) / 
        NULLIF(COUNT(DISTINCT o.order_id), 0)::FLOAT, 4
    ) AS refund_ratio
FROM PROD.dim_customers c
LEFT JOIN PROD.orders_v3_FINAL o 
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_segment, c.signup_date;


-- ==============================================================================
-- SECTION 4: REAL-TIME EVENT AUDITING (KAFKA INTEGRATION)
-- ==============================================================================

-- QUERY 4.1: Kafka Raw Event Reconciliation
-- Used to audit the raw ingestion layer (kafka_events_raw_copy) against the 
-- finalized Snowflake table (orders_v3_FINAL) to debug ingestion delays or webhook drops.
WITH raw_kafka_parsed AS (
    SELECT 
        -- Assuming JSON payload in Kafka event stream
        PARSE_JSON(event_payload):order_id::VARCHAR AS order_id,
        PARSE_JSON(event_payload):event_type::VARCHAR AS event_type,
        PARSE_JSON(event_payload):timestamp::TIMESTAMP AS event_timestamp,
        PARSE_JSON(event_payload):subtotal::DECIMAL(18,2) AS subtotal,
        PARSE_JSON(event_payload):shipping_cost::DECIMAL(18,2) AS shipping_cost
    FROM PROD.kafka_events_raw_copy
    WHERE event_timestamp >= CURRENT_DATE() - INTERVAL '3 DAY'
)
SELECT 
    k.order_id,
    k.event_type,
    k.event_timestamp,
    o.order_id AS snowflake_order_id,
    o.order_status AS snowflake_status,
    o.is_refunded AS snowflake_refund_flag,
    CASE 
        WHEN o.order_id IS NULL THEN 'MISSING_IN_SNOWFLAKE'
        WHEN k.event_type = 'ORDER_REFUNDED' AND o.is_refunded = FALSE THEN 'REFUND_NOT_PROPAGATED'
        ELSE 'SYNCHRONIZED'
    END AS sync_status
FROM raw_kafka_parsed k
LEFT JOIN PROD.orders_v3_FINAL o 
    ON k.order_id = o.order_id
ORDER BY k.event_timestamp DESC;


-- ==============================================================================
-- SECTION 5: DEPRECATED ASSET AUDIT (FOR CLEANUP PLANNING)
-- ==============================================================================

-- QUERY 5.1: Deprecated Schema Comparison (orders_v1 vs orders_v2 vs orders_v3_FINAL)
-- Run this query to verify that no legacy pipelines are still querying v1 or v2.
-- If queries are detected in the Snowflake query history for v1/v2, migrate them to v3_FINAL.
SELECT 
    'orders_v1' AS table_version,
    COUNT(*) AS row_count,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
FROM PROD.orders_v1

UNION ALL

SELECT 
    'orders_v2' AS table_version,
    COUNT(*) AS row_count,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
FROM PROD.orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL' AS table_version,
    COUNT(*) AS row_count,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
FROM PROD.orders_v3_FINAL;