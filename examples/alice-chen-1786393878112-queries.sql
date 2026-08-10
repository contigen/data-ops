-- =================================================================================
-- DATA OPS HANDOVER & REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Purpose: Operational continuity, recovery procedures, and asset integration
-- =================================================================================

-- =================================================================================
-- ASSET DIRECTORY & METADATA
-- =================================================================================
-- 1. Snowflake: PROD.orders_v3_FINAL
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)
--    SLA: Daily 6:00 AM UTC (Populates Executive Dashboard)
--    Quality: Passed dbt tests (not_null, accepted_values on order_status)
--
-- 2. Looker: revenue_dashboard_BACKUP
--    URN: urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD)
--
-- 3. Kafka: kafka_events_raw_copy
--    URN: urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD)
--
-- 4. Snowflake: PROD.orders_v2
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD)
--
-- 5. Snowflake: PROD.quarterly_board_metrics
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD)
--
-- 6. Airflow: daily_revenue_etl
--    URN: urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD)
--
-- 7. Snowflake: PROD.churn_prediction_features
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD)
--
-- 8. Snowflake: PROD.dim_customers
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD)
--
-- 9. Snowflake: PROD.orders_v1
--    URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD)
-- =================================================================================


-- =================================================================================
-- SECTION 1: OPERATIONAL RECOVERY & EDGE CASE DETECTION
-- =================================================================================

-- DETECTING THE CRITICAL EDGE CASE:
-- Canceled orders with $0 subtotal and positive shipping cost that lack the 
-- 'is_refunded = TRUE' flag. If left unfixed for >24 hours, this breaks the 
-- downstream 'daily_revenue_etl' pipeline due to negative margin calculations.

SELECT 
    order_id,
    customer_id,
    order_date,
    order_status,
    subtotal_amount,
    shipping_amount,
    is_refunded,
    DATEDIFF('hour', order_date, CURRENT_TIMESTAMP()) AS hours_since_order
FROM 
    PROD.orders_v3_FINAL
WHERE 
    order_status = 'CANCELED'
    AND subtotal_amount = 0.00
    AND shipping_amount > 0.00
    AND (is_refunded IS NULL OR is_refunded = FALSE)
ORDER BY 
    order_date DESC;

/*
=================================================================================
OPERATIONAL RECOVERY PROCEDURE (IF WEBHOOK FAILS TO SYNC WITHIN 24 HOURS):
=================================================================================
If the query above returns records, the Stripe webhook ingestion API has failed.
Follow these steps to recover:

1. RECONCILIATION SCRIPT:
   Navigate to the `data-ops` repository and run the catchup script.
   
   * For single/bulk execution with specific IDs:
     python stripe_reconciliation_catchup.py --order-ids "12345,67890"
     
   * For larger batches (exceeding 500 records), use a CSV file and append 
     the rate-limiting flag to prevent Stripe API timeouts:
     python stripe_reconciliation_catchup.py --file-path "/path/to/failed_orders.csv" --batch-size 100

2. DOWNSTREAM BACKFILL:
   Once the script completes and 'is_refunded' is updated to TRUE in Snowflake,
   manually trigger an Airflow backfill on the 'daily_revenue_etl' DAG for the 
   affected execution dates to recalculate the Looker dashboard metrics:
   
   airflow dags backfill -s YYYY-MM-DD -e YYYY-MM-DD daily_revenue_etl
=================================================================================
*/


-- =================================================================================
-- SECTION 2: PIPELINE SIMULATION (daily_revenue_etl)
-- =================================================================================

-- This query simulates the logic of the 'daily_revenue_etl' pipeline.
-- It aggregates daily financial metrics from orders_v3_FINAL, handling the 
-- edge case safely to prevent negative margin anomalies.

CREATE OR REPLACE TABLE PROD.daily_revenue_etl_SIMULATION AS
SELECT 
    CAST(order_date AS DATE) AS revenue_date,
    COUNT(DISTINCT order_id) AS total_orders,
    
    -- Gross Revenue: Subtotal + Shipping (excluding refunded orders)
    SUM(
        CASE 
            WHEN is_refunded = TRUE THEN 0 
            ELSE (subtotal_amount + shipping_amount) 
        END
    ) AS gross_revenue,
    
    -- Safe Margin Calculation: Prevents negative margin on canceled zero-dollar subtotal orders
    SUM(
        CASE 
            WHEN order_status = 'CANCELED' AND subtotal_amount = 0.00 AND is_refunded = FALSE 
                THEN 0 -- Force safety override if webhook hasn't run yet
            WHEN is_refunded = TRUE 
                THEN 0
            ELSE (subtotal_amount - (shipping_amount * 0.15)) -- Assuming 15% fulfillment cost on shipping
        END
    ) AS net_margin,
    
    -- Track count of un-refunded anomalies for data quality monitoring
    SUM(
        CASE 
            WHEN order_status = 'CANCELED' AND subtotal_amount = 0.00 AND shipping_amount > 0.00 AND (is_refunded IS NULL OR is_refunded = FALSE) 
                THEN 1 
            ELSE 0 
        END
    ) AS active_anomaly_count
FROM 
    PROD.orders_v3_FINAL
GROUP BY 
    1
ORDER BY 
    1 DESC;


-- =================================================================================
-- SECTION 3: DOWNSTREAM CONSUMPTION - QUARTERLY BOARD METRICS
-- =================================================================================

-- This query demonstrates how orders_v3_FINAL and dim_customers are aggregated
-- to populate the quarterly_board_metrics table.

INSERT INTO PROD.quarterly_board_metrics (
    fiscal_quarter,
    fiscal_year,
    total_active_customers,
    total_revenue,
    average_order_value,
    customer_acquisition_cost_est
)
WITH quarterly_aggregates AS (
    SELECT 
        CONCAT('Q', QUARTER(o.order_date)) AS f_quarter,
        YEAR(o.order_date) AS f_year,
        COUNT(DISTINCT o.customer_id) AS active_cust,
        SUM(o.subtotal_amount) AS rev,
        AVG(o.subtotal_amount) AS aov
    FROM 
        PROD.orders_v3_FINAL o
    INNER JOIN 
        PROD.dim_customers c ON o.customer_id = c.customer_id
    WHERE 
        o.order_status IN ('COMPLETED', 'SHIPPED') -- Exclude canceled/refunded orders
        AND o.is_refunded = FALSE
    GROUP BY 
        1, 2
)
SELECT 
    f_quarter,
    f_year,
    active_cust,
    rev,
    aov,
    -- Placeholder calculation for CAC estimation
    (rev * 0.12) / NULLIF(active_cust, 0) AS customer_acquisition_cost_est
FROM 
    quarterly_aggregates;


-- =================================================================================
-- SECTION 4: DOWNSTREAM CONSUMPTION - CHURN PREDICTION FEATURES
-- =================================================================================

-- This query generates features for the churn_prediction_features table,
-- combining customer profile data with historical order patterns from orders_v3_FINAL.

CREATE OR REPLACE TABLE PROD.churn_prediction_features AS
WITH customer_order_history AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS total_orders_lifetime,
        MAX(order_date) AS most_recent_order_date,
        MIN(order_date) AS first_order_date,
        SUM(subtotal_amount) AS lifetime_spend,
        AVG(subtotal_amount) AS average_order_value,
        SUM(CASE WHEN is_refunded = TRUE THEN 1 ELSE 0 END) AS total_refunded_orders
    FROM 
        PROD.orders_v3_FINAL
    GROUP BY 
        customer_id
)
SELECT 
    c.customer_id,
    c.customer_segment,
    c.signup_date,
    DATEDIFF('day', c.signup_date, CURRENT_DATE()) AS customer_tenure_days,
    COALESCE(h.total_orders_lifetime, 0) AS total_orders_lifetime,
    COALESCE(h.lifetime_spend, 0.00) AS lifetime_spend,
    COALESCE(h.average_order_value, 0.00) AS average_order_value,
    COALESCE(h.total_refunded_orders, 0) AS total_refunded_orders,
    DATEDIFF('day', h.most_recent_order_date, CURRENT_DATE()) AS days_since_last_purchase,
    CASE 
        WHEN DATEDIFF('day', h.most_recent_order_date, CURRENT_DATE()) > 90 THEN TRUE 
        ELSE FALSE 
    END AS is_churned_90d
FROM 
    PROD.dim_customers c
LEFT JOIN 
    customer_order_history h ON c.customer_id = h.customer_id;


-- =================================================================================
-- SECTION 5: HISTORICAL LINEAGE & SCHEMA EVOLUTION
-- =================================================================================

-- Reference query to track schema evolution and record counts across order table versions.
-- Useful for debugging historical discrepancies between v1, v2, and v3_FINAL.

SELECT 
    'orders_v1' AS table_version,
    COUNT(*) AS total_records,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date,
    'Legacy format, no refund flag' AS notes
FROM PROD.orders_v1

UNION ALL

SELECT 
    'orders_v2' AS table_version,
    COUNT(*) AS total_records,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date,
    'Intermediate format, partial refund tracking' AS notes
FROM PROD.orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL' AS table_version,
    COUNT(*) AS total_records,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date,
    'Production standard, strict dbt assertions, 6AM SLA' AS notes
FROM PROD.orders_v3_FINAL;