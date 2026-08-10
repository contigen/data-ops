-- =====================================================================
-- DATA ASSET HANDOVER & OPERATIONAL REFERENCE SCRIPT
-- Departing Engineer: alice.chen
-- Focus Area: Core Orders Pipeline, Revenue Reporting, & Customer Analytics
-- =====================================================================

-- =====================================================================
-- SECTION 1: ASSET INVENTORY & METADATA
-- =====================================================================
/*
1. Asset: orders_v3_FINAL (Dataset)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD)
   - SLA: Daily update by 6:00 AM UTC.
   - Quality: dbt assertions passed (not_null and accepted_values on order_status).
   - Critical Edge Case: Zero-dollar subtotal ($0.00) with positive shipping cost (> $0.00) 
     on canceled orders will break the downstream `daily_revenue_etl` pipeline (causing negative 
     margin calculations) if the `is_refunded` flag is not set to TRUE within 24 hours.

2. Asset: daily_revenue_etl (Airflow DAG / Dataset)
   - URN: urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD)
   - Downstream of: orders_v3_FINAL
   - Upstream of: revenue_dashboard_BACKUP (Looker)

3. Asset: revenue_dashboard_BACKUP (Looker Dashboard)
   - URN: urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD)
   - Backup dashboard for executive revenue tracking.

4. Asset: quarterly_board_metrics (Dataset)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD)
   - Aggregated financial and operational metrics for board reporting.

5. Asset: churn_prediction_features (Dataset)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD)
   - Feature store table for ML churn models, updated weekly.

6. Asset: dim_customers (Dataset)
   - URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD)
   - Customer dimension table containing demographic and account status.

7. Asset: kafka_events_raw_copy (Dataset)
   - URN: urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD)
   - Raw event stream copy used for real-time ingestion debugging.

8. Legacy Assets (Deprecating/Deprecated):
   - orders_v2 (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD))
   - orders_v1 (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD))
*/

-- =====================================================================
-- SECTION 2: OPERATIONAL RECOVERY PROCEDURES (RUNBOOK)
-- =====================================================================
/*
If the Stripe webhook ingestion API fails to sync the `is_refunded` flag within 24 hours:
1. Identify the broken records using QUERY 1 below.
2. Run the reconciliation script from the `data-ops` repository:
   
   # For single/bulk execution with specific IDs:
   python stripe_reconciliation_catchup.py --order-ids "12345,67890"
   
   # For batches exceeding 500 records (to prevent Stripe API rate-limiting):
   python stripe_reconciliation_catchup.py --file-path "/path/to/failed_orders.csv" --batch-size 100

3. Manually trigger an Airflow backfill on the `daily_revenue_etl` DAG for the affected execution dates:
   airflow dags backfill -s YYYY-MM-DD -e YYYY-MM-DD daily_revenue_etl
*/


-- =====================================================================
-- SECTION 3: REFERENCE & MONITORING QUERIES
-- =====================================================================

-- ---------------------------------------------------------------------
-- QUERY 1: SLA & Edge Case Monitoring (Run Daily to Detect ETL Blockers)
-- Target Asset: orders_v3_FINAL
-- Description: Identifies canceled orders with $0 subtotal and positive shipping
--              costs where `is_refunded` is NOT set to TRUE. These will break
--              the downstream `daily_revenue_etl` pipeline.
-- ---------------------------------------------------------------------

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
    AND is_refunded = FALSE
ORDER BY 
    order_date DESC;


-- ---------------------------------------------------------------------
-- QUERY 2: Daily Revenue ETL Simulation
-- Target Asset: daily_revenue_etl (Downstream representation)
-- Description: Simulates the core logic of the daily revenue pipeline,
--              safeguarding against the negative margin edge case.
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE PROD.daily_revenue_etl_SIMULATION AS
WITH cleaned_orders AS (
    SELECT
        order_id,
        customer_id,
        CAST(order_date AS DATE) AS revenue_date,
        order_status,
        subtotal_amount,
        shipping_amount,
        is_refunded,
        -- Safeguard logic: If it's a canceled order with $0 subtotal and positive shipping,
        -- and is_refunded is false, we force-zero the shipping to prevent negative margin calculations.
        CASE 
            WHEN order_status = 'CANCELED' AND subtotal_amount = 0.00 AND is_refunded = FALSE 
            THEN 0.00 
            ELSE shipping_amount 
        END AS adjusted_shipping_amount
    FROM 
        PROD.orders_v3_FINAL
    WHERE 
        order_date >= DATEADD('day', -30, CURRENT_DATE()) -- Rolling 30-day window
),

daily_aggregates AS (
    SELECT
        revenue_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(CASE WHEN order_status = 'COMPLETED' THEN subtotal_amount ELSE 0 END) AS gross_revenue,
        SUM(adjusted_shipping_amount) AS total_shipping_revenue,
        SUM(CASE WHEN is_refunded = TRUE THEN (subtotal_amount + adjusted_shipping_amount) ELSE 0 END) AS total_refunds
    FROM 
        cleaned_orders
    GROUP BY 
        1
)

SELECT
    revenue_date,
    total_orders,
    gross_revenue,
    total_shipping_revenue,
    total_refunds,
    (gross_revenue + total_shipping_revenue - total_refunds) AS net_revenue
FROM 
    daily_aggregates
ORDER BY 
    revenue_date DESC;


-- ---------------------------------------------------------------------
-- QUERY 3: Quarterly Board Metrics Aggregation
-- Target Asset: quarterly_board_metrics
-- Description: Aggregates historical orders and customer dimensions to
--              populate the quarterly board report.
-- ---------------------------------------------------------------------

INSERT INTO PROD.quarterly_board_metrics (
    fiscal_quarter,
    total_active_customers,
    total_orders_processed,
    gross_merchandise_value,
    refund_rate_percentage
)
WITH quarterly_raw AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.subtotal_amount,
        o.is_refunded,
        CONCAT(YEAR(o.order_date), '-Q', QUARTER(o.order_date)) AS f_quarter
    FROM 
        PROD.orders_v3_FINAL o
    INNER JOIN 
        PROD.dim_customers c ON o.customer_id = c.customer_id
    WHERE 
        o.order_status IN ('COMPLETED', 'CANCELED')
)

SELECT
    f_quarter AS fiscal_quarter,
    COUNT(DISTINCT customer_id) AS total_active_customers,
    COUNT(DISTINCT order_id) AS total_orders_processed,
    SUM(subtotal_amount) AS gross_merchandise_value,
    ROUND(
        (COUNT(CASE WHEN is_refunded = TRUE THEN 1 END) * 100.0) / NULLIF(COUNT(order_id), 0), 
        2
    ) AS refund_rate_percentage
FROM 
    quarterly_raw
GROUP BY 
    1
ORDER BY 
    1 DESC;


-- ---------------------------------------------------------------------
-- QUERY 4: Churn Prediction Feature Generation
-- Target Asset: churn_prediction_features
-- Description: Generates behavioral features for the ML churn model
--              by combining customer dimensions and order history.
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE PROD.churn_prediction_features AS
WITH customer_order_stats AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders_placed,
        SUM(subtotal_amount) AS lifetime_spend,
        MAX(order_date) AS last_order_timestamp,
        COUNT(CASE WHEN is_refunded = TRUE THEN 1 END) AS total_refunded_orders,
        DATEDIFF('day', MAX(order_date), CURRENT_TIMESTAMP()) AS days_since_last_order
    FROM 
        PROD.orders_v3_FINAL
    GROUP BY 
        customer_id
)

SELECT
    c.customer_id,
    c.signup_date,
    c.customer_segment,
    COALESCE(s.total_orders_placed, 0) AS total_orders_placed,
    COALESCE(s.lifetime_spend, 0.00) AS lifetime_spend,
    s.last_order_timestamp,
    COALESCE(s.total_refunded_orders, 0) AS total_refunded_orders,
    COALESCE(s.days_since_last_order, 999) AS days_since_last_order, -- Default high value for inactive
    CASE 
        WHEN s.days_since_last_order > 90 OR s.days_since_last_order IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_churned_candidate
FROM 
    PROD.dim_customers c
LEFT JOIN 
    customer_order_stats s ON c.customer_id = s.customer_id;


-- ---------------------------------------------------------------------
-- QUERY 5: Schema Evolution & Legacy Reconciliation
-- Target Assets: orders_v1, orders_v2, orders_v3_FINAL
-- Description: Audit query to verify volume consistency across historical
--              versions of the orders table during migration validation.
-- ---------------------------------------------------------------------

SELECT 
    'orders_v1' AS table_version, 
    COUNT(*) AS record_count, 
    MIN(order_date) AS min_date, 
    MAX(order_date) AS max_date 
FROM PROD.orders_v1

UNION ALL

SELECT 
    'orders_v2' AS table_version, 
    COUNT(*) AS record_count, 
    MIN(order_date) AS min_date, 
    MAX(order_date) AS max_date 
FROM PROD.orders_v2

UNION ALL

SELECT 
    'orders_v3_FINAL' AS table_version, 
    COUNT(*) AS record_count, 
    MIN(order_date) AS min_date, 
    MAX(order_date) AS max_date 
FROM PROD.orders_v3_FINAL;