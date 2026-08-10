-- ==============================================================================
-- HANDOFF REFERENCE SQL SCRIPT
-- Departing Engineer: alice.chen
-- Target Successor: Data Engineering Team
-- Date: October 2023
--
-- This script serves as the technical documentation and operational runbook
-- for the data assets previously managed by alice.chen. It contains asset
-- mappings, data quality checks, edge-case detection queries, and integration
-- examples demonstrating how these tables and pipelines fit together.
-- ==============================================================================

-- ==============================================================================
-- SECTION 1: ASSET INVENTORY & METADATA MAP
-- ==============================================================================
/*
  The following assets are covered in this handoff. 
  Their relationships and operational statuses are detailed below:

  1. Snowflake Datasets:
     - PROD.orders_v3_FINAL (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v3_FINAL,PROD))
       * Status: ACTIVE (Production Core)
       * SLA: Daily 6:00 AM UTC
       * Downstream: daily_revenue_etl, revenue_dashboard_BACKUP, quarterly_board_metrics
     - PROD.dim_customers (urn:li:dataset:(urn:li:dataPlatform:snowflake,dim_customers,PROD))
       * Status: ACTIVE (Production Dimension)
     - PROD.churn_prediction_features (urn:li:dataset:(urn:li:dataPlatform:snowflake,churn_prediction_features,PROD))
       * Status: ACTIVE (ML Feature Store)
     - PROD.quarterly_board_metrics (urn:li:dataset:(urn:li:dataPlatform:snowflake,quarterly_board_metrics,PROD))
       * Status: ACTIVE (Executive Reporting)
     - PROD.orders_v2 (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v2,PROD))
       * Status: DEPRECATED (Keep for historical audit only)
     - PROD.orders_v1 (urn:li:dataset:(urn:li:dataPlatform:snowflake,orders_v1,PROD))
       * Status: DEPRECATED (Keep for historical audit only)

  2. Streaming / Event Ingestion:
     - PROD.kafka_events_raw_copy (urn:li:dataset:(urn:li:dataPlatform:kafka,kafka_events_raw_copy,PROD))
       * Status: ACTIVE (Raw event stream copy for debugging)

  3. Orchestration & BI:
     - Airflow: daily_revenue_etl (urn:li:dataset:(urn:li:dataPlatform:airflow,daily_revenue_etl,PROD))
       * Status: ACTIVE (Daily DAG running at 06:30 AM UTC)
     - Looker: revenue_dashboard_BACKUP (urn:li:dataset:(urn:li:dataPlatform:looker,revenue_dashboard_BACKUP,PROD))
       * Status: ACTIVE (Executive Dashboard Backup)
*/

-- ==============================================================================
-- SECTION 2: OPERATIONAL RUNBOOK & EDGE-CASE MONITORING
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- CRITICAL EDGE CASE: Zero-Dollar Subtotal / Positive Shipping Cost
-- ------------------------------------------------------------------------------
-- Context:
-- Canceled orders with a $0 subtotal and a positive shipping cost will break 
-- the downstream `daily_revenue_etl` pipeline (causing negative margin calculations) 
-- if the `is_refunded` flag is not set to `true` within a 24-hour window.
--
-- Run the query below to identify any active "breaking" records in production.
-- If this query returns rows, you must trigger the Operational Recovery Procedure.
-- ------------------------------------------------------------------------------

-- Query to detect breaking orders:
SELECT 
    order_id,
    customer_id,
    subtotal,
    shipping_cost,
    order_status,
    is_refunded,
    created_at,
    DATEDIFF('hour', created_at, CURRENT_TIMESTAMP()) AS hours_since_creation
FROM PROD.orders_v3_FINAL
WHERE 
    subtotal = 0 
    AND shipping_cost > 0 
    AND order_status = 'CANCELED' 
    AND is_refunded = FALSE
    AND created_at >= DATEADD('day', -2, CURRENT_TIMESTAMP())
ORDER BY created_at DESC;


/*
  OPERATIONAL RECOVERY PROCEDURE (Stripe Webhook Failure):
  
  If the query above returns records older than 24 hours, the Stripe webhook 
  ingestion API has likely failed to sync. Follow these steps:

  1. RECONCILIATION SCRIPT:
     Navigate to the `data-ops` repository and execute the catchup script.
     
     * For a single or small batch of order IDs:
       python stripe_reconciliation_catchup.py --order-ids "ORD-12345,ORD-67890"
       
     * For bulk execution (exceeding 500 records), use a CSV file and append 
       the rate-limiting batch flag to prevent Stripe API timeouts:
       python stripe_reconciliation_catchup.py --file-path /path/to/failed_orders.csv --batch-size 100

  2. DOWNSTREAM BACKFILL:
     Once the script completes and `is_refunded` is updated to TRUE in Snowflake, 
     manually trigger an Airflow backfill on the `daily_revenue_etl` DAG for the 
     affected execution dates. This will recalculate downstream metrics and 
     automatically refresh the Looker `revenue_dashboard_BACKUP`.
*/


-- ==============================================================================
-- SECTION 3: DATA QUALITY & SLA ASSERTIONS
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- SLA Verification Query
-- ------------------------------------------------------------------------------
-- Verifies if the daily batch for orders_v3_FINAL completed before the 6:00 AM UTC SLA.
-- ------------------------------------------------------------------------------
SELECT 
    CAST(created_at AS DATE) AS order_date,
    MAX(created_at) AS last_ingested_record,
    CASE 
        WHEN MAX(created_at) >= DATEADD('hour', 6, CAST(CAST(created_at AS DATE) AS TIMESTAMP_NTZ)) 
        THEN 'SLA BREACHED'
        ELSE 'SLA MET'
    END AS sla_status
FROM PROD.orders_v3_FINAL
WHERE created_at >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;


-- ------------------------------------------------------------------------------
-- dbt Data Quality Assertions (Replication)
-- ------------------------------------------------------------------------------
-- The `FINAL` suffix indicates the table has passed dbt assertions. 
-- This query manually validates those assertions:
-- 1. `not_null` on critical columns.
-- 2. `accepted_values` on `order_status` ('PLACED', 'SHIPPED', 'DELIVERED', 'CANCELED').
-- ------------------------------------------------------------------------------
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) AS null_order_statuses,
    SUM(CASE WHEN order_status NOT IN ('PLACED', 'SHIPPED', 'DELIVERED', 'CANCELED') THEN 1 ELSE 0 END) AS invalid_status_count
FROM PROD.orders_v3_FINAL;


-- ==============================================================================
-- SECTION 4: INTEGRATION & DOWNSTREAM PIPELINE SIMULATION
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Pipeline Simulation: daily_revenue_etl
-- ------------------------------------------------------------------------------
-- This query simulates how the `daily_revenue_etl` pipeline aggregates daily 
-- financial metrics from `orders_v3_FINAL`, incorporating the safety logic 
-- for the zero-dollar subtotal edge case.
-- ------------------------------------------------------------------------------
WITH daily_metrics AS (
    SELECT 
        CAST(created_at AS DATE) AS reporting_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(subtotal) AS gross_subtotal,
        SUM(shipping_cost) AS gross_shipping,
        
        -- Safe Revenue Calculation: Excludes canceled orders that have been successfully refunded
        SUM(
            CASE 
                WHEN order_status = 'CANCELED' AND is_refunded = TRUE THEN 0
                ELSE subtotal
            END
        ) AS net_subtotal,
        
        -- Margin Calculation: Protects against negative margin caused by unrefunded $0 subtotal orders
        SUM(
            CASE 
                WHEN order_status = 'CANCELED' AND is_refunded = FALSE AND subtotal = 0 AND shipping_cost > 0 
                THEN 0 -- Override negative margin bug during ETL
                ELSE (subtotal * 0.65) - (shipping_cost * 0.15) -- Standard margin formula
            END
        ) AS estimated_margin
    FROM PROD.orders_v3_FINAL
    WHERE created_at >= DATEADD('month', -1, CURRENT_DATE())
    GROUP BY 1
)
SELECT 
    reporting_date,
    total_orders,
    gross_subtotal,
    gross_shipping,
    net_subtotal,
    estimated_margin,
    ROUND((estimated_margin / NULLIF(net_subtotal, 0)) * 100, 2) AS margin_percentage
FROM daily_metrics
ORDER BY reporting_date DESC;


-- ------------------------------------------------------------------------------
-- Downstream Integration: Customer Churn Feature Generation
-- ------------------------------------------------------------------------------
-- Demonstrates how `orders_v3_FINAL` joins with `dim_customers` and 
-- `churn_prediction_features` to provide a unified view of customer health.
-- ------------------------------------------------------------------------------
SELECT 
    c.customer_id,
    c.customer_name,
    c.segment,
    c.acquisition_channel,
    COUNT(DISTINCT o.order_id) AS lifetime_order_count,
    COALESCE(SUM(o.subtotal), 0) AS lifetime_spend,
    MAX(o.created_at) AS most_recent_order_date,
    f.churn_risk_score,
    f.predicted_churn_date,
    f.last_active_features_update
FROM PROD.dim_customers c
LEFT JOIN PROD.orders_v3_FINAL o 
    ON c.customer_id = o.customer_id 
    AND o.order_status != 'CANCELED'
LEFT JOIN PROD.churn_prediction_features f 
    ON c.customer_id = f.customer_id
GROUP BY 1, 2, 3, 4, f.churn_risk_score, f.predicted_churn_date, f.last_active_features_update
ORDER BY f.churn_risk_score DESC NULLS LAST;


-- ------------------------------------------------------------------------------
-- Downstream Integration: Quarterly Board Metrics Generation
-- ------------------------------------------------------------------------------
-- Demonstrates how quarterly financial aggregates are compiled from the 
-- production orders dataset to populate the `quarterly_board_metrics` table.
-- ------------------------------------------------------------------------------
INSERT OVERWRITE INTO PROD.quarterly_board_metrics
SELECT 
    DATE_TRUNC('quarter', created_at) AS fiscal_quarter,
    COUNT(DISTINCT customer_id) AS unique_active_customers,
    COUNT(DISTINCT order_id) AS total_completed_orders,
    SUM(subtotal) AS total_net_revenue,
    SUM(shipping_cost) AS total_shipping_revenue,
    ROUND(SUM(subtotal) / COUNT(DISTINCT order_id), 2) AS average_order_value,
    CURRENT_TIMESTAMP() AS updated_at
FROM PROD.orders_v3_FINAL
WHERE 
    order_status IN ('SHIPPED', 'DELIVERED')
    OR (order_status = 'PLACED' AND is_refunded = FALSE)
GROUP BY 1;


-- ==============================================================================
-- SECTION 5: HISTORICAL AUDIT & DEPRECATION MAPPING
-- ==============================================================================
-- Note: orders_v1 and orders_v2 are deprecated. 
-- Use this query only if historical reconciliation prior to 2022 is required.
-- ------------------------------------------------------------------------------
SELECT 'v1_historical' AS source, COUNT(*), MIN(created_at), MAX(created_at) FROM PROD.orders_v1
UNION ALL
SELECT 'v2_historical' AS source, COUNT(*), MIN(created_at), MAX(created_at) FROM PROD.orders_v2
UNION ALL
SELECT 'v3_production' AS source, COUNT(*), MIN(created_at), MAX(created_at) FROM PROD.orders_v3_FINAL;