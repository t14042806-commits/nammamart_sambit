-- include/sql/etl/reports.sql
-- Part A step 7: two report tables built from the ETL star schema.
-- CREATE OR REPLACE = rebuilt from scratch every run -> idempotent.

-- Business question 1: daily net revenue by zone and store (DELIVERED only, after discount)
CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
SELECT
    f.order_date,
    s.zone,
    f.store_id,
    s.store_name,
    COUNT(*)                       AS delivered_orders,
    ROUND(SUM(f.net_revenue), 2)   AS net_revenue
FROM etl_fact_orders f
JOIN etl_dim_store s ON s.store_id = f.store_id
WHERE f.order_status = 'DELIVERED'
GROUP BY f.order_date, s.zone, f.store_id, s.store_name
ORDER BY f.order_date, s.zone, f.store_id;

-- Business question 2: revenue and gross margin by category
CREATE OR REPLACE TABLE etl_category_margin AS
SELECT
    p.category,
    COUNT(*)                                                  AS delivered_orders,
    SUM(f.quantity)                                           AS units_sold,
    ROUND(SUM(f.net_revenue), 2)                              AS net_revenue,
    ROUND(SUM(f.cost_amount), 2)                              AS cost,
    ROUND(SUM(f.gross_margin), 2)                             AS gross_margin,
    ROUND(100 * SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0), 1) AS margin_pct
FROM etl_fact_orders f
JOIN etl_dim_product p ON p.product_id = f.product_id
WHERE f.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY net_revenue DESC;
