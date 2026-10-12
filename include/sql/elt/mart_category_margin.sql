-- include/sql/elt/mart_category_margin.sql
-- Business question 2: revenue and gross margin by category
-- margin = revenue - quantity x cost_price   (DELIVERED orders only)

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    COUNT(*)                                                       AS delivered_orders,
    SUM(o.quantity)                                                AS units_sold,
    ROUND(SUM(o.order_amount), 2)                                  AS net_revenue,
    ROUND(SUM(o.quantity * p.cost_price), 2)                       AS cost,
    ROUND(SUM(o.order_amount - o.quantity * p.cost_price), 2)      AS gross_margin,
    ROUND(100 * SUM(o.order_amount - o.quantity * p.cost_price) / NULLIF(SUM(o.order_amount), 0), 1) AS margin_pct
FROM stg_orders o
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED'
  AND o.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY net_revenue DESC;
