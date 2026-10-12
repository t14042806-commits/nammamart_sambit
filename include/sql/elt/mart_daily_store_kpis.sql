-- include/sql/elt/mart_daily_store_kpis.sql
-- Business question 1: daily net revenue by zone and store (DELIVERED only, after discount)
-- Only rows that are not FAILED in staging are used ("WARNING" rows count - they were fixed).

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    o.store_id,
    s.store_name,
    COUNT(*)                                                         AS total_orders,
    COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED')             AS delivered_orders,
    COUNT(*) FILTER (WHERE o.order_status = 'CANCELLED')             AS cancelled_orders,
    COUNT(*) FILTER (WHERE o.order_status = 'RETURNED')              AS returned_orders,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END), 2) AS net_revenue,
    ROUND(AVG(o.delivery_minutes) FILTER (WHERE o.order_status = 'DELIVERED'), 1)        AS avg_delivery_minutes
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED'
GROUP BY o.order_date, s.zone, o.store_id, s.store_name
ORDER BY o.order_date, s.zone, o.store_id;
