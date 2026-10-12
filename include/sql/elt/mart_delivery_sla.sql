-- include/sql/elt/mart_delivery_sla.sql
-- Business question 3: average delivery time per store and % of delivered orders within 15 minutes.
-- Impossible times (> {{max_delivery_minutes}} min) were already rejected in staging,
-- so they can't distort the "10-minute promise" KPI any more.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    s.store_name,
    s.zone,
    COUNT(*)                                                            AS delivered_orders,
    ROUND(AVG(o.delivery_minutes), 1)                                   AS avg_delivery_minutes,
    COUNT(*) FILTER (WHERE o.delivery_minutes <= {{sla_minutes}})       AS within_15_min_orders,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.delivery_minutes <= {{sla_minutes}}) / COUNT(*), 1) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED'
  AND o.order_status = 'DELIVERED'
GROUP BY o.store_id, s.store_name, s.zone
ORDER BY o.store_id;
