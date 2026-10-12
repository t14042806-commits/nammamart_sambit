-- include/sql/elt/mart_top_customers.sql
-- Business question 5: top customers by spend with loyalty tier.
-- Only MASKED name/email/phone exist in stg_customers, so nothing sensitive can leak here.
-- All customers are ranked; "top 10" = WHERE spend_rank <= 10.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    RANK() OVER (ORDER BY SUM(o.order_amount) DESC)  AS spend_rank,
    c.customer_id,
    c.customer_name,
    c.email_masked,
    c.phone_masked,
    c.loyalty_tier,
    COUNT(*)                                         AS delivered_orders,
    ROUND(SUM(o.order_amount), 2)                    AS total_spend
FROM stg_orders o
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED'
  AND o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.customer_name, c.email_masked, c.phone_masked, c.loyalty_tier
ORDER BY spend_rank, c.customer_id;
