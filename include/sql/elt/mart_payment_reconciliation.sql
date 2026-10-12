-- include/sql/elt/mart_payment_reconciliation.sql
-- Business question 4 (second half): every order/payment mismatch, one row each.
-- Same rules as reconcile() in include/code/etl_steps.py:
--   MISSING_SUCCESS_PAYMENT, MULTIPLE_SUCCESS_PAYMENTS, AMOUNT_MISMATCH,
--   RETURNED_BUT_NOT_REFUNDED, CANCELLED_BUT_CHARGED,
--   PAYMENT_FOR_REJECTED_ORDER (order exists but was quarantined), PAYMENT_WITHOUT_ORDER (never exported)
-- Payments are de-duplicated (is_duplicate = false) but otherwise used as sent,
-- because finance wants to see every rupee the gateway reports.

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH clean_orders AS (
    SELECT order_id, order_status, order_amount FROM stg_orders WHERE dq_status <> 'FAILED'
),
payments AS (
    SELECT * FROM stg_payments WHERE NOT is_duplicate
),
exported_orders AS (      -- every order id the order system sent this run, good or bad
    SELECT DISTINCT coalesce(trim(order_id), '') AS order_id FROM raw_orders WHERE _run_id = '{{run_id}}'
),
success AS (
    SELECT order_id,
           COUNT(*)                       AS n_success,
           SUM(coalesce(amount_paid, 0))  AS paid,
           MIN(payment_id)                AS payment_id
    FROM payments
    WHERE payment_status = 'SUCCESS'
    GROUP BY order_id
),
order_side AS (
    SELECT
        o.order_id,
        s.payment_id,
        o.order_status,
        o.order_amount,
        CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END AS expected_amount,
        coalesce(s.paid, 0)                                                   AS amount_paid,
        CASE
            WHEN o.order_status = 'DELIVERED' AND s.order_id IS NULL  THEN 'MISSING_SUCCESS_PAYMENT'
            WHEN o.order_status = 'DELIVERED' AND s.n_success > 1     THEN 'MULTIPLE_SUCCESS_PAYMENTS'
            WHEN o.order_status = 'DELIVERED'
             AND abs(s.paid - o.order_amount) > {{amount_tolerance}}  THEN 'AMOUNT_MISMATCH'
            WHEN o.order_status = 'RETURNED'  AND s.order_id IS NOT NULL THEN 'RETURNED_BUT_NOT_REFUNDED'
            WHEN o.order_status = 'CANCELLED' AND s.order_id IS NOT NULL THEN 'CANCELLED_BUT_CHARGED'
        END AS reason
    FROM clean_orders o
    LEFT JOIN success s ON s.order_id = o.order_id
),
payment_side AS (
    SELECT
        p.order_id, p.payment_id,
        NULL::VARCHAR AS order_status, NULL::DOUBLE AS order_amount,
        0.0 AS expected_amount, coalesce(p.amount_paid, 0) AS amount_paid,
        CASE WHEN p.order_id IN (SELECT order_id FROM exported_orders)
             THEN 'PAYMENT_FOR_REJECTED_ORDER' ELSE 'PAYMENT_WITHOUT_ORDER' END AS reason
    FROM payments p
    WHERE p.order_id NOT IN (SELECT order_id FROM clean_orders)
)
SELECT order_id, payment_id, order_status, order_amount, expected_amount, amount_paid,
       amount_paid - expected_amount AS difference, reason, '{{run_id}}' AS _run_id
FROM order_side WHERE reason IS NOT NULL
UNION ALL
SELECT order_id, payment_id, order_status, order_amount, expected_amount, amount_paid,
       amount_paid - expected_amount AS difference, reason, '{{run_id}}' AS _run_id
FROM payment_side;
