-- include/sql/elt/stg_payments.sql
-- Same pattern as stg_customers.sql: src -> std -> flagged -> final.
-- "order_exists" looks at EVERY order_id in this run's raw order rows
-- (orders.csv + increment if loaded), good or bad - same as the ETL pipeline.

CREATE OR REPLACE TABLE stg_payments AS
WITH src AS (
    SELECT * FROM raw_payments WHERE _run_id = '{{run_id}}'
),

known_orders AS (
    SELECT DISTINCT coalesce(trim(order_id), '') AS order_id
    FROM raw_orders WHERE _run_id = '{{run_id}}'
),

std AS (
    SELECT
        coalesce(trim(payment_id), '')   AS payment_id,
        coalesce(trim(order_id), '')     AS order_id,
        try_strptime(trim(payment_ts), '{{ts_format}}') AS payment_ts,
        coalesce(payment_mode, '')       AS payment_mode_raw,
        CASE lower(coalesce(trim(payment_mode), ''))
            WHEN 'upi'              THEN 'UPI'
            WHEN 'card'             THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cod'              THEN 'Cash on Delivery'
            WHEN 'wallet'           THEN 'Wallet'
            ELSE coalesce(trim(payment_mode), '')
        END                              AS payment_mode,
        TRY_CAST(NULLIF(trim(amount_paid), '') AS DOUBLE) AS amount_paid,
        upper(coalesce(trim(payment_status), ''))         AS payment_status,
        coalesce(trim(gateway_ref), '')  AS gateway_ref,
        _row_num, _source_file, _run_id, _loaded_at
    FROM src
),

flagged AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _row_num) > 1  AS chk_payment_id_unique,
        order_id = ''                                                      AS chk_order_id_not_null,
        payment_mode NOT IN ({{valid_payment_modes}})                      AS chk_payment_mode_valid,
        payment_status NOT IN ({{valid_payment_status}})                   AS chk_payment_status_valid,
        payment_ts IS NULL                                                 AS chk_payment_ts_format_valid,
        amount_paid IS NULL                                                AS chk_amount_paid_parses,
        coalesce(payment_status = 'SUCCESS' AND amount_paid <= 0, false)   AS chk_success_amount_positive,
        (order_id <> '' AND order_id NOT IN (SELECT order_id FROM known_orders)) AS chk_order_exists,
        coalesce(payment_ts > CAST('{{run_ts}}' AS TIMESTAMP), false)      AS chk_payment_ts_not_future,
        (payment_mode <> 'Cash on Delivery' AND gateway_ref = '')          AS chk_gateway_ref_present,
        (payment_mode_raw <> payment_mode AND payment_mode IN ({{valid_payment_modes}})) AS chk_payment_mode_standardised
    FROM std
)

SELECT
    payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref,
    CASE
        WHEN chk_payment_id_unique OR chk_order_id_not_null OR chk_payment_mode_valid
          OR chk_payment_status_valid OR chk_payment_ts_format_valid OR chk_amount_paid_parses
          OR chk_success_amount_positive OR chk_order_exists OR chk_payment_ts_not_future THEN 'FAILED'
        WHEN chk_gateway_ref_present OR chk_payment_mode_standardised                    THEN 'WARNING'
        ELSE 'PASSED'
    END AS dq_status,
    concat_ws('; ',
        CASE WHEN chk_payment_id_unique       THEN 'payment_id_unique' END,
        CASE WHEN chk_order_id_not_null       THEN 'order_id_not_null' END,
        CASE WHEN chk_payment_mode_valid      THEN 'payment_mode_valid' END,
        CASE WHEN chk_payment_status_valid    THEN 'payment_status_valid' END,
        CASE WHEN chk_payment_ts_format_valid THEN 'payment_ts_format_valid' END,
        CASE WHEN chk_amount_paid_parses      THEN 'amount_paid_parses' END,
        CASE WHEN chk_success_amount_positive THEN 'success_amount_positive' END,
        CASE WHEN chk_order_exists            THEN 'order_exists' END,
        CASE WHEN chk_payment_ts_not_future   THEN 'payment_ts_not_future' END
    ) AS failure_reason,
    concat_ws('; ',
        CASE WHEN chk_gateway_ref_present       THEN 'gateway_ref_present' END,
        CASE WHEN chk_payment_mode_standardised THEN 'payment_mode_standardised' END
    ) AS dq_warnings,
    chk_payment_id_unique AS is_duplicate,
    chk_payment_id_unique, chk_order_id_not_null, chk_payment_mode_valid, chk_payment_status_valid,
    chk_payment_ts_format_valid, chk_amount_paid_parses, chk_success_amount_positive,
    chk_order_exists, chk_payment_ts_not_future, chk_gateway_ref_present, chk_payment_mode_standardised,
    _row_num, _source_file, _run_id, _loaded_at
FROM flagged;
