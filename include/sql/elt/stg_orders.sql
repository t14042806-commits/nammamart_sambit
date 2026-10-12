-- include/sql/elt/stg_orders.sql
-- Staging for orders. Used TWICE with different placeholders:
--   {{target_table}} = stg_orders              {{source_file}} = orders.csv
--   {{target_table}} = stg_orders_increment    {{source_file}} = orders_2026-10-01_increment.csv
-- so the increment file gets exactly the same rules before it is MERGEd (stg_orders_merge.sql).
--
-- Needs stg_customers, stg_products and stg_stores to exist first (foreign-key checks
-- are made against their NON-FAILED rows, same as the ETL pipeline).

CREATE OR REPLACE TABLE {{target_table}} AS
WITH src AS (
    SELECT * FROM raw_orders
    WHERE _run_id = '{{run_id}}' AND _source_file = '{{source_file}}'
),

std AS (
    SELECT
        coalesce(trim(order_id), '')       AS order_id,
        coalesce(trim(order_ts), '')       AS order_ts_text,
        try_strptime(trim(order_ts), '{{ts_format}}') AS order_ts,
        coalesce(trim(customer_id), '')    AS customer_id,
        coalesce(trim(store_id), '')       AS store_id,
        coalesce(trim(product_id), '')     AS product_id,
        TRY_CAST(NULLIF(trim(quantity), '') AS DOUBLE)      AS quantity,
        TRY_CAST(NULLIF(trim(unit_price), '') AS DOUBLE)    AS unit_price,
        TRY_CAST(NULLIF(trim(discount_pct), '') AS DOUBLE)  AS discount_pct,
        TRY_CAST(NULLIF(trim(order_amount), '') AS DOUBLE)  AS order_amount,
        coalesce(trim(delivery_minutes), '')                AS delivery_minutes_text,
        TRY_CAST(NULLIF(trim(delivery_minutes), '') AS DOUBLE) AS delivery_minutes,
        coalesce(order_status, '')         AS order_status_raw,
        upper(coalesce(trim(order_status), '')) AS order_status,
        coalesce(payment_mode, '')         AS payment_mode_raw,
        -- keep in sync with config.PAYMENT_MODE_MAP ('Cashh' is deliberately not mapped)
        CASE lower(coalesce(trim(payment_mode), ''))
            WHEN 'upi'              THEN 'UPI'
            WHEN 'card'             THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cod'              THEN 'Cash on Delivery'
            WHEN 'wallet'           THEN 'Wallet'
            ELSE coalesce(trim(payment_mode), '')
        END                                AS payment_mode,
        coalesce(trim(channel), '')        AS channel,
        _row_num, _source_file, _run_id, _loaded_at
    FROM src
),

-- look-ups against the CLEAN masters
joined AS (
    SELECT
        s.*,
        p.mrp                       AS product_mrp,
        p.product_id IS NOT NULL    AS product_known,
        c.customer_id IS NOT NULL   AS customer_known,
        st.store_id IS NOT NULL     AS store_known
    FROM std s
    LEFT JOIN (SELECT product_id, mrp FROM stg_products  WHERE dq_status <> 'FAILED') p  ON p.product_id = s.product_id
    LEFT JOIN (SELECT customer_id     FROM stg_customers WHERE dq_status <> 'FAILED') c  ON c.customer_id = s.customer_id
    LEFT JOIN (SELECT store_id        FROM stg_stores    WHERE dq_status <> 'FAILED') st ON st.store_id = s.store_id
),

flagged AS (
    SELECT
        *,
        -- Completeness
        order_id = ''                                                       AS chk_order_id_not_null,
        customer_id = ''                                                    AS chk_customer_id_not_null,
        order_ts_text = ''                                                  AS chk_order_ts_not_null,
        (order_status = 'DELIVERED' AND delivery_minutes_text = '')         AS chk_delivered_has_delivery_minutes,
        -- Uniqueness
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _row_num) > 1     AS chk_order_id_unique,
        -- Validity
        order_status NOT IN ({{valid_order_status}})                        AS chk_order_status_valid,
        payment_mode NOT IN ({{valid_payment_modes}})                       AS chk_payment_mode_valid,
        (order_ts_text <> '' AND order_ts IS NULL)                          AS chk_order_ts_format_valid,
        (quantity IS NULL OR unit_price IS NULL OR discount_pct IS NULL OR order_amount IS NULL
         OR (delivery_minutes_text <> '' AND delivery_minutes IS NULL))     AS chk_numbers_parse,
        coalesce(discount_pct < 0 OR discount_pct > 100, false)             AS chk_discount_pct_in_range,
        -- Accuracy
        coalesce(quantity <= 0, false)                                      AS chk_quantity_positive,
        coalesce(abs(quantity * unit_price * (1 - discount_pct / 100) - order_amount) > {{amount_tolerance}}, false)
                                                                            AS chk_order_amount_matches_formula,
        coalesce(delivery_minutes <= 0 OR delivery_minutes > {{max_delivery_minutes}}, false)
                                                                            AS chk_delivery_minutes_believable,
        coalesce(unit_price > product_mrp + {{amount_tolerance}}, false)    AS chk_unit_price_not_above_mrp,
        -- Consistency
        (customer_id <> '' AND NOT customer_known)                          AS chk_customer_exists,
        NOT product_known                                                   AS chk_product_exists,
        NOT store_known                                                     AS chk_store_exists,
        -- Timeliness
        coalesce(order_ts > CAST('{{run_ts}}' AS TIMESTAMP), false)         AS chk_order_ts_not_future,
        -- Warnings
        (order_status_raw <> order_status AND order_status IN ({{valid_order_status}})) AS chk_order_status_standardised,
        (payment_mode_raw <> payment_mode AND payment_mode IN ({{valid_payment_modes}})) AS chk_payment_mode_standardised
    FROM joined
)

SELECT
    order_id, order_ts, CAST(order_ts AS DATE) AS order_date,
    customer_id, store_id, product_id,
    CAST(quantity AS INTEGER) AS quantity, unit_price, discount_pct, order_amount,
    payment_mode, order_status, channel,
    CAST(delivery_minutes AS INTEGER) AS delivery_minutes,
    order_ts_text,   -- the original text, useful when order_ts could not be parsed

    CASE
        WHEN chk_order_id_not_null OR chk_customer_id_not_null OR chk_order_ts_not_null
          OR chk_delivered_has_delivery_minutes OR chk_order_id_unique OR chk_order_status_valid
          OR chk_payment_mode_valid OR chk_order_ts_format_valid OR chk_numbers_parse
          OR chk_discount_pct_in_range OR chk_quantity_positive OR chk_order_amount_matches_formula
          OR chk_delivery_minutes_believable OR chk_unit_price_not_above_mrp OR chk_customer_exists
          OR chk_product_exists OR chk_store_exists OR chk_order_ts_not_future         THEN 'FAILED'
        WHEN chk_order_status_standardised OR chk_payment_mode_standardised           THEN 'WARNING'
        ELSE 'PASSED'
    END AS dq_status,
    -- same order as the catalog, so the text matches the ETL failure_reason
    concat_ws('; ',
        CASE WHEN chk_order_id_not_null              THEN 'order_id_not_null' END,
        CASE WHEN chk_customer_id_not_null           THEN 'customer_id_not_null' END,
        CASE WHEN chk_order_ts_not_null              THEN 'order_ts_not_null' END,
        CASE WHEN chk_delivered_has_delivery_minutes THEN 'delivered_has_delivery_minutes' END,
        CASE WHEN chk_order_id_unique                THEN 'order_id_unique' END,
        CASE WHEN chk_order_status_valid             THEN 'order_status_valid' END,
        CASE WHEN chk_payment_mode_valid             THEN 'payment_mode_valid' END,
        CASE WHEN chk_order_ts_format_valid          THEN 'order_ts_format_valid' END,
        CASE WHEN chk_numbers_parse                  THEN 'numbers_parse' END,
        CASE WHEN chk_discount_pct_in_range          THEN 'discount_pct_in_range' END,
        CASE WHEN chk_quantity_positive              THEN 'quantity_positive' END,
        CASE WHEN chk_order_amount_matches_formula   THEN 'order_amount_matches_formula' END,
        CASE WHEN chk_delivery_minutes_believable    THEN 'delivery_minutes_believable' END,
        CASE WHEN chk_unit_price_not_above_mrp       THEN 'unit_price_not_above_mrp' END,
        CASE WHEN chk_customer_exists                THEN 'customer_exists' END,
        CASE WHEN chk_product_exists                 THEN 'product_exists' END,
        CASE WHEN chk_store_exists                   THEN 'store_exists' END,
        CASE WHEN chk_order_ts_not_future            THEN 'order_ts_not_future' END
    ) AS failure_reason,
    concat_ws('; ',
        CASE WHEN chk_order_status_standardised THEN 'order_status_standardised' END,
        CASE WHEN chk_payment_mode_standardised THEN 'payment_mode_standardised' END
    ) AS dq_warnings,
    chk_order_id_unique AS is_duplicate,   -- used by the MERGE so a duplicate copy is never revived

    chk_order_id_not_null, chk_customer_id_not_null, chk_order_ts_not_null,
    chk_delivered_has_delivery_minutes, chk_order_id_unique, chk_order_status_valid,
    chk_payment_mode_valid, chk_order_ts_format_valid, chk_numbers_parse, chk_discount_pct_in_range,
    chk_quantity_positive, chk_order_amount_matches_formula, chk_delivery_minutes_believable,
    chk_unit_price_not_above_mrp, chk_customer_exists, chk_product_exists, chk_store_exists,
    chk_order_ts_not_future, chk_order_status_standardised, chk_payment_mode_standardised,
    _row_num, _source_file, _run_id, _loaded_at
FROM flagged;
