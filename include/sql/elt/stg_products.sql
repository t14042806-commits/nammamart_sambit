-- include/sql/elt/stg_products.sql
-- Same pattern as stg_customers.sql: src -> std -> flagged -> final.

CREATE OR REPLACE TABLE stg_products AS
WITH src AS (
    SELECT * FROM raw_products WHERE _run_id = '{{run_id}}'
),

std AS (
    SELECT
        coalesce(trim(product_id), '')    AS product_id,
        coalesce(trim(product_name), '')  AS product_name,
        coalesce(trim(category), '')      AS category,
        coalesce(trim(sub_category), '')  AS sub_category,
        coalesce(trim(brand), '')         AS brand,
        TRY_CAST(NULLIF(trim(mrp), '') AS DOUBLE)        AS mrp,
        TRY_CAST(NULLIF(trim(cost_price), '') AS DOUBLE) AS cost_price,
        coalesce(trim(unit), '')          AS unit,
        TRY_CAST(trim(is_perishable) AS BOOLEAN)         AS is_perishable,
        try_strptime(trim(launch_date), '{{date_format}}')::DATE AS launch_date,
        _row_num, _source_file, _run_id, _loaded_at
    FROM src
),

flagged AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _row_num) > 1   AS chk_product_id_unique,
        category = ''                                                       AS chk_category_not_null,
        (mrp IS NULL OR cost_price IS NULL)                                 AS chk_prices_parse,
        launch_date IS NULL                                                 AS chk_launch_date_valid,
        coalesce(mrp <= 0, false)                                           AS chk_mrp_positive,
        coalesce(launch_date > CAST('{{run_ts}}' AS TIMESTAMP)::DATE, false) AS chk_launch_date_not_future,
        coalesce(cost_price > mrp, false)                                   AS chk_cost_price_not_above_mrp
    FROM std
)

SELECT
    product_id, product_name, category, sub_category, brand, mrp, cost_price, unit,
    is_perishable, launch_date,
    CASE
        WHEN chk_product_id_unique OR chk_category_not_null OR chk_prices_parse
          OR chk_launch_date_valid OR chk_mrp_positive OR chk_launch_date_not_future THEN 'FAILED'
        WHEN chk_cost_price_not_above_mrp                                           THEN 'WARNING'
        ELSE 'PASSED'
    END AS dq_status,
    concat_ws('; ',
        CASE WHEN chk_product_id_unique      THEN 'product_id_unique' END,
        CASE WHEN chk_category_not_null      THEN 'category_not_null' END,
        CASE WHEN chk_prices_parse           THEN 'prices_parse' END,
        CASE WHEN chk_launch_date_valid      THEN 'launch_date_valid' END,
        CASE WHEN chk_mrp_positive           THEN 'mrp_positive' END,
        CASE WHEN chk_launch_date_not_future THEN 'launch_date_not_future' END
    ) AS failure_reason,
    concat_ws('; ',
        CASE WHEN chk_cost_price_not_above_mrp THEN 'cost_price_not_above_mrp' END
    ) AS dq_warnings,
    chk_product_id_unique, chk_category_not_null, chk_prices_parse, chk_launch_date_valid,
    chk_mrp_positive, chk_launch_date_not_future, chk_cost_price_not_above_mrp,
    _row_num, _source_file, _run_id, _loaded_at
FROM flagged;
