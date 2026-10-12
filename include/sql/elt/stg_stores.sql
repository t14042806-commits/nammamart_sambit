-- include/sql/elt/stg_stores.sql
-- Same pattern as stg_customers.sql: src -> std -> flagged -> final.

CREATE OR REPLACE TABLE stg_stores AS
WITH src AS (
    SELECT * FROM raw_stores WHERE _run_id = '{{run_id}}'
),

std AS (
    SELECT
        coalesce(trim(store_id), '')    AS store_id,
        coalesce(trim(store_name), '')  AS store_name,
        coalesce(trim(zone), '')        AS zone_raw,
        coalesce(trim(area), '')        AS area,
        coalesce(trim(pincode), '')     AS pincode,
        try_strptime(trim(open_date), '{{date_format}}')::DATE        AS open_date,
        TRY_CAST(NULLIF(trim(capacity_orders_per_day), '') AS DOUBLE) AS capacity_orders_per_day,
        _row_num, _source_file, _run_id, _loaded_at
    FROM src
),

flagged AS (
    SELECT
        *,
        store_id = ''                                                      AS chk_store_id_not_null,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _row_num) > 1    AS chk_store_id_unique,
        open_date IS NULL                                                  AS chk_open_date_valid,
        coalesce(open_date > CAST('{{run_ts}}' AS TIMESTAMP)::DATE, false) AS chk_open_date_not_future,
        zone_raw NOT IN ({{valid_zones}})                                  AS chk_zone_valid,
        NOT regexp_full_match(pincode, '[0-9]{6}')                         AS chk_pincode_6_digits,
        coalesce(capacity_orders_per_day <= 0, true)                       AS chk_capacity_positive
    FROM std
)

SELECT
    store_id, store_name,
    CASE WHEN chk_zone_valid THEN 'Unknown' ELSE zone_raw END AS zone,
    area, pincode, open_date,
    CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    -- manager_name is not selected: employee personal data, not needed for reporting
    CASE
        WHEN chk_store_id_not_null OR chk_store_id_unique OR chk_open_date_valid
          OR chk_open_date_not_future                                    THEN 'FAILED'
        WHEN chk_zone_valid OR chk_pincode_6_digits OR chk_capacity_positive THEN 'WARNING'
        ELSE 'PASSED'
    END AS dq_status,
    concat_ws('; ',
        CASE WHEN chk_store_id_not_null    THEN 'store_id_not_null' END,
        CASE WHEN chk_store_id_unique      THEN 'store_id_unique' END,
        CASE WHEN chk_open_date_valid      THEN 'open_date_valid' END,
        CASE WHEN chk_open_date_not_future THEN 'open_date_not_future' END
    ) AS failure_reason,
    concat_ws('; ',
        CASE WHEN chk_zone_valid        THEN 'zone_valid' END,
        CASE WHEN chk_pincode_6_digits  THEN 'pincode_6_digits' END,
        CASE WHEN chk_capacity_positive THEN 'capacity_positive' END
    ) AS dq_warnings,
    chk_store_id_not_null, chk_store_id_unique, chk_open_date_valid, chk_open_date_not_future,
    chk_zone_valid, chk_pincode_6_digits, chk_capacity_positive,
    _row_num, _source_file, _run_id, _loaded_at
FROM flagged;
