-- include/sql/elt/stg_customers.sql
-- ELT staging for customers: SQL only. Reads THIS run's rows from raw_customers.
--
-- Pattern used by every stg_*.sql file (read top to bottom):
--   src     : this run's raw rows (all text, untouched)
--   std     : trim / fix casing / cast types   (TRY_ functions give NULL instead of an error)
--   flagged : one TRUE/FALSE column per quality check, named chk_<check_name>
--             (names match include/code/checks_catalog.py exactly)
--   final   : dq_status + failure_reason + dq_warnings, PII masked, raw PII dropped
--
-- Placeholders like {{run_id}} are filled in by warehouse.run_sql_file().

CREATE OR REPLACE TABLE stg_customers AS
WITH src AS (
    SELECT * FROM raw_customers WHERE _run_id = '{{run_id}}'
),

std AS (
    SELECT
        coalesce(trim(customer_id), '')   AS customer_id,
        coalesce(trim(full_name), '')     AS full_name,
        coalesce(trim(email), '')         AS email,
        coalesce(trim(phone), '')         AS phone,
        coalesce(trim(city), '')          AS city,
        coalesce(trim(area), '')          AS area,
        coalesce(trim(pincode), '')       AS pincode,
        try_strptime(trim(signup_date), '{{date_format}}')::DATE  AS signup_date,
        coalesce(loyalty_tier, '')        AS loyalty_tier_raw,
        -- 'BRONZE' / 'bronze' -> 'Bronze'
        upper(left(coalesce(trim(loyalty_tier), ''), 1)) || lower(substr(coalesce(trim(loyalty_tier), ''), 2)) AS loyalty_tier,
        coalesce(trim(is_active), '')     AS is_active,
        _row_num, _source_file, _run_id, _loaded_at
    FROM src
),

flagged AS (
    SELECT
        *,
        customer_id = ''                                                    AS chk_customer_id_not_null,
        -- first copy (lowest _row_num) wins; later copies are duplicates
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _row_num) > 1  AS chk_customer_id_unique,
        loyalty_tier NOT IN ({{valid_loyalty_tiers}})                       AS chk_loyalty_tier_valid,
        signup_date IS NULL                                                 AS chk_signup_date_valid,
        coalesce(signup_date > CAST('{{run_ts}}' AS TIMESTAMP)::DATE, false) AS chk_signup_date_not_future,
        NOT regexp_full_match(email, '[^@\s]+@[^@\s]+\.[A-Za-z]{2,}')        AS chk_email_format_valid,
        NOT regexp_full_match(phone, '[0-9]{10}')                           AS chk_phone_10_digits,
        NOT regexp_full_match(pincode, '[0-9]{6}')                          AS chk_pincode_6_digits,
        city = ''                                                           AS chk_city_not_null,
        (loyalty_tier_raw <> loyalty_tier AND loyalty_tier IN ({{valid_loyalty_tiers}})) AS chk_loyalty_tier_standardised
    FROM std
)

SELECT
    customer_id,
    -- ── PII masking (same output as include/code/masking.py) ──
    CASE
        WHEN full_name = '' THEN ''
        WHEN len(string_split_regex(full_name, '\s+')) = 1 THEN full_name
        ELSE string_split_regex(full_name, '\s+')[1] || ' '
             || left(string_split_regex(full_name, '\s+')[-1], 1) || '.'
    END                                                         AS customer_name,
    CASE WHEN email LIKE '%@%'
         THEN left(split_part(email, '@', 1), 2) || '***@' || substr(email, strpos(email, '@') + 1)
         ELSE '***' END                                         AS email_masked,
    CASE WHEN length(phone) < 5 THEN '***'
         ELSE left(phone, 2) || repeat('*', length(phone) - 4) || right(phone, 2) END AS phone_masked,
    city, area, pincode, signup_date, loyalty_tier, is_active,

    -- ── quality result ──
    CASE
        WHEN chk_customer_id_not_null OR chk_customer_id_unique OR chk_loyalty_tier_valid
          OR chk_signup_date_valid OR chk_signup_date_not_future            THEN 'FAILED'
        WHEN chk_email_format_valid OR chk_phone_10_digits OR chk_pincode_6_digits
          OR chk_city_not_null OR chk_loyalty_tier_standardised             THEN 'WARNING'
        ELSE 'PASSED'
    END AS dq_status,
    concat_ws('; ',
        CASE WHEN chk_customer_id_not_null   THEN 'customer_id_not_null' END,
        CASE WHEN chk_customer_id_unique     THEN 'customer_id_unique' END,
        CASE WHEN chk_loyalty_tier_valid     THEN 'loyalty_tier_valid' END,
        CASE WHEN chk_signup_date_valid      THEN 'signup_date_valid' END,
        CASE WHEN chk_signup_date_not_future THEN 'signup_date_not_future' END
    ) AS failure_reason,
    concat_ws('; ',
        CASE WHEN chk_email_format_valid        THEN 'email_format_valid' END,
        CASE WHEN chk_phone_10_digits           THEN 'phone_10_digits' END,
        CASE WHEN chk_pincode_6_digits          THEN 'pincode_6_digits' END,
        CASE WHEN chk_city_not_null             THEN 'city_not_null' END,
        CASE WHEN chk_loyalty_tier_standardised THEN 'loyalty_tier_standardised' END
    ) AS dq_warnings,

    -- keep every check flag: dq_results is counted from these columns
    chk_customer_id_not_null, chk_customer_id_unique, chk_loyalty_tier_valid, chk_signup_date_valid,
    chk_signup_date_not_future, chk_email_format_valid, chk_phone_10_digits, chk_pincode_6_digits,
    chk_city_not_null, chk_loyalty_tier_standardised,
    _row_num, _source_file, _run_id, _loaded_at
FROM flagged;
-- NOTE: full_name, email and phone (unmasked) are NOT selected above.
-- Unmasked PII exists only in raw_customers, which only restricted roles may read.
