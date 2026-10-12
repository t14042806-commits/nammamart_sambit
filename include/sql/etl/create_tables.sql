-- include/sql/etl/create_tables.sql
-- ETL star schema. Only rows that passed every Critical check are loaded here.
-- PII is already masked by Python before loading (see include/code/masking.py).
--
--                 etl_dim_customer
--                        |
--   etl_dim_store -- etl_fact_orders -- etl_dim_product
--
-- Every table carries _run_id / _loaded_at so you can tell which run wrote a row.
-- The fact table also has _source_file / _source_row  -> exact lineage to the CSV line.

CREATE TABLE IF NOT EXISTS etl_dim_customer (
    customer_id      VARCHAR,     -- key, e.g. C00001
    customer_name    VARCHAR,     -- MASKED: "Priya H."
    email_masked     VARCHAR,     -- MASKED: "pr***@example.com"
    phone_masked     VARCHAR,     -- MASKED: "97******21"
    city             VARCHAR,
    area             VARCHAR,
    pincode          VARCHAR,
    signup_date      DATE,
    loyalty_tier     VARCHAR,     -- Bronze / Silver / Gold / Platinum (standardised casing)
    is_active        VARCHAR,     -- Y / N
    dq_warnings      VARCHAR,     -- Warning checks this row failed (loaded anyway)
    _run_id          VARCHAR,
    _loaded_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_dim_product (
    product_id       VARCHAR,
    product_name     VARCHAR,
    category         VARCHAR,
    sub_category     VARCHAR,
    brand            VARCHAR,
    mrp              DOUBLE,
    cost_price       DOUBLE,
    unit             VARCHAR,
    is_perishable    BOOLEAN,
    launch_date      DATE,
    dq_warnings      VARCHAR,
    _run_id          VARCHAR,
    _loaded_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_dim_store (
    store_id                 VARCHAR,
    store_name               VARCHAR,
    zone                     VARCHAR,   -- 'Unknown' when the source had no valid zone
    area                     VARCHAR,
    pincode                  VARCHAR,
    open_date                DATE,
    capacity_orders_per_day  INTEGER,
    dq_warnings              VARCHAR,
    _run_id                  VARCHAR,
    _loaded_at               TIMESTAMP
    -- manager_name is deliberately NOT copied: it is employee personal data
    -- and no business question needs it.
);

CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id          VARCHAR,     -- one row per order (grain)
    order_ts          TIMESTAMP,   -- IST
    order_date        DATE,
    customer_id       VARCHAR,     -- -> etl_dim_customer
    store_id          VARCHAR,     -- -> etl_dim_store
    product_id        VARCHAR,     -- -> etl_dim_product
    quantity          INTEGER,
    unit_price        DOUBLE,
    discount_pct      DOUBLE,
    order_amount      DOUBLE,      -- after discount, as sent by the order system (validated)
    payment_mode      VARCHAR,
    order_status      VARCHAR,     -- DELIVERED / CANCELLED / RETURNED
    channel           VARCHAR,
    delivery_minutes  INTEGER,
    net_revenue       DOUBLE,      -- order_amount if DELIVERED, else 0
    cost_amount       DOUBLE,      -- quantity x cost_price
    gross_margin      DOUBLE,      -- net_revenue - cost_amount if DELIVERED, else 0
    within_15_min     BOOLEAN,     -- delivery_minutes <= 15 (NULL if not delivered)
    dq_warnings       VARCHAR,
    _source_file      VARCHAR,     -- orders.csv or orders_2026-10-01_increment.csv
    _source_row       INTEGER,     -- data row number in that file (1 = first row after header)
    _run_id           VARCHAR,
    _loaded_at        TIMESTAMP
);

-- Row keys of every rejected row, per run (no PII here - the full row is in the
-- quarantine CSV). Answers business question 6: rejected rows by file and reason.
CREATE TABLE IF NOT EXISTS etl_rejected_rows (
    run_id          VARCHAR,
    source_file     VARCHAR,
    row_key         VARCHAR,     -- order_id / payment_id / customer_id / ...
    source_row      INTEGER,
    failure_reason  VARCHAR,
    quarantine_file VARCHAR
);

-- One row per order/payment mismatch (Part A step 6)
CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (
    order_id         VARCHAR,
    payment_id       VARCHAR,
    order_status     VARCHAR,
    order_amount     DOUBLE,
    expected_amount  DOUBLE,     -- what the gateway SHOULD have collected (0 for returned/cancelled)
    amount_paid      DOUBLE,     -- what SUCCESS payments actually collected
    difference       DOUBLE,     -- amount_paid - expected_amount
    reason           VARCHAR,
    _run_id          VARCHAR
);
