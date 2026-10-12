-- include/sql/elt/stg_orders_merge.sql
-- Incremental load (Part B step 3): MERGE / upsert the next-day file into stg_orders.
--
--   order_id already in stg_orders  -> UPDATE the row (e.g. DELIVERED -> RETURNED)
--   order_id not in stg_orders      -> INSERT it as a new order
--
-- Only increment rows that are not FAILED are merged; failed ones stay visible in
-- stg_orders_increment (and in dq_results) but never overwrite a good row.
-- "AND NOT t.is_duplicate" makes sure we update the real copy of an order, not a
-- rejected duplicate copy (otherwise the duplicate would become PASSED and double count).
--
-- Running it twice is safe: the second time every order already matches, and the
-- UPDATE writes the same values again -> no new rows.

MERGE INTO stg_orders AS t
USING (SELECT * FROM stg_orders_increment WHERE dq_status <> 'FAILED') AS s
ON t.order_id = s.order_id AND NOT t.is_duplicate
WHEN MATCHED THEN UPDATE SET
    order_ts = s.order_ts, order_date = s.order_date, customer_id = s.customer_id,
    store_id = s.store_id, product_id = s.product_id, quantity = s.quantity,
    unit_price = s.unit_price, discount_pct = s.discount_pct, order_amount = s.order_amount,
    payment_mode = s.payment_mode, order_status = s.order_status, channel = s.channel,
    delivery_minutes = s.delivery_minutes, order_ts_text = s.order_ts_text,
    dq_status = s.dq_status, failure_reason = s.failure_reason, dq_warnings = s.dq_warnings,
    _row_num = s._row_num, _source_file = s._source_file, _run_id = s._run_id, _loaded_at = s._loaded_at
WHEN NOT MATCHED THEN INSERT BY NAME;
