-- include/sql/business_questions.sql
-- The six business questions, each answered by ONE simple query - from both pipelines.
-- Open include/output/warehouse_sambit.duckdb in VS Code SQLTools / DBeaver (stop the DAGs first,
-- DuckDB allows one writer) and run each query. Paste SQL + result into SUBMISSION.md.

-- ═══ Q1. Daily net revenue by zone and by store (delivered only, after discount) ═══
-- ETL
SELECT order_date, zone, store_id, store_name, net_revenue
FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id;
-- ELT
SELECT order_date, zone, store_id, store_name, net_revenue
FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id;
-- (zone totals for the month)
SELECT zone, ROUND(SUM(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP BY zone ORDER BY 2 DESC;

-- ═══ Q2. Revenue and gross margin by product category ═══
SELECT * FROM etl_category_margin;      -- ETL
SELECT * FROM mart_category_margin;     -- ELT

-- ═══ Q3. Average delivery time per store, % of delivered orders within 15 minutes ═══
-- ETL
SELECT f.store_id, s.store_name,
       ROUND(AVG(f.delivery_minutes), 1)                         AS avg_delivery_minutes,
       ROUND(100.0 * AVG(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 1) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY f.store_id, s.store_name ORDER BY f.store_id;
-- ELT
SELECT store_id, store_name, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla;

-- ═══ Q4. Revenue by payment mode + total amount that doesn't reconcile ═══
-- ETL
SELECT payment_mode, ROUND(SUM(net_revenue), 2) AS net_revenue
FROM etl_fact_orders GROUP BY payment_mode ORDER BY 2 DESC;
SELECT ROUND(SUM(ABS(difference)), 2) AS amount_not_reconciled, COUNT(*) AS mismatches
FROM etl_payment_reconciliation;
-- ELT
SELECT payment_mode, ROUND(SUM(order_amount), 2) AS net_revenue
FROM stg_orders WHERE dq_status <> 'FAILED' AND order_status = 'DELIVERED'
GROUP BY payment_mode ORDER BY 2 DESC;
SELECT reason, COUNT(*) AS mismatches, ROUND(SUM(ABS(difference)), 2) AS amount
FROM mart_payment_reconciliation GROUP BY ROLLUP (reason) ORDER BY reason NULLS LAST;

-- ═══ Q5. Top 10 customers by spend, with loyalty tier (masked contact details only) ═══
-- ETL
SELECT c.customer_id, c.customer_name, c.email_masked, c.phone_masked, c.loyalty_tier,
       ROUND(SUM(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY total_spend DESC LIMIT 10;
-- ELT
SELECT * FROM mart_top_customers WHERE spend_rank <= 10;

-- ═══ Q6. Rows rejected today, by file and by reason ═══
-- ETL (latest run)
SELECT source_file, failure_reason, COUNT(*) AS rows_rejected
FROM etl_rejected_rows
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_sambit' ORDER BY start_time DESC LIMIT 1)
GROUP BY ALL ORDER BY source_file, rows_rejected DESC;
-- ELT (staging tables always hold the latest run)
SELECT 'orders'    AS source_file, failure_reason, COUNT(*) AS rows_rejected FROM stg_orders    WHERE dq_status = 'FAILED' GROUP BY ALL
UNION ALL SELECT 'payments',  failure_reason, COUNT(*) FROM stg_payments  WHERE dq_status = 'FAILED' GROUP BY ALL
UNION ALL SELECT 'customers', failure_reason, COUNT(*) FROM stg_customers WHERE dq_status = 'FAILED' GROUP BY ALL
UNION ALL SELECT 'products',  failure_reason, COUNT(*) FROM stg_products  WHERE dq_status = 'FAILED' GROUP BY ALL
UNION ALL SELECT 'stores',    failure_reason, COUNT(*) FROM stg_stores    WHERE dq_status = 'FAILED' GROUP BY ALL
ORDER BY source_file, rows_rejected DESC;

-- ═══ Observability tables (screenshots for the submission) ═══
SELECT * FROM pipeline_runs ORDER BY start_time DESC;
SELECT dag_id, source_file, check_name, dimension, severity, rows_checked, rows_failed, status
FROM dq_results
WHERE run_id IN (SELECT run_id FROM pipeline_runs ORDER BY start_time DESC LIMIT 2)
ORDER BY dag_id, status, rows_failed DESC;
-- quality trend over time (bonus: chart this)
SELECT dag_id, run_id, SUM(rows_failed) FILTER (WHERE severity = 'Critical') AS critical_failures,
       SUM(rows_failed) FILTER (WHERE severity = 'Warning') AS warnings
FROM dq_results GROUP BY dag_id, run_id ORDER BY MIN(checked_at);

-- ═══ Part E Q8 audit example: one rejected order, as received, and proof it's not in revenue ═══
SELECT * FROM raw_orders WHERE order_id = 'ORD001296';                      -- exactly as received (ELT raw)
SELECT order_id, dq_status, failure_reason FROM stg_orders WHERE order_id = 'ORD001296';
SELECT COUNT(*) AS in_etl_fact FROM etl_fact_orders WHERE order_id = 'ORD001296';   -- 0 = not in CFO revenue
