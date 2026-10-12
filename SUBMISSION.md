# NammaMart Quick Commerce: ETL & ELT Submission (Sambit)

> **How to use this file:** sections marked **✏️ TODO** need your own observations from running the
> DAGs in Airflow (screenshots, timings, UI states). Everything else is drafted from the code and from
> local test runs (`include/code/run_local.py`). Read it, check it against your own runs, and rewrite
> it in your own words before you submit.

**Deliverables.** All my work is in `nammamart_sambit/`, a standalone Astro project (own Dockerfile and requirements). Start it with `astro dev start` from that folder. All paths below are relative to it.

| Item | Location |
|---|---|
| ETL DAG | `dags/nammamart_etl_sambit.py` |
| ELT DAG | `dags/nammamart_elt_sambit.py` |
| Helper code (rules, masking, steps, LLM, alerts) | `include/code/`, with a reading guide in `README.md` |
| SQL (staging, marts, ETL DDL/reports, audit tables) | `include/sql/` |
| Business questions (both pipelines) | `include/sql/business_questions.sql` |
| Astro project files | `Dockerfile`, `requirements.txt` (no pandas), `.astro/config.yaml`, `tests/dags/` |
| Warehouse | `include/output/warehouse_sambit.duckdb` (gitignored because it contains raw PII) |
| Quarantine | `include/output/quarantine/<file>_failed_<run_id>.csv` (gitignored) |

---

## Part 0: Discovery and data profiling

The profile comes from `python -m include.code.profile_data`. Every file has issues.

### Data Issues Log

| # | File | Row key (examples) | Column | Problem | Dimension | Rule implemented | Severity |
|---|---|---|---|---|---|---|---|
| 1 | orders | ORD001462, ORD001326 (12 ids) | order_id | 12 orders appear twice (exact duplicate rows) | Uniqueness | `order_id_unique`: keep first copy, quarantine the rest | Critical |
| 2 | orders | ORD002255, ORD000701 (18 rows) | customer_id | customer_id blank | Completeness | `customer_id_not_null` | Critical |
| 3 | orders | ORD000362 | order_ts | order_ts blank | Completeness | `order_ts_not_null` | Critical |
| 4 | orders | ORD000403 (`N/A`), ORD001610 (`2026-13-05`), ORD002012 (`31/09/2026 10:00`) | order_ts | 5 values aren't valid `YYYY-MM-DD HH:MM:SS` timestamps | Validity | `order_ts_format_valid` | Critical |
| 5 | orders | ORD002119, ORD001693, ORD001121 (7 rows) | order_ts | future dates (2026-10-21, 2026-11-02, 2027-01-01) | Timeliness | `order_ts_not_future` (vs run time) | Critical |
| 6 | orders | 3 rows | order_status | `Pending??` is not an allowed status | Validity | `order_status_valid` | Critical |
| 7 | orders | 2 rows | order_status | `delivered` (lower case) | Consistency | upper-case it; `order_status_standardised` warning | Warning |
| 8 | orders | 8 rows | payment_mode | `upi`, `UPI ` (trailing space), `cod`, `CARD` | Consistency | map to the standard value; `payment_mode_standardised` warning | Warning |
| 9 | orders | ORD001812 + 1 | payment_mode | `Cashh`: unknown value (deliberately not guessed) | Validity | `payment_mode_valid` | Critical |
| 10 | orders | ORD001296 (−2), ORD000469 (0) (10 rows) | quantity | zero or negative quantity | Accuracy | `quantity_positive` | Critical |
| 11 | orders | ORD002103 (250), ORD001468 (120), ORD001237 (150) | discount_pct | discount above 100% | Validity | `discount_pct_in_range` (0–100) | Critical |
| 12 | orders | ORD002305 (6757.5 vs 675.75), ORD000845 (18 rows) | order_amount | doesn't match qty × price × (1 − disc/100); several are about 10× too high | Accuracy | `order_amount_matches_formula` (±₹0.01) | Critical |
| 13 | orders | ORD001554, ORD001800 (8 rows) | delivery_minutes | DELIVERED but no delivery time | Completeness | `delivered_has_delivery_minutes` | Critical |
| 14 | orders | ORD000455 (1440), ORD001104 (900), ORD001037 (480) (6 rows) | delivery_minutes | hours instead of minutes | Accuracy | `delivery_minutes_believable` (1–120) | Critical |
| 15 | orders | ORD000986 (P9455) (9 rows) | product_id | product not in the product master | Consistency | `product_exists` | Critical |
| 16 | orders | ORD001799 (S99), ORD001046 (S17), ORD002126 (S00) (5 rows) | store_id | store not in the store master | Consistency | `store_exists` | Critical |
| 17 | orders | 18 + 59 rows | customer_id / product_id | refer to customers/products that are themselves quarantined (future signup, missing category, negative MRP) | Consistency | FK checks use the **clean** masters | Critical |
| 18 | payments | PAY0000509, PAY0000755 (4 ids) | payment_id | exact duplicate payment rows | Uniqueness | `payment_id_unique` | Critical |
| 19 | payments | PAY0002217–PAY0002224 | order_id | 8 payments for orders `ORD9xxxxx` that don't exist, all at the same second (likely test entries) | Consistency | `order_exists` | Critical |
| 20 | payments | PAY0001568 (−43.05), PAY0000116 (0.0) (10 rows) | amount_paid | SUCCESS payment that is zero or negative | Accuracy | `success_amount_positive` | Critical |
| 21 | payments | 7 rows | payment_ts | future dates (2027-01-01) | Timeliness | `payment_ts_not_future` | Critical |
| 22 | payments | `Cashh`, `upi`, `UPI `, `cod`, `CARD` | payment_mode | same messy values as orders | Validity / Consistency | map, or quarantine `Cashh` | Critical / Warning |
| 23 | payments | PAY0000586 (paid 2.6 vs 102.6), PAY0000354 (14 rows) | amount_paid | SUCCESS amount ≠ order_amount | Consistency | listed in the **reconciliation** table (not quarantined: the payment is real) | n/a |
| 24 | payments | 301 rows | gateway_ref | blank, but **all** are Cash on Delivery, so this is expected | Completeness | `gateway_ref_present` only for non-cash payments (0 failures) | Warning |
| 25 | payments ↔ orders | 45 + 27 orders | n/a | DELIVERED orders with only a FAILED payment, or none at all | Consistency | reconciliation: `MISSING_SUCCESS_PAYMENT` | n/a |
| 26 | customers | C00226, C00036, C00252, C00103, C00313 | customer_id | 5 ids appear twice with **different** phone/area | Uniqueness | `customer_id_unique`: keep first, quarantine second | Critical |
| 27 | customers | C00400 (2027-01-05), C00398, C00038 | signup_date | signup in the future | Timeliness | `signup_date_not_future` | Critical |
| 28 | customers | C00337 (`N/A`), C00307 (no `@`), C00063 (no `.com`) (12 rows) | email | invalid email | Validity | `email_format_valid` | Warning |
| 29 | customers | C00151 (9 digits), C00290 (7 digits) (7 rows) | phone | not 10 digits | Validity | `phone_10_digits` | Warning |
| 30 | customers | C00132, C00083, C00166 | pincode | `5600XX` | Validity | `pincode_6_digits` | Warning |
| 31 | customers | 6 rows | city | blank | Completeness | `city_not_null` | Warning |
| 32 | customers | 8 rows | loyalty_tier | `bronze`, `BRONZE`, `SILVER` | Consistency | title-case it; `loyalty_tier_standardised` | Warning |
| 33 | products | P0030, P0140 | product_id | duplicate with an `(old)` name and lower MRP | Uniqueness | `product_id_unique` (keep the first, current row) | Critical |
| 34 | products | 3 products | category | blank | Completeness | `category_not_null` | Critical |
| 35 | products | P0109 | mrp | −203.0 | Accuracy | `mrp_positive` | Critical |
| 36 | products | P0019, P0073, P0074, P0109, P0117 | cost_price | cost_price > mrp (sold at a loss?) | Accuracy | `cost_price_not_above_mrp` | Warning (decision, see Q4) |
| 37 | stores | S07 (BTM Layout) | zone | blank | Completeness | `zone_valid`: shown as `Unknown` | Warning (decision) |
| 38 | stores | S12 (Rajajinagar) | capacity_orders_per_day | 0 | Accuracy | `capacity_positive` | Warning |
| 39 | increment | ORD000017 … ORD001990 | order_status | 10 corrections DELIVERED → RETURNED, plus 150 new orders on 2026-10-01 | n/a | MERGE / upsert on order_id | n/a |
| 40 | increment | ORD002542, ORD002465, ORD002486, ORD000822, … (6 rows) | product_id / customer_id | refer to quarantined products (P0068, P0051, P0109, P0060) or a future-signup customer (C00398) | Consistency | same FK rules as September | Critical |

**Totals (September files, first run):** 5,212 rows read; 221 quarantined
(orders 176, payments 31, customers 8, products 6, stores 0); 2,236 clean orders.

### Questions for the client

1. **Negative or zero quantity** (10 orders, and matching negative SUCCESS payments): is this how returns are recorded, or a data-entry error? Today they're rejected.
2. **Duplicate customer IDs with different phone/area** (5 customers): which row is current? Should the CRM merge them? Today the first row wins.
3. **`Cashh`**: can we safely map it to *Cash on Delivery*? Today it's quarantined so revenue isn't booked to a guessed payment mode.
4. **cost_price > MRP** (5 products): are these loss-leaders, or is the cost wrong? Today they load with a warning and show negative margin.
5. **Customers with future signup dates**: their 18 orders are real but rejected. Can the CRM date be fixed, or should orders be accepted anyway?
6. **Store S07 has no zone.** BTM Layout is usually counted as South. Please confirm.
7. **Returns that arrive later** (e.g. delivered 3 Sep, returned 1 Oct): should September revenue be restated, or should the return be booked in October?

---

## Parts A & B: Pipelines

| | ETL (`nammamart_etl_sambit`) | ELT (`nammamart_elt_sambit`) |
|---|---|---|
| Flow | extract (5 parallel) → validate in Python → quarantine + dq_results → transform → load star schema → reconcile → report | load raw (5 parallel, text only) → staging SQL → MERGE increment → stg_payments → 5 marts (parallel) → report |
| Bad rows | never reach the warehouse; CSV in `include/output/quarantine/` + keys in `etl_rejected_rows` | kept in `raw_*`; marked `dq_status='FAILED'` + `failure_reason` in `stg_*`; marts read only non-FAILED rows |
| PII | masked in Python before load | masked in `stg_customers.sql`; unmasked only in `raw_customers` |
| Idempotent | DELETE + INSERT in one transaction | `CREATE OR REPLACE` for stg/mart; raw is append-only |
| Increment | `load_increment=true` → upsert on order_id (DELETE + INSERT) | `load_increment=true` → `MERGE INTO stg_orders` |

**Do ETL and ELT totals match?** Yes. `run_local.py` compares them, and every check's failure count is identical:

| Run | Clean orders | Net revenue (₹) | Gross margin (₹) | Reconciliation rows |
|---|---|---|---|---|
| September only, ETL | 2,236 | 10,82,265.55 | 2,54,155.81 | 245 |
| September only, ELT | 2,236 | 10,82,265.55 | 2,54,155.81 | 245 |
| + increment, ETL | 2,381 | 11,54,576.40 | 2,70,714.35 | 399 |
| + increment, ELT | 2,381 | 11,54,576.40 | 2,70,714.35 | 399 |

They match because both pipelines use the same check names (`checks_catalog.py`), the same constants (`config.py`) and the same rule logic.
One small, expected difference: `mart_daily_store_kpis` has 447 rows and `etl_daily_zone_revenue` has 444, because the ELT KPI table also lists store-days that had only cancelled or returned orders.

**Increment result:** 9 orders updated (DELIVERED → RETURNED), 145 inserted, 6 rejected. The assignment expects 10 + 150.
The 6 rejected rows reference products or a customer that are quarantined in the master data. One of them, ORD000822, is the 10th correction, and its original September row was rejected for the same reason (product P0060). This is consistent behaviour, but it's a client question (see Q5 above).

✏️ **TODO:** screenshots of both DAGs in Graph view after a green run.

---

## Part C: Scheduling

| Question | Answer |
|---|---|
| Cron expression | `0 9 * * 1-6`: minute 0, hour 9, every day-of-month, every month, Monday(1)–Saturday(6) |
| Timezone | `start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")`, so Airflow evaluates the cron in IST |
| Time in UTC in the UI | **03:30 UTC** (IST = UTC+5:30, no daylight saving). In the UI's timezone menu you can switch the display to IST and see 09:00. |
| If `start_date` were UTC | the cron would mean 09:00 **UTC** = **14:30 IST**, five hours after the 9:30 stand-up. The numbers would be late every day. |
| Laptop / Docker off at 9:00 | nothing runs; the scheduler is off. When Airflow starts again, with `catchup=False` it creates **one** run for the most recent missed slot, not one per missed day. The report is late that day. In production the scheduler must run on an always-on server (a VM, or Astro / managed Airflow). |
| `catchup=False`, why | every run reloads the full source files, so one fresh run gives correct numbers. Replaying every missed morning would add load and many duplicate-looking runs for no benefit. |
| Retries | `retries=2`, `retry_delay=1 min`: covers short glitches (a file arriving a bit late, the DuckDB file briefly locked by the other DAG). |
| `execution_timeout=10 min` | a normal task takes seconds; a stuck task is killed so the run still finishes (or fails visibly) well before 9:30. |
| `max_active_runs=1` | two runs of the same DAG never write the warehouse at the same time. |

✏️ **TODO:** screenshot of the DAG list or details showing the schedule and the *Next run* time.

---

## Part D: Data quality dimensions

All checks are listed in `include/code/checks_catalog.py`. They're implemented in Python (`rules.py`) and in SQL (`stg_*.sql`, columns `chk_<name>`), and every run writes one row per check to `dq_results`.

| Dimension | Example checks (file) | Severity |
|---|---|---|
| Completeness | `customer_id_not_null`, `delivered_has_delivery_minutes` (orders); `category_not_null` (products); `city_not_null` (customers); `zone_valid` (stores); `volume_vs_previous_run` (dataset) | Critical / Warning |
| Uniqueness | `order_id_unique`, `payment_id_unique`, `customer_id_unique`, `product_id_unique`, `store_id_unique` | Critical |
| Validity | `order_status_valid`, `payment_mode_valid`, `order_ts_format_valid`, `discount_pct_in_range`; `email_format_valid`, `phone_10_digits`, `pincode_6_digits` | Critical / Warning |
| Accuracy | `quantity_positive`, `order_amount_matches_formula`, `delivery_minutes_believable`, `unit_price_not_above_mrp`, `mrp_positive`, `success_amount_positive`; `cost_price_not_above_mrp` | Critical / Warning |
| Consistency | `customer_exists`, `product_exists`, `store_exists`, `order_exists`; `*_standardised` (casing/mapping); payment reconciliation table | Critical / Warning |
| Timeliness | `order_ts_not_future`, `payment_ts_not_future`, `signup_date_not_future`; `latest_order_is_fresh` (dataset) | Critical / Warning |

**Severity decisions (to confirm with the client):** `cost_price_not_above_mrp` and a missing store zone are Warnings, not Critical, because rejecting them would drop real revenue (store S07 alone has about 176 orders).

✏️ **TODO:** screenshot of `SELECT * FROM dq_results ...` (query in `business_questions.sql`).

---

## Business questions (both pipelines)

The SQL for every question is in `include/sql/business_questions.sql`.

✏️ **TODO:** paste the result of each query.

| # | Question | ETL source | ELT source |
|---|---|---|---|
| 1 | Daily net revenue by zone / store | `etl_daily_zone_revenue` | `mart_daily_store_kpis` |
| 2 | Revenue and gross margin by category | `etl_category_margin` | `mart_category_margin` |
| 3 | Avg delivery time per store, % within 15 min | `etl_fact_orders` + `etl_dim_store` | `mart_delivery_sla` |
| 4 | Revenue by payment mode + unreconciled amount | `etl_fact_orders`, `etl_payment_reconciliation` | `stg_orders`, `mart_payment_reconciliation` |
| 5 | Top 10 customers (masked) | `etl_fact_orders` + `etl_dim_customer` | `mart_top_customers` |
| 6 | Rows rejected today by file and reason | `etl_rejected_rows` | `stg_*` where `dq_status='FAILED'` |

Values from the local test run (September only), for reference:

- **Zone revenue (₹):** East 2,80,748.15 · West 2,77,449.05 · South 2,30,322.40 · North 2,19,407.20 · Unknown (S07) 74,338.75
- **Category margin:** Staples has the highest revenue (₹2,62,374.50, 22.1% margin). Dairy & Bakery has the best margin (26.5%).
- **Reconciliation (September):** 245 mismatches, about ₹1,85,736 in total:
  - 157 payments for rejected orders
  - 68 delivered orders without a successful payment
  - 12 amount mismatches
  - 8 payments with no order at all

---

## Part E: Data governance

1. **Ownership.**
   - Orders: Ops / the order-management team.
   - Payments: Finance.
   - Customers: CRM / Marketing (personal data, so the Data Protection Officer is also involved).
   - Products: Category / Catalogue team.
   - Stores: Ops (store master).
   - Warehouse tables: the data team. Marts belong to the team that consumes them (Finance for revenue and reconciliation, Ops for the SLA mart).
   - A change to a quality rule (e.g. the 120-minute limit in `config.MAX_DELIVERY_MINUTES`) is proposed by the data team and approved by the owner of the data (Ops), with Finance signing off when revenue is affected. It ships as a reviewed Git pull request, so the change has an audit trail.

2. **Data dictionary.** The column-level dictionary is in the comments of `include/sql/etl/create_tables.sql` (type, meaning and allowed values per column). Summary:

   | Table.column | Type | Meaning | Source | Allowed values | PII |
   |---|---|---|---|---|---|
   | fact.order_id | VARCHAR | order key (grain: 1 row per order) | orders.csv | `ORD` + 6 digits | No |
   | fact.order_ts / order_date | TIMESTAMP / DATE | order time, IST | orders.order_ts | not in the future | No |
   | fact.customer_id / store_id / product_id | VARCHAR | foreign keys to the dimensions | orders.csv | must exist in the clean dimension | No (pseudonymous id) |
   | fact.order_amount | DOUBLE | amount after discount | orders.csv | = qty × price × (1 − disc/100) | No |
   | fact.net_revenue | DOUBLE | order_amount if DELIVERED, else 0 | derived | ≥ 0 | No |
   | fact.gross_margin | DOUBLE | net_revenue − qty × cost_price (DELIVERED only) | derived | any | No |
   | fact.within_15_min | BOOLEAN | delivery_minutes ≤ 15 | derived | true / false / NULL | No |
   | fact.order_status | VARCHAR | order state | orders.csv | DELIVERED, CANCELLED, RETURNED | No |
   | fact._source_file / _source_row / _run_id | VARCHAR / INT / VARCHAR | lineage | pipeline | n/a | No |
   | dim_customer.customer_name | VARCHAR | first name + last initial | customers.full_name | n/a | **Masked** |
   | dim_customer.email_masked / phone_masked | VARCHAR | `pr***@domain`, `98******21` | customers | n/a | **Masked** |
   | dim_customer.loyalty_tier | VARCHAR | tier | customers | Bronze, Silver, Gold, Platinum | No |
   | dim_product.mrp / cost_price | DOUBLE | list price / cost | products | mrp > 0 | No |
   | dim_store.zone | VARCHAR | zone | stores | North, South, East, West, Unknown | No |

3. **Lineage.** Take `mart_daily_store_kpis` for 2026-09-01, store S08 (East): `net_revenue = ₹2,984.70`.
   - **ELT:** `SELECT order_id, order_amount, _source_file, _row_num FROM stg_orders WHERE order_date='2026-09-01' AND store_id='S08' AND order_status='DELIVERED' AND dq_status<>'FAILED'` returns 4 orders: ORD000008 (₹2,060.00, row 1049), ORD000051 (₹73.60, row 228), ORD000056 (₹39.90, row 192) and ORD000079 (₹811.20, row 1350), all from `orders.csv`. Each one can be shown exactly as received with `SELECT * FROM raw_orders WHERE _row_num IN (...) AND _run_id = ...`.
   - **ETL:** the same query on `etl_fact_orders` gives the same 4 rows, thanks to `_source_file` and `_source_row`. But the original text is only in the source CSV, which may have been overwritten by the next day's export.
   - **ELT is easier:** the raw layer keeps every version, and lineage is just SQL joins inside one database.

4. **Personal data.**
   - PII columns: `customers.full_name`, `email`, `phone`, plus `area` and `pincode` (location; personal when combined with other data). `stores.manager_name` is employee personal data.
   - How it's masked: email becomes `pr***@example.com`, phone `98******21`, and the name "Priya H." (`masking.py` and the same logic in `stg_customers.sql`). `manager_name` isn't copied to reporting tables at all.
   - Why mask rather than delete: support staff can still recognise a customer, `customer_id` still joins, and analytics still work, while nobody can contact or identify a person from a dashboard.
   - Law: India's **Digital Personal Data Protection Act, 2023 (DPDP Act)** and the DPDP Rules. NammaMart is the *data fiduciary*. It must:
     - process personal data only for the purpose the customer consented to (purpose limitation)
     - collect only what it needs (data minimisation)
     - protect the data with reasonable security safeguards
     - report breaches to the Data Protection Board and to affected people
     - erase data when the purpose is over
     - respect people's rights to access, correct and erase their data

5. **Access control.**

   | Layer | Who can read it |
   |---|---|
   | Raw (unmasked phone/email in `raw_customers`) | the pipeline service account, plus a named data engineer for break-glass debugging; access is logged. Nobody in ops or analytics. |
   | Staging | data team |
   | Marts | Finance (revenue, reconciliation), Ops / store managers (KPIs, SLA), Analytics (all marts) |
   | Quarantine files | data team and data owners investigating issues (they contain rows as received, which may include PII) |

   In DuckDB this means separate files or folders with OS permissions. On a server database (e.g. Postgres) it would be roles plus GRANTs per schema.

6. **Retention.**
   - Raw data: as long as finance and audit need it (e.g. 8 years for financial records under Indian tax rules). Customer PII inside raw should be minimised or erased once the purpose ends, per DPDP.
   - Quarantine files: 90 days. They are for fixing issues, not long-term storage.
   - Intermediate work files: 7 days (`cleanup_old_work_dirs`).
   - Masked customer dimension: while the customer is active plus a defined period. After that, anonymise (drop the masked name, keep `customer_id` and aggregates) or delete, with the deletion logged.

7. **Secrets.**
   - `SLACK_WEBHOOK_URL` and `LLM_API_KEY` are **Airflow Variables** (Admin → Variables, or `airflow_settings.yaml`, which is gitignored), read with `Variable.get()` at run time.
   - They must never be in a DAG file or on GitHub. Anyone with read access to the repo, or to its history even after a "delete", could post to the Slack channel or spend money on the API key. Bots scan public GitHub for keys within minutes.
   - In production, use a secrets backend (Vault, AWS/GCP/Azure secret manager).

8. **Audit.** Example: ORD001296 (quantity −2).
   - **ETL:**
     - the row exactly as received is in `include/output/quarantine/orders_failed_<run_id>.csv`, with `failure_reason = quantity_positive`
     - `etl_rejected_rows` holds its key, source row and reason
     - `SELECT COUNT(*) FROM etl_fact_orders WHERE order_id='ORD001296'` returns 0, so the order isn't in the CFO total
   - **ELT:**
     - `SELECT * FROM raw_orders WHERE order_id='ORD001296'` shows it exactly as received
     - `stg_orders` shows `dq_status='FAILED'` and the reason
     - every mart filters `dq_status <> 'FAILED'`, so it can't be in revenue
   - Queries are at the bottom of `business_questions.sql`.

9. **Change history.** ORD000017 was DELIVERED on 1 September and RETURNED in the 1 October file.
   - **What each pipeline shows:** after the incremental load, **both** pipelines overwrite the status. September revenue drops from ₹10,82,265.55 to ₹10,78,602.85 (−₹3,662.70 for the 9 corrected orders that passed).
   - **What finance wants:** usually two views. *As-reported* (what we told the COO on 1 Oct, which shouldn't silently change) and *restated* (current truth).
   - **How to keep both:**
     - **ELT:** raw keeps both versions (`_source_file` and `_loaded_at`), so an "as of date X" view can be rebuilt with SQL.
     - **ETL:** overwrites in place. Keeping history would need a status-history table or SCD Type 2 (`valid_from` / `valid_to`) on the fact.
     - A monthly snapshot table, frozen at month-end close, gives finance the as-reported number.

---

## Part F: Observability tests

Run audit: the `audit` task (`trigger_rule="all_done"`) writes one row per run to `pipeline_runs`. The `final_status` task then fails the DAG run if the audit status is `failed`, so a failure can't hide behind a green leaf task.

Experiment data: `python -m include.code.make_experiment_data` creates broken **copies** in `include/data_experiments/`. Trigger with `{"data_dir": "/usr/local/airflow/include/data_experiments/<name>"}`. The originals are never edited, so there's nothing to restore.

"Local run" means the result from `run_local.py`. ✏️ **TODO:** repeat each experiment in Airflow and fill in what the UI shows.

| # | Experiment | Expected | Observed | Where I saw it | What I would change |
|---|---|---|---|---|---|
| 1 | Happy path | all tasks green; 5 extracts and 5 raw loads run in parallel; 221 rows quarantined | local run: ETL and ELT both 2,236 clean orders, 221 quarantined. ✏️ task durations from Gantt view | Grid / Gantt view, `pipeline_runs` | n/a |
| 2 | Re-run | fact/mart counts unchanged; ELT raw grows by 5,212 rows (append-only, correct: it's the audit trail) | local run: second run gave identical totals; `raw_orders` grew 2,412 → 4,824 | `[Raw] ... now holds N rows` log line; `pipeline_runs` | raw retention policy (Part E Q6) |
| 3 | Incremental load | 10 updated, 150 inserted, no duplicates | local run: 9 updated, 145 inserted, 6 rejected (quarantined masters); `stg_orders` 2,412 → 2,557; same in ETL | `merge_increment` / `load` logs; `dq_results` for `orders_increment` | client decision on Q5 |
| 4 | Missing `products.csv` | `extract_products` / `load_raw_products`: up_for_retry ×2 → failed; downstream upstream_failed; audit writes `failed`; notify sends alert; final_status red | local run: `FileNotFoundError: Source file not found: .../products.csv`. ✏️ UI states and retry timing | Grid view (orange → red), task log, `pipeline_runs.status='failed'`, Slack/log alert | file-arrival sensor before extract |
| 5 | 5 new bad rows | each quarantined with its own reason | local run: ORD900001 `customer_id_not_null`; ORD900002 `quantity_positive; order_amount_matches_formula`; ORD900003 `store_exists`; ORD900004 `order_status_valid`; ORD900005 `order_ts_not_future` | quarantine CSV, `dq_results` counts +1 each | n/a |
| 6 | Rename `order_amount` → `total` | fails loudly at extract / raw load, no silent wrong load | local run: `SchemaError: orders.csv: missing expected column(s) ['order_amount']` | `extract_orders` / `load_raw_orders` log | schema contract agreed with the source team |
| 7 | Only 50 orders | `volume_vs_previous_run` = WARN | local run: `orders rows now = 50, previous run = 2417, big drop = True` → WARN | `dq_results`, alert message | make a > 90% drop Critical (stop the report) |
| 8 | `unit_price` ×100 for store S05 | `unit_price_not_above_mrp` catches it | local run: 142 S05 orders quarantined; total net revenue fell ₹10,82,265.55 → ₹10,04,859.90 (−₹77,405.65, i.e. S05 disappears from `mart_daily_store_kpis`) | `dq_results`, mart totals | per-store volume/revenue drift check so a vanished store raises an alert |
| 9 | Latest `order_ts` 30 days old | `latest_order_is_fresh` = WARN | local run: latest valid order 2026-09-12 vs run 2026-10-12 → WARN (note: the real September data also always warns, since it's historical) | `dq_results` | Critical in production (stale = don't send the report) |
| 10 | Recovery: fix the cause, **Clear** the failed task | only the failed task and its downstream tasks re-run; raw has no duplicate rows (same-run delete) | ✏️ Airflow only | Grid view, task try numbers | n/a |

**The five pillars of data observability**

| Pillar | How this pipeline covers it | Strength |
|---|---|---|
| Freshness | `latest_order_is_fresh`; `pipeline_runs` start/end times; 9:00 schedule | medium (Warning only) |
| Volume | rows extracted / loaded / quarantined per run; `volume_vs_previous_run` | good |
| Schema | header check before every extract / raw load; explicit DDL | **strongest**: fails loudly |
| Distribution | value-range rules (formula, price ≤ MRP, 1–120 min) | **weakest**: no statistical drift (mean/percentile per store) |
| Lineage | `_source_file`, `_source_row`, `_run_id` in every table; raw kept forever in ELT | good (ELT), medium (ETL) |

---

## Part G: Recommendation to Sameer (CTO)

> ✏️ This is a **draft** based on the local test runs. Update it with what you observe in Airflow.

**To:** Sameer, CTO, NammaMart · **From:** Sambit, FDE · **Subject:** ETL or ELT for the 9 AM report

**Recommendation: ELT, with an ETL-style quality gate in front of finance.** Load every export into a raw layer exactly as received, clean and model it in SQL, and let finance-facing marts read **only** rows that passed every Critical check.

**Evidence from running both pipelines side by side on the same data**
- **Same numbers.** Both produce identical September revenue (₹10,82,265.55), margin, rejected-row counts per rule, and 245 payment mismatches. The quality guarantee finance asked for comes from the *rules*, not from the ETL/ELT choice.
- **Auditability.** In ELT every rejected order can be shown exactly as received with one SQL query, and a mart number can be traced back to file rows. In ETL the raw evidence lives only in quarantine CSVs and the original files.
- **History.** When ORD000017 was returned a month later, both pipelines updated September revenue. Only ELT still holds the original DELIVERED record, so finance can have both the "as reported" and the "restated" view.
- **Ease of change.** A new question (e.g. revenue by channel) or a rule change in ELT is a SQL edit plus a re-run over data we already have. In ETL, anything not loaded is gone and has to be re-extracted.
- **Data-quality guarantee.** The ETL property finance wants ("a bad row never reaches the CFO's dashboard") holds in ELT too: marts filter `dq_status <> 'FAILED'`, and the `final_status` task turns the run red if anything fails.

**Growth (1000× ≈ 2.4M orders/month).** Row-by-row Python validation (ETL) gets slow and memory-heavy at that size. Set-based SQL in a columnar engine (DuckDB today; MotherDuck, BigQuery, Snowflake or Postgres later) scales much better and needs only SQL skills. DuckDB's single-writer limit means we'd move the warehouse to a server engine before that point.

**Cost.** Both use only open-source tools today. ELT stores more (raw copies of every run), but storage is the cheapest part. Raw retention keeps it bounded.

**Top 3 risks of the recommended design**
1. **Raw layer holds unmasked PII.** It needs strict access control, logging and DPDP-compliant retention and erasure from day one.
2. **SQL rule sprawl.** Many `stg_*.sql` files can drift apart. Keep one check catalog, code review for rule changes, and automated tests comparing expected rejections.
3. **Operational dependence on one machine.** If the laptop or Docker host is off at 9:00, there's no report. Move Airflow and the warehouse to an always-on managed environment, and add alerting (Slack is already wired via an Airflow Variable).

---

## Bonus done

- Slack alert with an Airflow Variable (`SLACK_WEBHOOK_URL`) on every run, with optional LLM-written wording through a provider-agnostic `llm.py`. The provider is switched with the `LLM_PROVIDER` Variable.
- Data-quality trend query from `dq_results` (`business_questions.sql`). ✏️ chart it.

## Screenshot checklist ✏️

- [ ] Both DAGs in Graph view (green run)
- [ ] Schedule showing 9:00 AM IST and the next-run time
- [ ] One quarantine file (open the CSV in VS Code, Rainbow CSV)
- [ ] `dq_results`, `pipeline_runs` and the payment reconciliation table query results
- [ ] Experiment 4: a task in `up_for_retry` and then `failed`
