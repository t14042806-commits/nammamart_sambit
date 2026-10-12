"""
## NammaMart ELT pipeline (Sambit)

**Extract → Load raw → Transform in the warehouse with SQL.** Every source row is kept
exactly as received in `raw_*`; cleaning happens afterwards in SQL.

| Layer   | Tables | Built by |
|---------|--------|----------|
| Raw     | raw_orders, raw_payments, raw_customers, raw_products, raw_stores | `load_raw_*` (append-only, all text) |
| Staging | stg_orders, stg_payments, stg_customers, stg_products, stg_stores | `include/sql/elt/stg_*.sql` |
| Mart    | mart_daily_store_kpis, mart_category_margin, mart_delivery_sla, mart_payment_reconciliation, mart_top_customers | `include/sql/elt/mart_*.sql` |

```
load_raw_customers ─► stg_customers ─┐
load_raw_products  ─► stg_products  ─┼─► stg_orders ─► merge_increment ─► stg_payments ─► marts (x5) ─► report
load_raw_stores    ─► stg_stores    ─┘        ▲               ▲                ▲                        ▲
load_raw_orders  ─────────────────────────────┘               │                │           dataset_checks
load_raw_increment (only if load_increment=true) ─────────────┘                │
load_raw_payments ─────────────────────────────────────────────────────────────┘
                                                        report ─► audit ─► notify ─► final_status
```

**Trigger with config:** `load_increment` (MERGE the next-day file) and `data_dir` (experiments).
All logic lives in `include/code/elt_steps.py` and `include/sql/elt/`.
"""

from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

from include.code import config, elt_steps

DAG_ID = elt_steps.DAG_ID   # "nammamart_elt_sambit"

default_args = {
    "owner": "sambit",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


def run_info():
    """Same as in the ETL DAG: run_id, run start time in IST, data folder, increment flag."""
    ctx = get_current_context()
    started = ctx["dag_run"].start_date or pendulum.now("UTC")
    return {
        "run_id": ctx["run_id"],
        "run_ts": pendulum.instance(started).in_tz(config.TIMEZONE).naive(),
        "data_dir": ctx["params"].get("data_dir") or str(config.DEFAULT_DATA_DIR),
        "load_increment": bool(ctx["params"].get("load_increment")),
    }


@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: keep every raw row, clean and model with SQL in DuckDB",
    schedule="0 9 * * 1-6",                                  # 9:00 AM Mon-Sat ...
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),   # ... in IST
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "load_increment": Param(False, type="boolean",
                                description="Also load orders_2026-10-01_increment.csv and MERGE it"),
        "data_dir": Param("", type="string",
                          description="Folder with the CSV files. Empty = include/data"),
    },
    tags=["nammamart", "elt", "sambit"],
    doc_md=__doc__,
)
def nammamart_elt_sambit():

    # ── 1. RAW: load every file exactly as received (parallel) ────────────────
    @task
    def load_raw(entity):
        info = run_info()
        return elt_steps.load_raw(entity, info["data_dir"], info["run_id"])

    @task
    def load_raw_increment():
        info = run_info()
        if not info["load_increment"]:
            print("[Raw] load_increment=false -> increment file not loaded this run")
            return None
        return elt_steps.load_raw("orders_increment", info["data_dir"], info["run_id"])

    raw_customers = load_raw.override(task_id="load_raw_customers")("customers")
    raw_products = load_raw.override(task_id="load_raw_products")("products")
    raw_stores = load_raw.override(task_id="load_raw_stores")("stores")
    raw_orders = load_raw.override(task_id="load_raw_orders")("orders")
    raw_payments = load_raw.override(task_id="load_raw_payments")("payments")
    raw_increment = load_raw_increment()

    # ── 2. STAGING: one SQL file per table ────────────────────────────────────
    @task
    def staging(entity):
        info = run_info()
        return elt_steps.build_staging(entity, info["run_id"], info["run_ts"])

    stg_customers = staging.override(task_id="stg_customers")("customers")
    stg_products = staging.override(task_id="stg_products")("products")
    stg_stores = staging.override(task_id="stg_stores")("stores")
    stg_orders = staging.override(task_id="stg_orders")("orders")
    stg_payments = staging.override(task_id="stg_payments")("payments")

    raw_customers >> stg_customers
    raw_products >> stg_products
    raw_stores >> stg_stores
    [raw_orders, stg_customers, stg_products, stg_stores] >> stg_orders

    # ── 3. INCREMENT: MERGE / upsert into stg_orders on order_id ──────────────
    @task
    def merge_increment(raw_increment_info):
        if raw_increment_info is None:
            print("[Merge] no increment this run - nothing to merge")
            return None
        info = run_info()
        return elt_steps.merge_increment(info["run_id"], info["run_ts"])

    merged = merge_increment(raw_increment)
    stg_orders >> merged

    # payments need every order id of this run (incl. increment) for the order_exists check
    [raw_payments, merged] >> stg_payments

    @task
    def dataset_checks():
        info = run_info()
        elt_steps.record_dataset_checks(info["run_id"], info["run_ts"])

    dataset_t = dataset_checks()
    merged >> dataset_t

    # ── 4. MARTS: business tables (parallel) ──────────────────────────────────
    @task
    def mart(name):
        info = run_info()
        return elt_steps.build_mart(name, info["run_id"], info["run_ts"])

    marts = [mart.override(task_id=name)(name) for name in elt_steps.MARTS]
    for m in marts:
        stg_payments >> m

    @task
    def report():
        return elt_steps.report()

    report_t = report()
    marts >> report_t
    dataset_t >> report_t

    # ── RUN AUDIT + ALERT + FINAL STATUS (same pattern as the ETL DAG) ────────
    @task(trigger_rule="all_done")
    def audit():
        ctx = get_current_context()
        info = run_info()
        status = "success" if ctx["ti"].xcom_pull(task_ids="report") else "failed"
        extracted, loaded, quarantined = elt_steps.audit_counts(info["run_id"], info["load_increment"])

        from include.code.warehouse import connect, write_pipeline_run
        con = connect()
        try:
            write_pipeline_run(con, info["run_id"], DAG_ID, ctx["dag_run"].start_date.replace(tzinfo=None),
                               extracted, loaded, quarantined, status)
        finally:
            con.close()
        return status

    @task(trigger_rule="all_done")
    def notify():
        from airflow.sdk import Variable
        from include.code.alerts import build_and_send_alert
        webhook = Variable.get("SLACK_WEBHOOK_URL", default=None)
        build_and_send_alert(DAG_ID, run_info()["run_id"], webhook_url=webhook)

    @task(trigger_rule="all_done")
    def final_status():
        status = get_current_context()["ti"].xcom_pull(task_ids="audit")
        if status != "success":
            raise AirflowFailException(f"Pipeline status = {status}. See pipeline_runs and the red task's log.")
        print("[Final] pipeline succeeded")

    report_t >> audit() >> notify() >> final_status()


# Calling the decorated function creates the DAG; Airflow finds it in this file.
elt_dag = nammamart_elt_sambit()
