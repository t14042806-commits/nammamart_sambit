"""
## NammaMart ETL pipeline (Sambit)

**Extract → Validate → Transform → Load.** Bad rows are stopped *before* the warehouse,
so the finance tables (`etl_fact_orders` + dimensions) only ever contain rows that passed
every Critical check.

```
extract_customers ─► validate_customers ─┐
extract_products  ─► validate_products  ─┼─► validate_orders ──────┐
extract_stores    ─► validate_stores    ─┘   validate_increment ───┤
extract_orders    ───────────────────────────────────┘             ├─► quarantine ─────┐
extract_increment (only if load_increment=true) ─────┘             ├─► record_dq ──────┤
extract_payments  ─► validate_payments ─────────────────────────── ┘                   │
                     transform ─► load ─► reconcile ─────────────────────────────────► report
                                                                    report ─► audit ─► notify ─► final_status
```

**Trigger with config** (Airflow UI → ▶ → Trigger DAG w/ config):
- `load_increment` = true → also load `orders_2026-10-01_increment.csv` (upsert on order_id)
- `data_dir` → folder with the CSVs (for Part F experiments). Empty = `include/data`

All logic lives in `include/code/etl_steps.py`; this file only wires tasks together.
"""

from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

from include.code import config, etl_steps

DAG_ID = etl_steps.DAG_ID   # "nammamart_etl_sambit"

# ── Part C: retries / timeouts for every task ─────────────────────────────────
default_args = {
    "owner": "sambit",
    "retries": 2,                                  # a missing file or a busy warehouse often fixes itself
    "retry_delay": timedelta(minutes=1),           # wait 1 minute between attempts
    "execution_timeout": timedelta(minutes=10),    # a task stuck > 10 min is killed (the run must finish before 9:30)
}


def run_info():
    """
    Facts about the current run that every task needs.
    run_ts = when this DAG run STARTED, in IST (all tasks of a run use the same value,
    even if a task is retried later). Anything after run_ts is "in the future".
    """
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
    description="NammaMart ETL: validate in Python before loading the finance star schema",
    # ── Part C: 9:00 AM, Monday(1) to Saturday(6) ──
    # cron = minute hour day-of-month month day-of-week
    schedule="0 9 * * 1-6",
    # The timezone in start_date makes the cron run in IST, not UTC.
    # 9:00 IST = 03:30 UTC (the UI may show 03:30 if it is set to display UTC).
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    # catchup=False: if the DAG is switched on (or Docker was off) we do NOT want Airflow
    # to replay every missed morning. Each run reloads the full source files anyway,
    # so one fresh run gives the correct numbers.
    catchup=False,
    max_active_runs=1,        # never two runs of this DAG writing the warehouse at the same time
    default_args=default_args,
    params={
        "load_increment": Param(False, type="boolean",
                                description="Also load orders_2026-10-01_increment.csv (upsert)"),
        "data_dir": Param("", type="string",
                          description="Folder with the CSV files. Empty = include/data"),
    },
    tags=["nammamart", "etl", "sambit"],
    doc_md=__doc__,
)
def nammamart_etl_sambit():

    # ── 1. EXTRACT: five parallel tasks, one per source file ──────────────────
    @task
    def extract(entity):
        info = run_info()
        return etl_steps.extract(entity, info["data_dir"], info["run_id"])

    @task
    def extract_increment():
        info = run_info()
        if not info["load_increment"]:
            print("[Extract] load_increment=false -> increment file not loaded this run")
            return None
        return etl_steps.extract("orders_increment", info["data_dir"], info["run_id"])

    # .override(task_id=...) re-uses one function to make several differently-named tasks
    customers_x = extract.override(task_id="extract_customers")("customers")
    products_x = extract.override(task_id="extract_products")("products")
    stores_x = extract.override(task_id="extract_stores")("stores")
    orders_x = extract.override(task_id="extract_orders")("orders")
    payments_x = extract.override(task_id="extract_payments")("payments")
    increment_x = extract_increment()

    # ── 2. VALIDATE: split every file into passed / failed ────────────────────
    @task
    def validate_dimension(entity, extracted):
        info = run_info()
        return etl_steps.validate_dimension(entity, extracted, info["run_id"], info["run_ts"])

    @task
    def validate_orders(extracted, customers_res, products_res, stores_res):
        if extracted is None:                      # increment not requested
            return None
        info = run_info()
        return etl_steps.validate_orders(extracted, customers_res, products_res, stores_res,
                                         info["run_id"], info["run_ts"])

    @task
    def validate_payments(extracted, orders_extracted, increment_extracted):
        info = run_info()
        return etl_steps.validate_payments(extracted, orders_extracted, increment_extracted,
                                           info["run_id"], info["run_ts"])

    customers_v = validate_dimension.override(task_id="validate_customers")("customers", customers_x)
    products_v = validate_dimension.override(task_id="validate_products")("products", products_x)
    stores_v = validate_dimension.override(task_id="validate_stores")("stores", stores_x)
    orders_v = validate_orders(orders_x, customers_v, products_v, stores_v)
    increment_v = validate_orders.override(task_id="validate_increment")(increment_x, customers_v, products_v, stores_v)
    payments_v = validate_payments(payments_x, orders_x, increment_x)

    all_results = [customers_v, products_v, stores_v, orders_v, increment_v, payments_v]

    # ── 3. QUARANTINE + DQ RESULTS ────────────────────────────────────────────
    @task
    def quarantine(results):
        return etl_steps.quarantine(results, run_info()["run_id"])

    @task
    def record_dq(results, orders_res, increment_res):
        info = run_info()
        etl_steps.record_dq_results(results, orders_res, increment_res, info["run_id"], info["run_ts"])

    quarantine_t = quarantine(all_results)
    dq_t = record_dq(all_results, orders_v, increment_v)

    # ── 4. TRANSFORM → 5. LOAD → 6. RECONCILE ─────────────────────────────────
    @task
    def transform(customers_res, products_res, stores_res, orders_res, increment_res):
        return etl_steps.transform(customers_res, products_res, stores_res, orders_res, increment_res,
                                   run_info()["run_id"])

    @task
    def load(paths):
        return etl_steps.load(paths, run_info()["run_id"])

    @task
    def reconcile(load_counts, payments_extracted, orders_extracted, increment_extracted):
        return etl_steps.reconcile(payments_extracted, orders_extracted, increment_extracted,
                                   run_info()["run_id"])

    paths = transform(customers_v, products_v, stores_v, orders_v, increment_v)
    load_counts = load(paths)
    recon = reconcile(load_counts, payments_x, orders_x, increment_x)

    # ── 7. REPORT (only after everything above succeeded) ─────────────────────
    @task
    def report(_recon, _dq, _quarantined):
        return etl_steps.report()

    report_t = report(recon, dq_t, quarantine_t)

    # ── RUN AUDIT: runs even if something failed (trigger_rule="all_done") ────
    @task(trigger_rule="all_done")
    def audit():
        ctx = get_current_context()
        info = run_info()
        ti = ctx["ti"]
        # xcom_pull returns None for a task that failed or never ran
        extracts = ti.xcom_pull(task_ids=["extract_customers", "extract_products", "extract_stores",
                                          "extract_orders", "extract_payments", "extract_increment"])
        rows_extracted = sum(x["rows"] for x in extracts if x)
        status = "success" if ti.xcom_pull(task_ids="report") else "failed"
        loaded, quarantined = etl_steps.audit_counts(info["run_id"])

        from include.code.warehouse import connect, write_pipeline_run
        con = connect()
        try:
            write_pipeline_run(con, info["run_id"], DAG_ID, ctx["dag_run"].start_date.replace(tzinfo=None),
                               rows_extracted, loaded, quarantined, status)
        finally:
            con.close()
        etl_steps.cleanup_old_work_dirs(keep_days=7)
        return status

    # ── ALERT (Slack if configured, else log) with optional LLM wording ───────
    @task(trigger_rule="all_done")
    def notify():
        from airflow.sdk import Variable
        from include.code.alerts import build_and_send_alert
        webhook = Variable.get("SLACK_WEBHOOK_URL", default=None)   # secret lives in Airflow, not in code
        build_and_send_alert(DAG_ID, run_info()["run_id"], webhook_url=webhook)

    # ── Make the DAG run RED when the pipeline failed ─────────────────────────
    # audit/notify use all_done, so they succeed even after a failure. Without this
    # last task Airflow would colour the whole run green (leaf tasks succeeded).
    @task(trigger_rule="all_done")
    def final_status():
        status = get_current_context()["ti"].xcom_pull(task_ids="audit")
        if status != "success":
            raise AirflowFailException(f"Pipeline status = {status}. See pipeline_runs and the red task's log.")
        print("[Final] pipeline succeeded")

    audit_t = audit()
    report_t >> audit_t >> notify() >> final_status()


# Calling the decorated function creates the DAG; Airflow finds it in this file.
etl_dag = nammamart_etl_sambit()
