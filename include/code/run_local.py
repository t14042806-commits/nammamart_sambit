"""
include/code/run_local.py
------------------------------
Run BOTH pipelines on your laptop WITHOUT Airflow or Docker, calling the same
step functions the DAG tasks call, in the same order. Great for:
  - checking your rule changes quickly
  - comparing ETL vs ELT totals (Part B step 4)

Run from inside nammamart_sambit/ (the folder with the Dockerfile); the venv is one level up:

    ..\\.venv\\Scripts\\python -m include.code.run_local
    ..\\.venv\\Scripts\\python -m include.code.run_local --increment
    ..\\.venv\\Scripts\\python -m include.code.run_local --data-dir include/data_experiments/exp05_bad_rows

What it does NOT do (Airflow does): scheduling, retries, parallelism, the UI.
"""

import argparse
from datetime import datetime

from include.code import config, elt_steps, etl_steps
from include.code.warehouse import connect, print_query, write_pipeline_run


def run_etl(run_id, run_ts, data_dir, increment):
    start = datetime.utcnow()
    x = {e: etl_steps.extract(e, data_dir, run_id) for e in ["customers", "products", "stores", "orders", "payments"]}
    x["orders_increment"] = etl_steps.extract("orders_increment", data_dir, run_id) if increment else None

    c = etl_steps.validate_dimension("customers", x["customers"], run_id, run_ts)
    p = etl_steps.validate_dimension("products", x["products"], run_id, run_ts)
    s = etl_steps.validate_dimension("stores", x["stores"], run_id, run_ts)
    o = etl_steps.validate_orders(x["orders"], c, p, s, run_id, run_ts)
    inc = etl_steps.validate_orders(x["orders_increment"], c, p, s, run_id, run_ts) if increment else None
    pay = etl_steps.validate_payments(x["payments"], x["orders"], x["orders_increment"], run_id, run_ts)

    results = [c, p, s, o, inc, pay]
    etl_steps.quarantine(results, run_id)
    etl_steps.record_dq_results(results, o, inc, run_id, run_ts)
    paths = etl_steps.transform(c, p, s, o, inc, run_id)
    etl_steps.load(paths, run_id)
    etl_steps.reconcile(x["payments"], x["orders"], x["orders_increment"], run_id)
    etl_steps.report()

    loaded, quarantined = etl_steps.audit_counts(run_id)
    con = connect()
    write_pipeline_run(con, run_id, etl_steps.DAG_ID, start, sum(v["rows"] for v in x.values() if v),
                       loaded, quarantined, "success")
    con.close()


def run_elt(run_id, run_ts, data_dir, increment):
    start = datetime.utcnow()
    for e in ["customers", "products", "stores", "orders", "payments"]:
        elt_steps.load_raw(e, data_dir, run_id)
    if increment:
        elt_steps.load_raw("orders_increment", data_dir, run_id)

    for e in ["customers", "products", "stores", "orders"]:
        elt_steps.build_staging(e, run_id, run_ts)
    if increment:
        elt_steps.merge_increment(run_id, run_ts)
    elt_steps.build_staging("payments", run_id, run_ts)
    elt_steps.record_dataset_checks(run_id, run_ts)
    for m in elt_steps.MARTS:
        elt_steps.build_mart(m, run_id, run_ts)
    elt_steps.report()

    extracted, loaded, quarantined = elt_steps.audit_counts(run_id, increment)
    con = connect()
    write_pipeline_run(con, run_id, elt_steps.DAG_ID, start, extracted, loaded, quarantined, "success")
    con.close()


def compare(etl_run_id, elt_run_id):
    """Part B step 4: do ETL and ELT agree?"""
    con = connect()
    print_query(con, """
        SELECT 'ETL' AS pipeline,
               (SELECT COUNT(*) FROM etl_fact_orders)                                   AS clean_orders,
               (SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders)                 AS net_revenue,
               (SELECT ROUND(SUM(gross_margin), 2) FROM etl_fact_orders)                AS gross_margin,
               (SELECT COUNT(*) FROM etl_payment_reconciliation)                        AS recon_rows
        UNION ALL
        SELECT 'ELT',
               (SELECT COUNT(*) FROM stg_orders WHERE dq_status <> 'FAILED'),
               (SELECT ROUND(SUM(net_revenue), 2) FROM mart_daily_store_kpis),
               (SELECT ROUND(SUM(gross_margin), 2) FROM mart_category_margin),
               (SELECT COUNT(*) FROM mart_payment_reconciliation)
    """, title="ETL vs ELT totals")
    print_query(con, f"""
        SELECT check_name,
               MAX(CASE WHEN dag_id = '{etl_steps.DAG_ID}' THEN rows_failed END) AS etl_failed,
               MAX(CASE WHEN dag_id = '{elt_steps.DAG_ID}' THEN rows_failed END) AS elt_failed
        FROM dq_results
        WHERE run_id IN ('{etl_run_id}', '{elt_run_id}')
        GROUP BY check_name
        HAVING etl_failed IS DISTINCT FROM elt_failed
    """, title="Checks where ETL and ELT disagree (should be empty)")
    con.close()


def main():
    parser = argparse.ArgumentParser(description="Run the NammaMart pipelines without Airflow")
    parser.add_argument("--increment", action="store_true", help="also load the next-day increment file")
    parser.add_argument("--data-dir", default=str(config.DEFAULT_DATA_DIR))
    parser.add_argument("--only", choices=["etl", "elt"], help="run just one pipeline")
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    run_ts = datetime.now()           # your laptop's local time - use IST on your machine
    etl_run, elt_run = f"local_etl__{stamp}", f"local_elt__{stamp}"

    if args.only in (None, "etl"):
        print("\n################ ETL ################")
        run_etl(etl_run, run_ts, args.data_dir, args.increment)
    if args.only in (None, "elt"):
        print("\n################ ELT ################")
        run_elt(elt_run, run_ts, args.data_dir, args.increment)
    if args.only is None:
        compare(etl_run, elt_run)


if __name__ == "__main__":
    main()
