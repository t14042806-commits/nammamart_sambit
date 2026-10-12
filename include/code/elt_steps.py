"""
include/code/elt_steps.py
------------------------------
The ELT pipeline, one function per Airflow task:

    load_raw (x5, + increment)  ->  staging SQL  ->  MERGE increment  ->  mart SQL  ->  report

Python here does only two things:
  1. copy each CSV into a raw_* table EXACTLY as received (all text, no cleaning)
  2. run the .sql files in include/sql/elt/ in the right order
All cleaning, typing, masking and quality rules are SQL - that's what makes it ELT.
"""

from pathlib import Path

from include.code import config, rules
from include.code.checks_catalog import checks_for
from include.code.csv_utils import read_csv_rows
from include.code.warehouse import (
    connect, print_query, run_sql_file, sql_text_value, utc_now, write_dq_results, previous_orders_row_count,
)

DAG_ID = f"nammamart_elt_{config.LEARNER}"

RAW_TABLE = {
    "orders": "raw_orders", "orders_increment": "raw_orders",   # same table, told apart by _source_file
    "payments": "raw_payments", "customers": "raw_customers",
    "products": "raw_products", "stores": "raw_stores",
}


def _params(run_id, run_ts, **extra):
    """Placeholders available to every ELT .sql file."""
    p = {"run_id": sql_text_value(run_id), "run_ts": str(run_ts)}
    p.update(extra)
    return p


# ══════════════════════════════════════════════════════════════════════════════
# 1. RAW LAYER - exact copy, all columns as text, append-only
# ══════════════════════════════════════════════════════════════════════════════

def load_raw(entity, data_dir, run_id):
    """
    Append this file's rows to raw_<entity> with _row_num, _source_file, _run_id, _loaded_at.

    Raw is NEVER cleaned and NEVER deleted across runs (audit trail).
    The only delete is for the SAME run_id + file: if Airflow retries this task,
    we don't want the same run's rows twice.
    """
    if entity == "orders_increment":
        file_name, columns = config.INCREMENT_FILE, config.ORDER_COLUMNS
    else:
        file_name, columns, _ = config.SOURCE_FILES[entity]
    path = Path(data_dir) / file_name

    # Header check first: a renamed column must fail loudly (schema pillar)
    rows_in_file = len(read_csv_rows(path, expected_columns=columns))

    table = RAW_TABLE[entity]
    column_ddl = ", ".join(f"{c} VARCHAR" for c in columns)
    con = connect()
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {column_ddl},
                _row_num BIGINT, _source_file VARCHAR, _run_id VARCHAR, _loaded_at TIMESTAMP  -- UTC
            )
        """)
        con.execute("BEGIN")
        con.execute(f"DELETE FROM {table} WHERE _run_id = ? AND _source_file = ?", [run_id, file_name])
        # all_varchar=true -> every value stays text, exactly as in the file.
        # row_number() OVER () numbers rows in file order (1 = first data row).
        con.execute(f"""
            INSERT INTO {table} BY NAME
            SELECT row_number() OVER () AS _row_num, *,
                   ? AS _source_file, ? AS _run_id, ? AS _loaded_at
            FROM read_csv(?, header = true, all_varchar = true)
        """, [file_name, run_id, utc_now(), str(path)])
        con.execute("COMMIT")
        loaded = con.execute(f"SELECT COUNT(*) FROM {table} WHERE _run_id = ? AND _source_file = ?",
                             [run_id, file_name]).fetchone()[0]
        total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        con.close()

    print(f"[Raw] {file_name}: {loaded} rows appended to {table} (file had {rows_in_file}); "
          f"{table} now holds {total} rows across all runs")
    return {"entity": entity, "source_file": file_name, "rows": loaded}


# ══════════════════════════════════════════════════════════════════════════════
# 2. STAGING - SQL files, then record the quality results of that table
# ══════════════════════════════════════════════════════════════════════════════

def _record_stg_dq(con, table, entity, file_label, run_id, dag_id):
    """
    Count each chk_<name> column of a staging table -> dq_results rows.
    Because the column names come from checks_catalog.py, ETL and ELT
    produce the same check_name values.
    """
    checks = []
    for c in checks_for(entity):
        rows_checked, rows_failed = con.execute(
            f"SELECT COUNT(*), COALESCE(SUM(CASE WHEN chk_{c['name']} THEN 1 ELSE 0 END), 0) FROM {table}"
        ).fetchone()
        checks.append({"check_name": c["name"], "dimension": c["dimension"], "severity": c["severity"],
                       "rows_checked": rows_checked, "rows_failed": rows_failed})
    write_dq_results(con, run_id, dag_id, file_label, checks)
    for c in checks:
        if c["rows_failed"]:
            print(f"    {c['severity']:8} {c['check_name']:32} failed {c['rows_failed']}")


def build_staging(entity, run_id, run_ts, dag_id=DAG_ID):
    """entity: customers | products | stores | orders | payments"""
    table = f"stg_{entity}"
    con = connect()
    try:
        if entity == "orders":
            run_sql_file(con, "elt/stg_orders.sql",
                         _params(run_id, run_ts, target_table="stg_orders", source_file="orders.csv"))
        else:
            run_sql_file(con, f"elt/{table}.sql", _params(run_id, run_ts))
        _record_stg_dq(con, table, entity, entity, run_id, dag_id)
        counts = dict(con.execute(f"SELECT dq_status, COUNT(*) FROM {table} GROUP BY dq_status").fetchall())
    finally:
        con.close()
    print(f"[Staging] {table}: {counts}")
    return counts


def merge_increment(run_id, run_ts, dag_id=DAG_ID):
    """Stage the increment file with the same rules, then MERGE it into stg_orders."""
    con = connect()
    try:
        run_sql_file(con, "elt/stg_orders.sql",
                     _params(run_id, run_ts, target_table="stg_orders_increment",
                             source_file=config.INCREMENT_FILE))
        _record_stg_dq(con, "stg_orders_increment", "orders", "orders_increment", run_id, dag_id)

        good = "(SELECT order_id FROM stg_orders_increment WHERE dq_status <> 'FAILED')"
        to_update = con.execute(f"SELECT COUNT(*) FROM stg_orders WHERE NOT is_duplicate AND order_id IN {good}").fetchone()[0]
        to_insert = con.execute(f"SELECT COUNT(*) FROM {good} g WHERE order_id NOT IN (SELECT order_id FROM stg_orders)").fetchone()[0]
        rejected = con.execute("SELECT COUNT(*) FROM stg_orders_increment WHERE dq_status = 'FAILED'").fetchone()[0]
        before = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]

        run_sql_file(con, "elt/stg_orders_merge.sql")

        after = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    finally:
        con.close()
    print(f"[Merge] updated={to_update} inserted={to_insert} rejected={rejected} | "
          f"stg_orders rows {before} -> {after}")
    return {"updated": to_update, "inserted": to_insert, "rejected": rejected}


def record_dataset_checks(run_id, run_ts, dag_id=DAG_ID):
    """Freshness + volume checks (Warnings) - same function the ETL uses."""
    con = connect()
    try:
        latest = con.execute("SELECT MAX(order_ts) FROM stg_orders WHERE dq_status <> 'FAILED'").fetchone()[0]
        rows_now = con.execute("SELECT COUNT(*) FROM raw_orders WHERE _run_id = ? AND _source_file = 'orders.csv'",
                               [run_id]).fetchone()[0]
        previous = previous_orders_row_count(con, dag_id, run_id)
        write_dq_results(con, run_id, dag_id, "orders_dataset",
                         rules.dataset_checks(latest, rows_now, previous, run_ts))
    finally:
        con.close()


# ══════════════════════════════════════════════════════════════════════════════
# 3. MARTS - one SQL file per business table
# ══════════════════════════════════════════════════════════════════════════════

MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_reconciliation", "mart_top_customers"]


def build_mart(name, run_id, run_ts):
    con = connect()
    try:
        run_sql_file(con, f"elt/{name}.sql", _params(run_id, run_ts))
        rows = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    finally:
        con.close()
    print(f"[Mart] {name}: {rows} rows")
    return rows


def report():
    con = connect()
    try:
        print_query(con, """
            SELECT zone, ROUND(SUM(net_revenue), 2) AS net_revenue, SUM(delivered_orders) AS orders
            FROM mart_daily_store_kpis GROUP BY zone ORDER BY net_revenue DESC
        """, title="Net revenue by zone (ELT)")
        print_query(con, "SELECT * FROM mart_category_margin", title="Category margin (ELT)")
        print_query(con, """
            SELECT reason, COUNT(*) AS mismatches, ROUND(SUM(ABS(difference)), 2) AS amount_not_reconciled
            FROM mart_payment_reconciliation GROUP BY reason ORDER BY mismatches DESC
        """, title="Payment reconciliation (ELT)")
        total = con.execute("SELECT ROUND(SUM(net_revenue), 2) FROM mart_daily_store_kpis").fetchone()[0]
        print(f"[Report] total net revenue = {total}")
        return {"total_net_revenue": total}
    finally:
        con.close()


# ══════════════════════════════════════════════════════════════════════════════
# 4. RUN AUDIT helpers
# ══════════════════════════════════════════════════════════════════════════════

def _count_or_zero(con, sql, params):
    """COUNT that returns 0 if the table doesn't exist yet (e.g. first run failed early)."""
    try:
        return con.execute(sql, params).fetchone()[0]
    except Exception:
        return 0


def audit_counts(run_id, increment_loaded):
    """rows_extracted / rows_loaded / rows_quarantined for pipeline_runs."""
    con = connect()
    try:
        extracted = sum(_count_or_zero(con, f"SELECT COUNT(*) FROM {t} WHERE _run_id = ?", [run_id])
                        for t in ["raw_orders", "raw_payments", "raw_customers", "raw_products", "raw_stores"])
        stg = ["stg_customers", "stg_products", "stg_stores", "stg_orders"]
        loaded = sum(_count_or_zero(con, f"SELECT COUNT(*) FROM {t} WHERE dq_status <> 'FAILED' AND _run_id = ?",
                                    [run_id]) for t in stg)
        failed_tables = stg + ["stg_payments"] + (["stg_orders_increment"] if increment_loaded else [])
        quarantined = sum(_count_or_zero(con, f"SELECT COUNT(*) FROM {t} WHERE dq_status = 'FAILED' AND _run_id = ?",
                                         [run_id]) for t in failed_tables)
        return extracted, loaded, quarantined
    finally:
        con.close()
