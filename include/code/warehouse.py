"""
include/code/warehouse.py
------------------------------
Everything that talks to DuckDB (include/output/warehouse_sambit.duckdb).

Used by both pipelines:
  connect()             open the warehouse, waiting politely if it is busy
  run_sql_file()        run one .sql file from include/sql/, filling {{placeholders}}
  ensure_audit_tables() create dq_results and pipeline_runs if missing
  write_dq_results()    one row per check per run          (Part D scorecard)
  write_pipeline_run()  one row per DAG run                (Part F run audit)
  print_query()         print a query result as a neat table in the task log
"""

import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from include.code import config


# ── Connection ────────────────────────────────────────────────────────────────

def connect(path=None, attempts=30, wait_seconds=2):
    """
    Open the DuckDB file.

    DuckDB allows only ONE process to write to a file at a time. Our ETL and
    ELT DAGs both run at 9:00 AM and parallel tasks can overlap, so if the file
    is locked we wait and try again (up to ~1 minute) instead of failing.
    Tip: close DBeaver/SQLTools connections to this file while DAGs run.
    """
    path = Path(path or config.WAREHOUSE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)       # create output/ on the first run
    path = str(path)
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return duckdb.connect(path)
        except duckdb.IOException as e:          # "Could not set lock on file ..."
            last_error = e
            print(f"[Warehouse] busy (attempt {attempt}/{attempts}), waiting {wait_seconds}s...")
            time.sleep(wait_seconds)
    raise last_error


# ── SQL files with {{placeholders}} ───────────────────────────────────────────

def render_sql(sql_text, params):
    """
    Replace {{name}} with params["name"]. Very small version of Airflow/Jinja templating.
    Example:  "WHERE _run_id = '{{run_id}}'"  ->  "WHERE _run_id = 'manual__2026...'"
    """
    for key, value in params.items():
        sql_text = sql_text.replace("{{" + key + "}}", str(value))
    if "{{" in sql_text:
        start = sql_text.index("{{")
        raise ValueError(f"Unfilled placeholder in SQL near: {sql_text[start:start + 40]}")
    return sql_text


def sql_text_value(value):
    """Escape a value for use inside '...' in SQL (doubles any single quote)."""
    return str(value).replace("'", "''")


def run_sql_file(con, relative_path, params=None):
    """
    Run include/sql/<relative_path>. The file may contain several statements
    separated by ';'. Config constants (allowed lists, limits) are always available
    as placeholders, plus anything passed in `params`.
    """
    path = config.SQL_DIR / relative_path
    all_params = config.sql_params()
    all_params.update(params or {})
    sql = render_sql(path.read_text(encoding="utf-8"), all_params)
    print(f"[SQL] running {relative_path}")
    con.execute(sql)


# ── Audit tables shared by both DAGs ──────────────────────────────────────────

def ensure_audit_tables(con):
    run_sql_file(con, "common/audit_tables.sql")


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def write_dq_results(con, run_id, dag_id, file_label, checks):
    """
    checks: list of {check_name, dimension, severity, rows_checked, rows_failed}
    status: PASS (0 failed) / FAIL (Critical check failed) / WARN (Warning check failed)

    Re-running the same run_id (retry or "Clear") first deletes that run's rows,
    so the scorecard never double-counts.
    """
    ensure_audit_tables(con)
    con.execute(
        "DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND source_file = ?",
        [run_id, dag_id, file_label],
    )
    now = utc_now()
    for c in checks:
        if c["rows_failed"] == 0:
            status = "PASS"
        elif c["severity"] == "Critical":
            status = "FAIL"
        else:
            status = "WARN"
        con.execute(
            """
            INSERT INTO dq_results (run_id, dag_id, source_file, check_name, dimension, severity,
                                    rows_checked, rows_failed, status, checked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [run_id, dag_id, file_label, f"{file_label}.{c['check_name']}", c["dimension"],
             c["severity"], c["rows_checked"], c["rows_failed"], status, now],
        )
    print(f"[DQ] wrote {len(checks)} check results for {file_label}")


def previous_orders_row_count(con, dag_id, run_id):
    """How many rows orders.csv had on the previous run of this DAG (for the volume check)."""
    ensure_audit_tables(con)
    row = con.execute(
        """
        SELECT rows_checked FROM dq_results
        WHERE dag_id = ? AND run_id <> ? AND check_name = 'orders.order_id_unique'
        ORDER BY checked_at DESC LIMIT 1
        """,
        [dag_id, run_id],
    ).fetchone()
    return row[0] if row else None


def write_pipeline_run(con, run_id, dag_id, start_time, rows_extracted, rows_loaded,
                       rows_quarantined, status):
    """One row per DAG run. Re-running the audit task replaces the row (idempotent)."""
    ensure_audit_tables(con)
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    con.execute(
        """
        INSERT INTO pipeline_runs (run_id, dag_id, start_time, end_time, rows_extracted,
                                   rows_loaded, rows_quarantined, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [run_id, dag_id, start_time, utc_now(), rows_extracted, rows_loaded, rows_quarantined, status],
    )
    print(f"[Audit] {dag_id} {run_id}: extracted={rows_extracted} loaded={rows_loaded} "
          f"quarantined={rows_quarantined} status={status}")


# ── Pretty printing for task logs (no pandas needed) ──────────────────────────

def print_query(con, sql, title=None, limit=50):
    """Run a SELECT and print the result as a text table in the Airflow log."""
    cursor = con.execute(sql)
    columns = [d[0] for d in cursor.description]
    rows = cursor.fetchmany(limit)
    if title:
        print(f"\n=== {title} ===")
    cells = [[("" if v is None else str(v)) for v in r] for r in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    print(" | ".join(c.ljust(w) for c, w in zip(columns, widths)))
    print("-+-".join("-" * w for w in widths))
    for r in cells:
        print(" | ".join(v.ljust(w) for v, w in zip(r, widths)))
    print(f"({len(rows)} rows shown)")
    return rows
