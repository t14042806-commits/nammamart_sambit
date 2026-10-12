"""
include/code/etl_steps.py
------------------------------
The ETL pipeline, one function per Airflow task:

    extract  ->  validate  ->  quarantine + dq_results  ->  transform  ->  load
                                                                 -> reconcile -> report

Design choice: tasks pass FILE PATHS to each other, not the data itself.
Each step writes its output as a CSV under include/output/work/<run_id>/ and
returns a small dict (path + counts). Airflow stores that small dict as XCom.
Big data in XCom makes the Airflow database slow; paths keep it tiny.
Bonus: you can open any intermediate CSV in VS Code (Rainbow CSV) to debug.

Every function here is plain Python - no Airflow imports - so run_local.py can
call them directly on your laptop.
"""

from datetime import datetime, timedelta
from pathlib import Path

from include.code import config, rules
from include.code.csv_utils import read_csv_rows, safe_name, write_csv_rows
from include.code.masking import mask_email, mask_name, mask_phone
from include.code.warehouse import (
    connect, print_query, run_sql_file, utc_now, write_dq_results, previous_orders_row_count,
)

DAG_ID = f"nammamart_etl_{config.LEARNER}"


def work_dir_for(run_id):
    """include/output/work/<run_id>/ - every run gets its own folder, so runs never mix."""
    return config.WORK_DIR / safe_name(run_id)


# ══════════════════════════════════════════════════════════════════════════════
# 1. EXTRACT - copy one source file into the run's work folder (after a schema check)
# ══════════════════════════════════════════════════════════════════════════════

def extract(entity, data_dir, run_id):
    """
    entity: 'orders' | 'payments' | 'customers' | 'products' | 'stores' | 'orders_increment'
    Returns {"entity", "source_file", "path", "rows"}  (small - fine for XCom)
    """
    if entity == "orders_increment":
        file_name, columns = config.INCREMENT_FILE, config.ORDER_COLUMNS
    else:
        file_name, columns, _ = config.SOURCE_FILES[entity]

    source = Path(data_dir) / file_name
    rows = read_csv_rows(source, expected_columns=columns)      # raises on missing file / column
    out = write_csv_rows(work_dir_for(run_id) / "extracted" / f"{entity}.csv", rows, columns + ["_row_num"])
    print(f"[Extract] {file_name}: {len(rows)} rows -> {out}")
    return {"entity": entity, "source_file": file_name, "path": out, "rows": len(rows)}


# ══════════════════════════════════════════════════════════════════════════════
# 2. VALIDATE - split each file into passed / failed rows
# ══════════════════════════════════════════════════════════════════════════════

def _save_validation(entity, extracted, result, run_id):
    """Write passed/failed rows to CSV and return a small summary dict."""
    folder = work_dir_for(run_id)
    columns = (config.ORDER_COLUMNS if entity.startswith("orders")
               else config.SOURCE_FILES[entity][1])
    passed_path = write_csv_rows(folder / "passed" / f"{entity}.csv", result["passed"],
                                 columns + ["_row_num", "dq_warnings"])
    failed_path = write_csv_rows(folder / "failed" / f"{entity}.csv", result["failed"],
                                 ["failure_reason"] + columns + ["_row_num", "dq_warnings"])
    summary = {
        "entity": entity,
        "source_file": extracted["source_file"],
        "rows_in": extracted["rows"],
        "rows_passed": len(result["passed"]),
        "rows_failed": len(result["failed"]),
        "passed_path": passed_path,
        "failed_path": failed_path,
        "checks": result["checks"],
    }
    print(f"[Validate] {entity}: in={summary['rows_in']} passed={summary['rows_passed']} "
          f"quarantined={summary['rows_failed']}")
    for c in result["checks"]:
        if c["rows_failed"]:
            print(f"    {c['severity']:8} {c['check_name']:32} failed {c['rows_failed']}")
    return summary


def validate_dimension(entity, extracted, run_id, run_ts):
    """customers / products / stores"""
    rows = read_csv_rows(extracted["path"])
    validator = {
        "customers": rules.validate_customers,
        "products": rules.validate_products,
        "stores": rules.validate_stores,
    }[entity]
    return _save_validation(entity, extracted, validator(rows, run_ts), run_id)


def validate_orders(extracted, customers_res, products_res, stores_res, run_id, run_ts):
    """orders.csv or the increment file (extracted['entity'] tells which)."""
    clean_customer_ids = {r["customer_id"] for r in read_csv_rows(customers_res["passed_path"])}
    clean_products = {r["product_id"]: r for r in read_csv_rows(products_res["passed_path"])}
    clean_store_ids = {r["store_id"] for r in read_csv_rows(stores_res["passed_path"])}

    rows = read_csv_rows(extracted["path"])
    result = rules.validate_orders(rows, clean_customer_ids, clean_products, clean_store_ids, run_ts)
    summary = _save_validation(extracted["entity"], extracted, result, run_id)

    # Remember the newest VALID order time for the freshness check
    times = [rules.to_timestamp(r["order_ts"]) for r in result["passed"]]
    summary["latest_valid_order_ts"] = str(max(times)) if times else None
    return summary


def validate_payments(extracted, orders_extracted, increment_extracted, run_id, run_ts):
    """A payment must point to an order that exists in this run's order export(s)."""
    known_ids = {rules.text(r, "order_id") for r in read_csv_rows(orders_extracted["path"])}
    if increment_extracted:
        known_ids |= {rules.text(r, "order_id") for r in read_csv_rows(increment_extracted["path"])}

    rows = read_csv_rows(extracted["path"])
    return _save_validation("payments", extracted, rules.validate_payments(rows, known_ids, run_ts), run_id)


# ══════════════════════════════════════════════════════════════════════════════
# 3. QUARANTINE + DQ RESULTS
# ══════════════════════════════════════════════════════════════════════════════

def quarantine(results, run_id):
    """
    Write failed rows to include/output/quarantine/<file>_failed_<run_id>.csv
    (exactly as received + failure_reason) and log their keys in etl_rejected_rows.
    Rows are set aside with a reason - never silently dropped.
    """
    con = connect()
    try:
        run_sql_file(con, "etl/create_tables.sql")
        con.execute("BEGIN")
        con.execute("DELETE FROM etl_rejected_rows WHERE run_id = ?", [run_id])   # safe to re-run
        total = 0
        for res in results:
            if not res:                       # increment not loaded this run
                continue
            failed = read_csv_rows(res["failed_path"])
            if not failed:
                continue
            entity = res["entity"]
            key_col = "order_id" if entity.startswith("orders") else config.SOURCE_FILES[entity][2]
            out = config.QUARANTINE_DIR / f"{entity}_failed_{safe_name(run_id)}.csv"
            columns = [c for c in failed[0].keys() if c != "_row_num"] + ["_row_num"]
            write_csv_rows(out, failed, columns)
            for r in failed:
                con.execute(
                    "INSERT INTO etl_rejected_rows VALUES (?, ?, ?, ?, ?, ?)",
                    [run_id, res["source_file"], r.get(key_col), int(r["_row_num"]),
                     r["failure_reason"], out.name],
                )
            total += len(failed)
            print(f"[Quarantine] {len(failed):4} rows -> {out}")
        con.execute("COMMIT")
    finally:
        con.close()
    return total


def record_dq_results(results, orders_res, increment_res, run_id, run_ts, dag_id=DAG_ID):
    """Write every check result to dq_results, plus the dataset-level checks."""
    con = connect()
    try:
        for res in results:
            if res:
                write_dq_results(con, run_id, dag_id, res["entity"], res["checks"])

        # freshness = newest VALID order across this run's order files
        candidates = [r.get("latest_valid_order_ts") for r in (orders_res, increment_res) if r]
        candidates = [datetime.fromisoformat(t) for t in candidates if t]
        latest = max(candidates) if candidates else None
        previous = previous_orders_row_count(con, dag_id, run_id)
        dataset = rules.dataset_checks(latest, orders_res["rows_in"], previous, run_ts)
        write_dq_results(con, run_id, dag_id, "orders_dataset", dataset)
    finally:
        con.close()


# ══════════════════════════════════════════════════════════════════════════════
# 4. TRANSFORM - mask PII, build dimensions, join and derive fact columns
# ══════════════════════════════════════════════════════════════════════════════

def _build_fact_rows(order_rows, products_by_id, run_id, loaded_at):
    """Turn validated order rows into fact rows with derived measures."""
    fact = []
    for o in order_rows:
        qty = rules.to_number(o["quantity"])
        amount = rules.to_number(o["order_amount"])
        minutes = rules.to_number(o["delivery_minutes"])
        cost_price = rules.to_number(products_by_id[o["product_id"]]["cost_price"])
        delivered = o["order_status"] == "DELIVERED"

        cost_amount = qty * cost_price
        net_revenue = amount if delivered else 0.0
        fact.append({
            "order_id": o["order_id"],
            "order_ts": o["order_ts"],
            "order_date": o["order_ts"][:10],                    # 'YYYY-MM-DD'
            "customer_id": o["customer_id"],
            "store_id": o["store_id"],
            "product_id": o["product_id"],
            "quantity": int(qty),
            "unit_price": o["unit_price"],
            "discount_pct": o["discount_pct"],
            "order_amount": o["order_amount"],
            "payment_mode": o["payment_mode"],
            "order_status": o["order_status"],
            "channel": o["channel"],
            "delivery_minutes": "" if minutes is None else int(minutes),
            "net_revenue": net_revenue,
            "cost_amount": cost_amount,
            "gross_margin": (net_revenue - cost_amount) if delivered else 0.0,
            "within_15_min": "" if not delivered else str(minutes <= config.SLA_MINUTES).lower(),
            "dq_warnings": o["dq_warnings"],
            "_source_file": o["_source_file"],
            "_source_row": o["_row_num"],
            "_run_id": run_id,
            "_loaded_at": loaded_at,
        })
    return fact


FACT_COLUMNS = [
    "order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id", "quantity",
    "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel",
    "delivery_minutes", "net_revenue", "cost_amount", "gross_margin", "within_15_min",
    "dq_warnings", "_source_file", "_source_row", "_run_id", "_loaded_at",
]


def transform(customers_res, products_res, stores_res, orders_res, increment_res, run_id):
    folder = work_dir_for(run_id) / "transformed"
    loaded_at = str(utc_now())
    stamp = {"_run_id": run_id, "_loaded_at": loaded_at}

    # ── dim_customer: mask PII here. Raw name/email/phone never leave this function.
    dim_customer = []
    for c in read_csv_rows(customers_res["passed_path"]):
        dim_customer.append({
            "customer_id": c["customer_id"],
            "customer_name": mask_name(c["full_name"]),
            "email_masked": mask_email(c["email"]),
            "phone_masked": mask_phone(c["phone"]),
            "city": c["city"], "area": c["area"], "pincode": c["pincode"],
            "signup_date": c["signup_date"], "loyalty_tier": c["loyalty_tier"],
            "is_active": c["is_active"], "dq_warnings": c["dq_warnings"], **stamp,
        })

    # ── dim_product (duplicates were already rejected by product_id_unique)
    products = read_csv_rows(products_res["passed_path"])
    products_by_id = {p["product_id"]: p for p in products}
    dim_product = [{
        **{k: p[k] for k in config.SOURCE_FILES["products"][1]},
        "is_perishable": p["is_perishable"].lower(),            # 'True' -> 'true'
        "dq_warnings": p["dq_warnings"], **stamp,
    } for p in products]

    # ── dim_store: a missing/invalid zone becomes 'Unknown' (zone_valid Warning)
    dim_store = []
    for s in read_csv_rows(stores_res["passed_path"]):
        dim_store.append({
            "store_id": s["store_id"], "store_name": s["store_name"],
            "zone": s["zone"] if s["zone"] in config.VALID_ZONES else "Unknown",
            "area": s["area"], "pincode": s["pincode"], "open_date": s["open_date"],
            "capacity_orders_per_day": s["capacity_orders_per_day"],
            "dq_warnings": s["dq_warnings"], **stamp,
        })

    # ── fact_orders: "join" = look up the product's cost_price by product_id
    orders = read_csv_rows(orders_res["passed_path"])
    for o in orders:
        o["_source_file"] = orders_res["source_file"]
    fact = _build_fact_rows(orders, products_by_id, run_id, loaded_at)

    paths = {
        "dim_customer": write_csv_rows(folder / "dim_customer.csv", dim_customer),
        "dim_product": write_csv_rows(folder / "dim_product.csv", dim_product),
        "dim_store": write_csv_rows(folder / "dim_store.csv", dim_store),
        "fact_orders": write_csv_rows(folder / "fact_orders.csv", fact, FACT_COLUMNS),
        "fact_increment": None,
    }

    if increment_res:
        inc = read_csv_rows(increment_res["passed_path"])
        for o in inc:
            o["_source_file"] = increment_res["source_file"]
        paths["fact_increment"] = write_csv_rows(
            folder / "fact_increment.csv", _build_fact_rows(inc, products_by_id, run_id, loaded_at), FACT_COLUMNS)

    print(f"[Transform] customers={len(dim_customer)} products={len(dim_product)} "
          f"stores={len(dim_store)} orders={len(fact)}")
    return paths


# ══════════════════════════════════════════════════════════════════════════════
# 5. LOAD - idempotent load of the star schema + incremental upsert
# ══════════════════════════════════════════════════════════════════════════════

def load(paths, run_id):
    """
    Idempotent = running it twice gives the same tables.
    How: inside ONE transaction, delete everything and insert this run's rows.
    If anything fails half-way, ROLLBACK leaves yesterday's tables untouched.

    Increment (only when the file was loaded this run): an UPSERT on order_id -
    existing orders are replaced (status corrections), new ones are inserted.
    """
    con = connect()
    try:
        run_sql_file(con, "etl/create_tables.sql")
        con.execute("BEGIN")
        for table, key in [("etl_dim_customer", "dim_customer"), ("etl_dim_product", "dim_product"),
                           ("etl_dim_store", "dim_store"), ("etl_fact_orders", "fact_orders")]:
            con.execute(f"DELETE FROM {table}")
            # BY NAME = match CSV columns to table columns by name; DuckDB casts text to the column types
            con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM read_csv(?, header=true, all_varchar=true)",
                        [paths[key]])

        inserted = updated = 0
        if paths.get("fact_increment"):
            con.execute("CREATE OR REPLACE TEMP TABLE inc AS "
                        "SELECT * FROM read_csv(?, header=true, all_varchar=true)", [paths["fact_increment"]])
            updated = con.execute("SELECT COUNT(*) FROM inc WHERE order_id IN (SELECT order_id FROM etl_fact_orders)").fetchone()[0]
            inserted = con.execute("SELECT COUNT(*) FROM inc").fetchone()[0] - updated
            con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM inc)")
            con.execute("INSERT INTO etl_fact_orders BY NAME SELECT * FROM inc")
            print(f"[Load] increment upsert: {updated} orders updated, {inserted} orders inserted")
        con.execute("COMMIT")

        counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ["etl_dim_customer", "etl_dim_product", "etl_dim_store", "etl_fact_orders"]}
        counts.update({"increment_inserted": inserted, "increment_updated": updated})
        print(f"[Load] {counts}")
        return counts
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


# ══════════════════════════════════════════════════════════════════════════════
# 6. RECONCILE - clean orders vs the payment gateway (plain Python)
# ══════════════════════════════════════════════════════════════════════════════

def reconcile(payments_extracted, orders_extracted, increment_extracted, run_id):
    """
    One output row per mismatch. Payments are standardised and de-duplicated
    (a repeated payment_id is an export glitch, not extra money).

    reason values:
      MISSING_SUCCESS_PAYMENT      delivered order, no successful payment
      MULTIPLE_SUCCESS_PAYMENTS    delivered order paid more than once
      AMOUNT_MISMATCH              paid amount != order_amount
      RETURNED_BUT_NOT_REFUNDED    returned order still has a SUCCESS payment
      CANCELLED_BUT_CHARGED        cancelled order has a SUCCESS payment
      PAYMENT_FOR_REJECTED_ORDER   payment for an order that exists but was quarantined
      PAYMENT_WITHOUT_ORDER        payment for an order_id the order system never exported
    """
    con = connect()
    try:
        orders = {r[0]: {"status": r[1], "amount": r[2]} for r in
                  con.execute("SELECT order_id, order_status, order_amount FROM etl_fact_orders").fetchall()}

        # standardise + dedupe payments (keep the first copy of each payment_id)
        payments, seen = [], set()
        for p in read_csv_rows(payments_extracted["path"]):
            pid = rules.text(p, "payment_id")
            if pid in seen:
                continue
            seen.add(pid)
            payments.append({"payment_id": pid, "order_id": rules.text(p, "order_id"),
                             "status": rules.text(p, "payment_status").upper(),
                             "amount": rules.to_number(rules.text(p, "amount_paid"))})

        # group SUCCESS payments by order
        success = {}
        for p in payments:
            if p["status"] == "SUCCESS":
                success.setdefault(p["order_id"], []).append(p)

        out = []
        tol = config.AMOUNT_TOLERANCE
        for order_id, o in orders.items():
            paid_list = success.get(order_id, [])
            paid = sum(p["amount"] or 0 for p in paid_list)
            first_pid = min(p["payment_id"] for p in paid_list) if paid_list else None
            reason, expected = None, 0.0
            if o["status"] == "DELIVERED":
                expected = o["amount"]
                if not paid_list:
                    reason = "MISSING_SUCCESS_PAYMENT"
                elif len(paid_list) > 1:
                    reason = "MULTIPLE_SUCCESS_PAYMENTS"
                elif abs(paid - o["amount"]) > tol:
                    reason = "AMOUNT_MISMATCH"
            elif o["status"] == "RETURNED" and paid_list:
                reason = "RETURNED_BUT_NOT_REFUNDED"
            elif o["status"] == "CANCELLED" and paid_list:
                reason = "CANCELLED_BUT_CHARGED"
            if reason:
                out.append([order_id, first_pid, o["status"], o["amount"], expected, paid,
                            paid - expected, reason, run_id])

        # every order_id the order system exported this run (good or bad)
        exported = {rules.text(r, "order_id") for r in read_csv_rows(orders_extracted["path"])}
        if increment_extracted:
            exported |= {rules.text(r, "order_id") for r in read_csv_rows(increment_extracted["path"])}

        for p in payments:
            if p["order_id"] not in orders:
                amt = p["amount"] or 0.0
                reason = "PAYMENT_FOR_REJECTED_ORDER" if p["order_id"] in exported else "PAYMENT_WITHOUT_ORDER"
                out.append([p["order_id"], p["payment_id"], None, None, 0.0, amt, amt, reason, run_id])

        con.execute("BEGIN")
        con.execute("DELETE FROM etl_payment_reconciliation")
        if out:
            con.executemany("INSERT INTO etl_payment_reconciliation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", out)
        con.execute("COMMIT")

        print_query(con, """
            SELECT reason, COUNT(*) AS mismatches, ROUND(SUM(ABS(difference)), 2) AS amount_not_reconciled
            FROM etl_payment_reconciliation GROUP BY reason ORDER BY mismatches DESC
        """, title="Payment reconciliation (ETL)")
        return len(out)
    finally:
        con.close()


# ══════════════════════════════════════════════════════════════════════════════
# 7. REPORT
# ══════════════════════════════════════════════════════════════════════════════

def report():
    con = connect()
    try:
        run_sql_file(con, "etl/reports.sql")
        print_query(con, """
            SELECT zone, ROUND(SUM(net_revenue), 2) AS net_revenue, SUM(delivered_orders) AS orders
            FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC
        """, title="Net revenue by zone (ETL)")
        print_query(con, "SELECT * FROM etl_category_margin", title="Category margin (ETL)")
        total = con.execute("SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders").fetchone()[0]
        print(f"[Report] total net revenue = {total}")
        return {"total_net_revenue": total}
    finally:
        con.close()


# ══════════════════════════════════════════════════════════════════════════════
# 8. RUN AUDIT helpers
# ══════════════════════════════════════════════════════════════════════════════

def audit_counts(run_id):
    """rows_loaded and rows_quarantined for pipeline_runs, read back from the warehouse."""
    con = connect()
    try:
        run_sql_file(con, "etl/create_tables.sql")
        loaded = sum(con.execute(f"SELECT COUNT(*) FROM {t} WHERE _run_id = ?", [run_id]).fetchone()[0]
                     for t in ["etl_dim_customer", "etl_dim_product", "etl_dim_store", "etl_fact_orders"])
        quarantined = con.execute("SELECT COUNT(*) FROM etl_rejected_rows WHERE run_id = ?", [run_id]).fetchone()[0]
        return loaded, quarantined
    finally:
        con.close()


def cleanup_old_work_dirs(keep_days=7):
    """Retention: delete intermediate work folders older than keep_days."""
    import shutil
    cutoff = datetime.now() - timedelta(days=keep_days)
    if not config.WORK_DIR.exists():
        return
    for folder in config.WORK_DIR.iterdir():
        if folder.is_dir() and datetime.fromtimestamp(folder.stat().st_mtime) < cutoff:
            shutil.rmtree(folder, ignore_errors=True)
            print(f"[Cleanup] removed {folder}")
