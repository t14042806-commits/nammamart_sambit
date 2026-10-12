"""
include/code/make_experiment_data.py
-----------------------------------------
Part F: create BROKEN COPIES of the dataset for the observability experiments.
The originals in include/data/ are never touched, so there is nothing
to "restore" afterwards - you just stop pointing the DAG at the copy.

Run from inside nammamart_sambit/ (the folder with the Dockerfile):
    ..\\.venv\\Scripts\\python -m include.code.make_experiment_data          (makes all)
    ..\\.venv\\Scripts\\python -m include.code.make_experiment_data exp05_bad_rows

Each experiment becomes a folder include/data_experiments/<name>/.
Then trigger the DAG with config (Airflow UI -> Trigger DAG w/ config):
    {"data_dir": "/usr/local/airflow/include/data_experiments/<name>"}
(That is the path INSIDE the Astro container. Locally with run_local.py use
 --data-dir include/data_experiments/<name>)

Experiments 1, 2, 3 and 10 need no special data (normal run, re-run,
load_increment=true, and Clear-after-failure).
"""

import shutil
import sys
from datetime import datetime, timedelta

from include.code import config
from include.code.csv_utils import read_csv_rows, write_csv_rows

OUT_ROOT = config.EXPERIMENTS_DIR
ORDERS = config.SOURCE_FILES["orders"][0]


def fresh_copy(name):
    """Copy all original files into a new experiment folder and return it."""
    folder = OUT_ROOT / name
    if folder.exists():
        shutil.rmtree(folder)
    shutil.copytree(config.DEFAULT_DATA_DIR, folder)
    return folder


def orders_of(folder):
    return read_csv_rows(folder / ORDERS)


def save_orders(folder, rows, columns=None):
    write_csv_rows(folder / ORDERS, rows, columns or config.ORDER_COLUMNS)


# ── Experiment 4: missing source file ─────────────────────────────────────────
def exp04_missing_file():
    folder = fresh_copy("exp04_missing_file")
    (folder / "products.csv").rename(folder / "products_RENAMED.csv")
    return folder, "products.csv renamed -> extract_products / load_raw_products should retry, then fail"


# ── Experiment 5: five new bad rows ───────────────────────────────────────────
def exp05_bad_rows():
    folder = fresh_copy("exp05_bad_rows")
    rows = orders_of(folder)
    good = rows[0]
    base = {c: good[c] for c in config.ORDER_COLUMNS}

    def bad(order_id, **changes):
        r = dict(base, order_id=order_id)
        r.update(changes)
        return r

    rows += [
        bad("ORD900001", customer_id=""),                                   # null customer
        bad("ORD900002", quantity="-2", order_amount=str(-2 * float(base["unit_price"]))),  # negative qty
        bad("ORD900003", store_id="S55"),                                   # unknown store
        bad("ORD900004", order_status="SHIPPED"),                           # invalid status
        bad("ORD900005", order_ts="2027-03-15 10:00:00"),                   # future timestamp
    ]
    save_orders(folder, rows)
    return folder, "5 bad orders ORD900001-ORD900005 added; each should be quarantined with its own reason"


# ── Experiment 6: schema change ───────────────────────────────────────────────
def exp06_schema_change():
    folder = fresh_copy("exp06_schema_change")
    rows = orders_of(folder)
    for r in rows:
        r["total"] = r.pop("order_amount")
    cols = [("total" if c == "order_amount" else c) for c in config.ORDER_COLUMNS]
    save_orders(folder, rows, cols)
    return folder, "order_amount renamed to total -> extract_orders / load_raw_orders should fail loudly (SchemaError)"


# ── Experiment 7: volume anomaly ──────────────────────────────────────────────
def exp07_low_volume():
    folder = fresh_copy("exp07_low_volume")
    save_orders(folder, orders_of(folder)[:50])
    return folder, "only 50 orders -> volume_vs_previous_run should WARN (run the normal data once first)"


# ── Experiment 8: distribution drift ──────────────────────────────────────────
def exp08_price_drift(store_id="S05"):
    folder = fresh_copy("exp08_price_drift")
    rows = orders_of(folder)
    for r in rows:
        if r["store_id"] == store_id:
            r["unit_price"] = str(round(float(r["unit_price"]) * 100, 2))
            # also scale order_amount so the formula check still passes -
            # the point is to see whether anything ELSE catches the drift
            try:
                r["order_amount"] = str(round(float(r["order_amount"]) * 100, 2))
            except ValueError:
                pass
    save_orders(folder, rows)
    return folder, f"unit_price x100 for {store_id} -> unit_price_not_above_mrp should catch it"


# ── Experiment 9: freshness ───────────────────────────────────────────────────
def exp09_stale_data(days_old=30):
    """
    Shift every timestamp so the newest order (including the deliberately
    future-dated ones) is `days_old` days before NOW. Side effect to note in
    your results: those future-dated rows are now in the past, so they pass.
    """
    folder = fresh_copy("exp09_stale_data")
    rows = orders_of(folder)
    times = [datetime.strptime(r["order_ts"], config.TS_FORMAT) for r in rows if _parses(r["order_ts"])]
    shift = (datetime.now() - timedelta(days=days_old)) - max(times)
    for r in rows:
        if _parses(r["order_ts"]):
            r["order_ts"] = (datetime.strptime(r["order_ts"], config.TS_FORMAT) + shift).strftime(config.TS_FORMAT)
    save_orders(folder, rows)
    return folder, f"newest September order moved to {days_old} days ago -> latest_order_is_fresh should WARN"


def _parses(value):
    try:
        datetime.strptime(value, config.TS_FORMAT)
        return True
    except ValueError:
        return False


EXPERIMENTS = {
    "exp04_missing_file": exp04_missing_file,
    "exp05_bad_rows": exp05_bad_rows,
    "exp06_schema_change": exp06_schema_change,
    "exp07_low_volume": exp07_low_volume,
    "exp08_price_drift": exp08_price_drift,
    "exp09_stale_data": exp09_stale_data,
}


def main():
    names = sys.argv[1:] or list(EXPERIMENTS)
    for name in names:
        folder, what = EXPERIMENTS[name]()
        print(f"[{name}] {folder}\n    {what}")
        print(f'    Airflow config: {{"data_dir": "/usr/local/airflow/include/data_experiments/{name}"}}')


if __name__ == "__main__":
    main()
