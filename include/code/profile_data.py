"""
include/code/profile_data.py
---------------------------------
Part 0: profile all six files BEFORE writing pipeline code. Plain Python (csv module).

Run from inside nammamart_sambit/ (the folder with the Dockerfile):
    ..\\.venv\\Scripts\\python -m include.code.profile_data
    ..\\.venv\\Scripts\\python -m include.code.profile_data --data-dir <other folder>

It prints, per file:
  - row count, blanks per column, duplicate keys
  - min / max of numeric and date columns
  - distinct values of category columns
and then referential-integrity and business-rule checks across files.
Copy the interesting findings into the Data Issues Log in SUBMISSION.md.
"""

import argparse
from collections import Counter
from pathlib import Path

from include.code import config
from include.code.csv_utils import read_csv_rows
from include.code.rules import to_date, to_number, to_timestamp

NUMERIC = {
    "orders": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "payments": ["amount_paid"],
    "products": ["mrp", "cost_price"],
    "stores": ["capacity_orders_per_day"],
    "customers": [],
}
TIMESTAMPS = {"orders": ["order_ts"], "payments": ["payment_ts"]}
DATES = {"customers": ["signup_date"], "products": ["launch_date"], "stores": ["open_date"]}
CATEGORIES = {
    "orders": ["order_status", "payment_mode", "channel"],
    "payments": ["payment_mode", "payment_status"],
    "customers": ["loyalty_tier", "is_active", "city"],
    "products": ["category", "unit", "is_perishable"],
    "stores": ["zone"],
}


def header(title):
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)


def profile_file(entity, rows, columns, key):
    header(f"{entity.upper()}  ({len(rows)} rows)")

    # blanks per column
    blanks = {c: sum(1 for r in rows if (r.get(c) or "").strip() == "") for c in columns}
    print("Blank values per column:", {c: n for c, n in blanks.items() if n} or "none")

    # duplicate keys
    counts = Counter((r.get(key) or "").strip() for r in rows)
    dups = {k: n for k, n in counts.items() if n > 1}
    print(f"Duplicate {key}: {len(dups)} keys -> {sorted(dups)[:10]}")
    exact = len(rows) - len({tuple((r.get(c) or "") for c in columns) for r in rows})
    print(f"Exact duplicate rows (all columns identical): {exact}")

    # numeric min / max + values that are not numbers
    for c in NUMERIC.get(entity, []):
        values = [(r.get(c) or "").strip() for r in rows]
        nums = [to_number(v) for v in values if v != ""]
        bad = [v for v in values if v != "" and to_number(v) is None]
        good = [n for n in nums if n is not None]
        print(f"  {c:26} min={min(good) if good else None:<10} max={max(good) if good else None:<10} "
              f"not-a-number={bad[:5]}")

    # timestamp / date ranges + unparseable values
    for c, parser in [(c, to_timestamp) for c in TIMESTAMPS.get(entity, [])] + \
                     [(c, to_date) for c in DATES.get(entity, [])]:
        values = [(r.get(c) or "").strip() for r in rows]
        parsed = [parser(v) for v in values if v]
        good = [p for p in parsed if p is not None]
        bad = sorted({v for v in values if v and parser(v) is None})
        print(f"  {c:26} min={min(good) if good else None}  max={max(good) if good else None}  "
              f"unparseable={bad[:6]}")

    # distinct values of category columns
    for c in CATEGORIES.get(entity, []):
        dist = Counter((r.get(c) if r.get(c) is not None else "<blank>") for r in rows)
        print(f"  {c:26} {dict(dist.most_common())}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=str(config.DEFAULT_DATA_DIR))
    args = parser.parse_args()
    data = Path(args.data_dir)

    files = {}
    for entity, (file_name, columns, key) in config.SOURCE_FILES.items():
        files[entity] = read_csv_rows(data / file_name, expected_columns=columns)
        profile_file(entity, files[entity], columns, key)
    increment = read_csv_rows(data / config.INCREMENT_FILE, expected_columns=config.ORDER_COLUMNS)
    profile_file("orders", increment, config.ORDER_COLUMNS, "order_id")
    print("(the block above is the increment file)")

    orders, payments = files["orders"], files["payments"]
    ids = {e: {(r.get(config.SOURCE_FILES[e][2]) or "").strip() for r in files[e]} for e in files}

    header("REFERENTIAL INTEGRITY")
    for col, master in [("customer_id", "customers"), ("product_id", "products"), ("store_id", "stores")]:
        missing = [r["order_id"] for r in orders if (r.get(col) or "").strip() and r[col].strip() not in ids[master]]
        print(f"orders.{col} not in {master}: {len(missing)} -> {missing[:10]}")
    orphans = [r["payment_id"] for r in payments if r["order_id"].strip() not in ids["orders"]]
    print(f"payments.order_id not in orders: {len(orphans)} -> {orphans[:10]}")
    paid_orders = {r["order_id"].strip() for r in payments}
    cancelled_paid = [r["order_id"] for r in orders if r["order_status"].strip().upper() == "CANCELLED"
                      and r["order_id"].strip() in paid_orders]
    print(f"cancelled orders that have a payment: {len(cancelled_paid)}")

    header("BUSINESS RULES")
    wrong_amount = []
    for r in orders:
        q, p, d, a = (to_number((r.get(c) or "").strip()) for c in ["quantity", "unit_price", "discount_pct", "order_amount"])
        if None not in (q, p, d, a) and abs(round(q * p * (1 - d / 100), 2) - a) > config.AMOUNT_TOLERANCE:
            wrong_amount.append(r["order_id"])
    print(f"order_amount != quantity x unit_price x (1 - discount/100): {len(wrong_amount)} -> {wrong_amount[:10]}")

    no_minutes = [r["order_id"] for r in orders if r["order_status"].strip().upper() == "DELIVERED"
                  and not (r.get("delivery_minutes") or "").strip()]
    print(f"DELIVERED without delivery_minutes: {len(no_minutes)} -> {no_minutes[:10]}")

    slow = [(r["order_id"], r["delivery_minutes"]) for r in orders
            if (to_number((r.get("delivery_minutes") or "").strip()) or 0) > config.MAX_DELIVERY_MINUTES]
    print(f"delivery_minutes > {config.MAX_DELIVERY_MINUTES}: {len(slow)} -> {slow[:10]}")

    amount_by_order = {r["order_id"].strip(): to_number(r["order_amount"].strip()) for r in orders}
    mismatch = [r["payment_id"] for r in payments
                if r["payment_status"].strip() == "SUCCESS" and r["order_id"].strip() in amount_by_order
                and to_number(r["amount_paid"].strip()) is not None and amount_by_order[r["order_id"].strip()] is not None
                and abs(to_number(r["amount_paid"].strip()) - amount_by_order[r["order_id"].strip()]) > config.AMOUNT_TOLERANCE]
    print(f"SUCCESS payment != order_amount: {len(mismatch)} -> {mismatch[:10]}")

    cost_above = [r["product_id"] for r in files["products"]
                  if None not in (to_number(r["mrp"]), to_number(r["cost_price"]))
                  and to_number(r["cost_price"]) > to_number(r["mrp"])]
    print(f"products with cost_price > mrp: {cost_above}")

    in_both = sorted({r["order_id"].strip() for r in increment} & ids["orders"])
    print(f"increment: {len(increment)} rows, {len(in_both)} already in orders.csv (corrections) -> {in_both}")


if __name__ == "__main__":
    main()
