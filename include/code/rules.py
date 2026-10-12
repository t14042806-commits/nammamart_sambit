"""
include/code/rules.py
--------------------------
Data-quality rules for the ETL pipeline, written in plain Python (no pandas).

How every validate_<file>() function works
  1. Loop over the rows (each row is a dict of text values).
  2. Standardise the row (trim spaces, fix casing, map 'upi' -> 'UPI').
  3. Run every check from checks_catalog.py and collect the names that FAILED.
  4. If any failed check is Critical  -> the row goes to `failed` (quarantine)
     else                            -> the row goes to `passed`
     Warning-only problems are written into the row's `dq_warnings` column.
  5. Count failures per check -> these counts become rows in dq_results.

The SQL in include/sql/elt/stg_*.sql implements the SAME checks with the SAME
names, so ETL and ELT should reject exactly the same rows.
"""

import re
from datetime import datetime

from include.code import config
from include.code.checks_catalog import CRITICAL, checks_for, severity_of

EMAIL_RE = re.compile(r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}")
TEN_DIGITS_RE = re.compile(r"[0-9]{10}")
SIX_DIGITS_RE = re.compile(r"[0-9]{6}")


# ══════════════════════════════════════════════════════════════════════════════
# Small converters. They return None instead of crashing on bad input,
# exactly like TRY_CAST / TRY_STRPTIME do in DuckDB.
# ══════════════════════════════════════════════════════════════════════════════

def text(row, column):
    """Value of a column with spaces trimmed; missing/blank becomes ""."""
    return (row.get(column) or "").strip()


def to_number(value):
    """'12.5' -> 12.5 ; '' or 'abc' -> None"""
    try:
        return float(value) if value != "" else None
    except ValueError:
        return None


def to_timestamp(value):
    """'2026-09-01 06:30:15' -> datetime ; anything else ('N/A', '31/09/2026 10:00') -> None"""
    try:
        return datetime.strptime(value, config.TS_FORMAT)
    except (ValueError, TypeError):
        return None


def to_date(value):
    """'2026-09-01' -> date ; anything else -> None"""
    try:
        return datetime.strptime(value, config.DATE_FORMAT).date()
    except (ValueError, TypeError):
        return None


def title_case(value):
    """'BRONZE' / 'bronze' -> 'Bronze'  (same as SQL: upper(left(x,1)) || lower(substr(x,2)))"""
    return value[:1].upper() + value[1:].lower()


# ══════════════════════════════════════════════════════════════════════════════
# The "split" step shared by all files
# ══════════════════════════════════════════════════════════════════════════════

def split_rows(entity, checked_rows):
    """
    checked_rows: list of (raw_row, standardised_row, set_of_failed_check_names)

    Returns a result dict:
      passed  - STANDARDISED rows with no Critical failure (may have dq_warnings)
      failed  - ORIGINAL rows exactly as received + failure_reason, for quarantine
                (auditors must see what the source system really sent)
      checks  - [{check_name, dimension, severity, rows_checked, rows_failed}, ...]
    """
    catalog = checks_for(entity)
    counts = {c["name"]: 0 for c in catalog}
    passed, failed = [], []

    for raw, row, failed_names in checked_rows:
        # keep catalog order so failure_reason text is predictable
        ordered = [c["name"] for c in catalog if c["name"] in failed_names]
        for name in ordered:
            counts[name] += 1

        critical = [n for n in ordered if severity_of(entity, n) == CRITICAL]
        warnings = [n for n in ordered if severity_of(entity, n) != CRITICAL]
        row["dq_warnings"] = "; ".join(warnings)

        if critical:
            rejected = dict(raw)                    # untouched source values (+ _row_num)
            rejected["failure_reason"] = "; ".join(critical)
            rejected["dq_warnings"] = row["dq_warnings"]
            failed.append(rejected)
        else:
            passed.append(row)

    checks = [
        {
            "check_name": c["name"],
            "dimension": c["dimension"],
            "severity": c["severity"],
            "rows_checked": len(checked_rows),
            "rows_failed": counts[c["name"]],
        }
        for c in catalog
    ]
    return {"passed": passed, "failed": failed, "checks": checks}


# ══════════════════════════════════════════════════════════════════════════════
# Dimension files first: orders need the CLEAN customer/product/store ids.
# ══════════════════════════════════════════════════════════════════════════════

def validate_customers(rows, run_ts):
    seen_ids = set()
    checked = []
    for raw in rows:
        row = dict(raw)                                    # copy; never change the source row
        for col in config.SOURCE_FILES["customers"][1]:
            row[col] = text(raw, col)                      # trim every column
        tier_raw = raw.get("loyalty_tier") or ""
        row["loyalty_tier"] = title_case(row["loyalty_tier"])

        signup = to_date(row["signup_date"])
        fails = set()

        if row["customer_id"] == "":
            fails.add("customer_id_not_null")
        if row["customer_id"] in seen_ids:
            fails.add("customer_id_unique")
        if row["loyalty_tier"] not in config.VALID_LOYALTY_TIERS:
            fails.add("loyalty_tier_valid")
        if signup is None:
            fails.add("signup_date_valid")
        elif signup > run_ts.date():
            fails.add("signup_date_not_future")
        if not EMAIL_RE.fullmatch(row["email"]):
            fails.add("email_format_valid")
        if not TEN_DIGITS_RE.fullmatch(row["phone"]):
            fails.add("phone_10_digits")
        if not SIX_DIGITS_RE.fullmatch(row["pincode"]):
            fails.add("pincode_6_digits")
        if row["city"] == "":
            fails.add("city_not_null")
        if tier_raw != row["loyalty_tier"] and row["loyalty_tier"] in config.VALID_LOYALTY_TIERS:
            fails.add("loyalty_tier_standardised")

        seen_ids.add(row["customer_id"])
        checked.append((raw, row, fails))
    return split_rows("customers", checked)


def validate_products(rows, run_ts):
    seen_ids = set()
    checked = []
    for raw in rows:
        row = dict(raw)
        for col in config.SOURCE_FILES["products"][1]:
            row[col] = text(raw, col)

        mrp = to_number(row["mrp"])
        cost = to_number(row["cost_price"])
        launch = to_date(row["launch_date"])
        fails = set()

        if row["product_id"] in seen_ids:
            fails.add("product_id_unique")
        if row["category"] == "":
            fails.add("category_not_null")
        if mrp is None or cost is None:
            fails.add("prices_parse")
        if launch is None:
            fails.add("launch_date_valid")
        elif launch > run_ts.date():
            fails.add("launch_date_not_future")
        if mrp is not None and mrp <= 0:
            fails.add("mrp_positive")
        if mrp is not None and cost is not None and cost > mrp:
            fails.add("cost_price_not_above_mrp")

        seen_ids.add(row["product_id"])
        checked.append((raw, row, fails))
    return split_rows("products", checked)


def validate_stores(rows, run_ts):
    seen_ids = set()
    checked = []
    for raw in rows:
        row = dict(raw)
        for col in config.SOURCE_FILES["stores"][1]:
            row[col] = text(raw, col)

        opened = to_date(row["open_date"])
        capacity = to_number(row["capacity_orders_per_day"])
        fails = set()

        if row["store_id"] == "":
            fails.add("store_id_not_null")
        if row["store_id"] in seen_ids:
            fails.add("store_id_unique")
        if opened is None:
            fails.add("open_date_valid")
        elif opened > run_ts.date():
            fails.add("open_date_not_future")
        if row["zone"] not in config.VALID_ZONES:
            fails.add("zone_valid")
        if not SIX_DIGITS_RE.fullmatch(row["pincode"]):
            fails.add("pincode_6_digits")
        if capacity is None or capacity <= 0:
            fails.add("capacity_positive")

        seen_ids.add(row["store_id"])
        checked.append((raw, row, fails))
    return split_rows("stores", checked)


# ══════════════════════════════════════════════════════════════════════════════
# Orders (also used for the increment file)
# ══════════════════════════════════════════════════════════════════════════════

def standardise_payment_mode(value):
    """'upi', 'UPI ', 'Upi' -> 'UPI' ; 'cod' -> 'Cash on Delivery' ; unknown -> unchanged (trimmed)"""
    return config.PAYMENT_MODE_MAP.get(value.strip().lower(), value.strip())


def validate_orders(rows, clean_customer_ids, clean_products, clean_store_ids, run_ts):
    """
    clean_customer_ids : set of customer_ids that PASSED validation
    clean_products     : dict product_id -> passed product row (we need its mrp)
    clean_store_ids    : set of store_ids that PASSED validation
    run_ts             : datetime of this run in IST (anything later is "future")
    """
    seen_ids = set()
    checked = []
    for raw in rows:
        row = dict(raw)
        for col in config.ORDER_COLUMNS:
            row[col] = text(raw, col)
        row["order_status"] = row["order_status"].upper()
        row["payment_mode"] = standardise_payment_mode(row["payment_mode"])

        ts = to_timestamp(row["order_ts"])
        qty = to_number(row["quantity"])
        price = to_number(row["unit_price"])
        disc = to_number(row["discount_pct"])
        amount = to_number(row["order_amount"])
        minutes = to_number(row["delivery_minutes"])
        product = clean_products.get(row["product_id"])
        fails = set()

        # Completeness
        if row["order_id"] == "":
            fails.add("order_id_not_null")
        if row["customer_id"] == "":
            fails.add("customer_id_not_null")
        if row["order_ts"] == "":
            fails.add("order_ts_not_null")
        if row["order_status"] == "DELIVERED" and row["delivery_minutes"] == "":
            fails.add("delivered_has_delivery_minutes")

        # Uniqueness - the first copy passes, later copies fail
        if row["order_id"] in seen_ids:
            fails.add("order_id_unique")

        # Validity
        if row["order_status"] not in config.VALID_ORDER_STATUS:
            fails.add("order_status_valid")
        if row["payment_mode"] not in config.VALID_PAYMENT_MODES:
            fails.add("payment_mode_valid")
        if row["order_ts"] != "" and ts is None:
            fails.add("order_ts_format_valid")
        if (qty is None or price is None or disc is None or amount is None
                or (row["delivery_minutes"] != "" and minutes is None)):
            fails.add("numbers_parse")
        if disc is not None and not (0 <= disc <= 100):
            fails.add("discount_pct_in_range")

        # Accuracy
        if qty is not None and qty <= 0:
            fails.add("quantity_positive")
        if None not in (qty, price, disc, amount):
            expected = qty * price * (1 - disc / 100)
            if abs(expected - amount) > config.AMOUNT_TOLERANCE:
                fails.add("order_amount_matches_formula")
        if minutes is not None and (minutes <= 0 or minutes > config.MAX_DELIVERY_MINUTES):
            fails.add("delivery_minutes_believable")
        if product is not None and price is not None:
            mrp = to_number(product["mrp"])
            if mrp is not None and price > mrp + config.AMOUNT_TOLERANCE:
                fails.add("unit_price_not_above_mrp")

        # Consistency (foreign keys against the CLEAN masters)
        if row["customer_id"] != "" and row["customer_id"] not in clean_customer_ids:
            fails.add("customer_exists")
        if row["product_id"] not in clean_products:
            fails.add("product_exists")
        if row["store_id"] not in clean_store_ids:
            fails.add("store_exists")

        # Timeliness
        if ts is not None and ts > run_ts:
            fails.add("order_ts_not_future")

        # Warnings: value was fixed during standardisation
        if (raw.get("order_status") or "") != row["order_status"] and row["order_status"] in config.VALID_ORDER_STATUS:
            fails.add("order_status_standardised")
        if (raw.get("payment_mode") or "") != row["payment_mode"] and row["payment_mode"] in config.VALID_PAYMENT_MODES:
            fails.add("payment_mode_standardised")

        seen_ids.add(row["order_id"])
        checked.append((raw, row, fails))
    return split_rows("orders", checked)


# ══════════════════════════════════════════════════════════════════════════════
# Payments
# ══════════════════════════════════════════════════════════════════════════════

def validate_payments(rows, known_order_ids, run_ts):
    """
    known_order_ids: every order_id in the order export(s) of this run,
    good or bad. A payment for an order that was never exported is an orphan.
    (Whether the money matches is a separate question - see reconciliation.)
    """
    seen_ids = set()
    checked = []
    for raw in rows:
        row = dict(raw)
        for col in config.SOURCE_FILES["payments"][1]:
            row[col] = text(raw, col)
        row["payment_status"] = row["payment_status"].upper()
        row["payment_mode"] = standardise_payment_mode(row["payment_mode"])

        ts = to_timestamp(row["payment_ts"])
        paid = to_number(row["amount_paid"])
        fails = set()

        if row["payment_id"] in seen_ids:
            fails.add("payment_id_unique")
        if row["order_id"] == "":
            fails.add("order_id_not_null")
        if row["payment_mode"] not in config.VALID_PAYMENT_MODES:
            fails.add("payment_mode_valid")
        if row["payment_status"] not in config.VALID_PAYMENT_STATUS:
            fails.add("payment_status_valid")
        if ts is None:
            fails.add("payment_ts_format_valid")
        if paid is None:
            fails.add("amount_paid_parses")
        if row["payment_status"] == "SUCCESS" and paid is not None and paid <= 0:
            fails.add("success_amount_positive")
        if row["order_id"] != "" and row["order_id"] not in known_order_ids:
            fails.add("order_exists")
        if ts is not None and ts > run_ts:
            fails.add("payment_ts_not_future")
        if row["payment_mode"] != "Cash on Delivery" and row["gateway_ref"] == "":
            fails.add("gateway_ref_present")
        if (raw.get("payment_mode") or "") != row["payment_mode"] and row["payment_mode"] in config.VALID_PAYMENT_MODES:
            fails.add("payment_mode_standardised")

        seen_ids.add(row["payment_id"])
        checked.append((raw, row, fails))
    return split_rows("payments", checked)


# ══════════════════════════════════════════════════════════════════════════════
# Dataset-level checks (Warnings only). Used by BOTH pipelines.
# ══════════════════════════════════════════════════════════════════════════════

def dataset_checks(latest_valid_order_ts, orders_row_count, previous_row_count, run_ts):
    """
    latest_valid_order_ts : newest order_ts among orders that passed (datetime or None)
    orders_row_count      : rows in orders.csv this run
    previous_row_count    : rows in orders.csv on the previous run (None on first run)

    Returns dq check dicts like split_rows() does. rows_failed is either 0 (ok)
    or rows_checked (the whole file is affected).
    """
    stale = (
        latest_valid_order_ts is None
        or (run_ts - latest_valid_order_ts).total_seconds() > config.STALE_AFTER_HOURS * 3600
    )
    dropped = (
        previous_row_count is not None
        and previous_row_count > 0
        and orders_row_count < previous_row_count * (1 - config.VOLUME_DROP_PCT / 100)
    )
    print(f"[DQ] latest valid order_ts = {latest_valid_order_ts} | run_ts = {run_ts} | stale = {stale}")
    print(f"[DQ] orders rows now = {orders_row_count} | previous run = {previous_row_count} | big drop = {dropped}")
    return [
        {"check_name": "latest_order_is_fresh", "dimension": "Timeliness", "severity": "Warning",
         "rows_checked": orders_row_count, "rows_failed": orders_row_count if stale else 0},
        {"check_name": "volume_vs_previous_run", "dimension": "Completeness", "severity": "Warning",
         "rows_checked": orders_row_count, "rows_failed": orders_row_count if dropped else 0},
    ]
