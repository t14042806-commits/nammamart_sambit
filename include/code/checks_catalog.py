"""
include/code/checks_catalog.py
-----------------------------------
The single list of every data-quality check (Part D).

Each check has:
  name        - short id. Python (rules.py) and SQL (stg_*.sql, column chk_<name>)
                use EXACTLY this name, so dq_results rows from ETL and ELT can be compared.
  dimension   - Completeness / Uniqueness / Validity / Accuracy / Consistency / Timeliness
  severity    - "Critical": the row is quarantined (never reaches finance tables)
                "Warning" : the row is loaded, but the problem is logged
  description - plain English, used in docs and alerts

The ORDER of checks in each list matters a little: failure_reason text lists
failed checks in this order in both pipelines, so they produce identical strings.

Severity decisions that differ from the "suggested" column in ASSIGNMENT.md are
marked "DECISION" - explain these in SUBMISSION.md and confirm with the client.
"""

CRITICAL = "Critical"
WARNING = "Warning"


def _c(name, dimension, severity, description):
    return {"name": name, "dimension": dimension, "severity": severity, "description": description}


CHECKS = {
    # ── orders.csv (and the increment file, which has the same columns) ───────
    "orders": [
        _c("order_id_not_null",              "Completeness", CRITICAL, "order_id is present"),
        _c("customer_id_not_null",           "Completeness", CRITICAL, "customer_id is present"),
        _c("order_ts_not_null",              "Completeness", CRITICAL, "order_ts is present"),
        _c("delivered_has_delivery_minutes", "Completeness", CRITICAL, "a DELIVERED order has delivery_minutes"),
        _c("order_id_unique",                "Uniqueness",   CRITICAL, "order_id appears once (later copies are rejected)"),
        _c("order_status_valid",             "Validity",     CRITICAL, "order_status is DELIVERED / CANCELLED / RETURNED"),
        _c("payment_mode_valid",             "Validity",     CRITICAL, "payment_mode is UPI / Card / Cash on Delivery / Wallet"),
        _c("order_ts_format_valid",          "Validity",     CRITICAL, "order_ts parses as YYYY-MM-DD HH:MM:SS"),
        _c("numbers_parse",                  "Validity",     CRITICAL, "quantity, unit_price, discount_pct, order_amount, delivery_minutes are numbers"),
        _c("discount_pct_in_range",          "Validity",     CRITICAL, "discount_pct is between 0 and 100"),
        _c("quantity_positive",              "Accuracy",     CRITICAL, "quantity > 0"),
        _c("order_amount_matches_formula",   "Accuracy",     CRITICAL, "order_amount = quantity x unit_price x (1 - discount_pct/100)"),
        _c("delivery_minutes_believable",    "Accuracy",     CRITICAL, "delivery_minutes is between 1 and 120"),
        _c("unit_price_not_above_mrp",       "Accuracy",     CRITICAL, "unit_price is not higher than the product MRP (catches price drift)"),
        _c("customer_exists",                "Consistency",  CRITICAL, "customer_id exists in the clean customer master"),
        _c("product_exists",                 "Consistency",  CRITICAL, "product_id exists in the clean product master"),
        _c("store_exists",                   "Consistency",  CRITICAL, "store_id exists in the clean store master"),
        _c("order_ts_not_future",            "Timeliness",   CRITICAL, "order_ts is not later than the run time"),
        _c("order_status_standardised",      "Consistency",  WARNING,  "order_status had to be re-cased/trimmed (e.g. 'delivered')"),
        _c("payment_mode_standardised",      "Consistency",  WARNING,  "payment_mode had to be mapped (e.g. 'upi', 'UPI ', 'cod')"),
    ],

    # ── payments.csv ──────────────────────────────────────────────────────────
    "payments": [
        _c("payment_id_unique",          "Uniqueness",   CRITICAL, "payment_id appears once"),
        _c("order_id_not_null",          "Completeness", CRITICAL, "order_id is present"),
        _c("payment_mode_valid",         "Validity",     CRITICAL, "payment_mode is in the allowed list"),
        _c("payment_status_valid",       "Validity",     CRITICAL, "payment_status is SUCCESS / FAILED / REFUNDED"),
        _c("payment_ts_format_valid",    "Validity",     CRITICAL, "payment_ts parses as YYYY-MM-DD HH:MM:SS"),
        _c("amount_paid_parses",         "Validity",     CRITICAL, "amount_paid is a number"),
        _c("success_amount_positive",    "Accuracy",     CRITICAL, "a SUCCESS payment has amount_paid > 0"),
        _c("order_exists",               "Consistency",  CRITICAL, "order_id exists in the order export"),
        _c("payment_ts_not_future",      "Timeliness",   CRITICAL, "payment_ts is not later than the run time"),
        _c("gateway_ref_present",        "Completeness", WARNING,  "non-cash payments have a gateway_ref"),
        _c("payment_mode_standardised",  "Consistency",  WARNING,  "payment_mode had to be mapped"),
    ],

    # ── customers.csv ─────────────────────────────────────────────────────────
    "customers": [
        _c("customer_id_not_null",       "Completeness", CRITICAL, "customer_id is present"),
        _c("customer_id_unique",         "Uniqueness",   CRITICAL, "customer_id appears once (later copies are rejected)"),
        _c("loyalty_tier_valid",         "Validity",     CRITICAL, "loyalty_tier is Bronze / Silver / Gold / Platinum"),
        _c("signup_date_valid",          "Validity",     CRITICAL, "signup_date parses as YYYY-MM-DD"),
        _c("signup_date_not_future",     "Timeliness",   CRITICAL, "signup_date is not after the run date"),
        _c("email_format_valid",         "Validity",     WARNING,  "email looks like name@domain.tld"),
        _c("phone_10_digits",            "Validity",     WARNING,  "phone is exactly 10 digits"),
        _c("pincode_6_digits",           "Validity",     WARNING,  "pincode is exactly 6 digits"),
        _c("city_not_null",              "Completeness", WARNING,  "city is present"),
        _c("loyalty_tier_standardised",  "Consistency",  WARNING,  "loyalty_tier had to be re-cased (e.g. 'BRONZE')"),
    ],

    # ── products.csv ──────────────────────────────────────────────────────────
    "products": [
        _c("product_id_unique",          "Uniqueness",   CRITICAL, "product_id appears once (later copies are rejected)"),
        _c("category_not_null",          "Completeness", CRITICAL, "category is present"),
        _c("prices_parse",               "Validity",     CRITICAL, "mrp and cost_price are numbers"),
        _c("launch_date_valid",          "Validity",     CRITICAL, "launch_date parses as YYYY-MM-DD"),
        _c("mrp_positive",               "Accuracy",     CRITICAL, "mrp > 0"),
        _c("launch_date_not_future",     "Timeliness",   CRITICAL, "launch_date is not after the run date"),
        # DECISION: cost_price > mrp is suspicious but the product is real and sold,
        # so we load it (margin will show negative) instead of losing its revenue.
        _c("cost_price_not_above_mrp",   "Accuracy",     WARNING,  "cost_price <= mrp"),
    ],

    # ── stores.csv ────────────────────────────────────────────────────────────
    "stores": [
        _c("store_id_not_null",          "Completeness", CRITICAL, "store_id is present"),
        _c("store_id_unique",            "Uniqueness",   CRITICAL, "store_id appears once"),
        _c("open_date_valid",            "Validity",     CRITICAL, "open_date parses as YYYY-MM-DD"),
        _c("open_date_not_future",       "Timeliness",   CRITICAL, "open_date is not after the run date"),
        # DECISION: a missing zone is a Warning. Rejecting store S07 would drop
        # ~176 real orders; instead the zone is shown as 'Unknown' and flagged.
        _c("zone_valid",                 "Completeness", WARNING,  "zone is North / South / East / West"),
        _c("pincode_6_digits",           "Validity",     WARNING,  "pincode is exactly 6 digits"),
        _c("capacity_positive",          "Accuracy",     WARNING,  "capacity_orders_per_day > 0"),
    ],
}

# Dataset-level checks: they look at the whole file, not one row.
# They are always Warnings (they never quarantine rows).
DATASET_CHECKS = [
    _c("latest_order_is_fresh",   "Timeliness",   WARNING, "newest valid order_ts is less than 24 hours old at run time"),
    _c("volume_vs_previous_run",  "Completeness", WARNING, "orders row count did not drop by more than 90% vs the previous run"),
]


def checks_for(entity):
    """List of check dicts for 'orders', 'payments', ..."""
    return CHECKS[entity]


def severity_of(entity, check_name):
    for c in CHECKS[entity]:
        if c["name"] == check_name:
            return c["severity"]
    raise KeyError(f"Unknown check {entity}.{check_name}")


def dimension_of(check_name):
    """Look up a check's dimension (searches all entities and dataset checks)."""
    for check_list in list(CHECKS.values()) + [DATASET_CHECKS]:
        for c in check_list:
            if c["name"] == check_name:
                return c["dimension"]
    return "Unknown"
