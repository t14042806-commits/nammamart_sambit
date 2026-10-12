"""
include/code/config.py
---------------------------
One place for every path, file name and business constant.

If the client changes a rule (for example "deliveries may now take up to
150 minutes"), you change it here and BOTH pipelines pick it up.
The ELT SQL files receive the same values through {{placeholders}}
(see warehouse.render_sql), so Python and SQL never drift apart.
"""

from pathlib import Path

# ── Who am I? Used in table/file names so I don't collide with other learners ──
LEARNER = "sambit"

# ── Folders ───────────────────────────────────────────────────────────────────
# nammamart_sambit/ is a standalone Astro project. Astro live-syncs its dags/ and
# include/ folders with the container (/usr/local/airflow/dags, /usr/local/airflow/include),
# so everything the DAGs read or write lives under include/:
#   include/code/  include/sql/  include/data/  include/output/
# Path(__file__) is this file: parents[0] = include/code, parents[1] = include.
# Relative to the code (not hard-coded), so it works on your laptop AND in the container.
INCLUDE_DIR = Path(__file__).resolve().parents[1]           # .../nammamart_sambit/include
DEFAULT_DATA_DIR = INCLUDE_DIR / "data"
SQL_DIR = INCLUDE_DIR / "sql"
EXPERIMENTS_DIR = INCLUDE_DIR / "data_experiments"          # broken copies for Part F

# Files the pipelines WRITE (gitignored; visible in VS Code thanks to live sync)
OUTPUT_DIR = INCLUDE_DIR / "output"
QUARANTINE_DIR = OUTPUT_DIR / "quarantine"
WORK_DIR = OUTPUT_DIR / "work"                             # intermediate files between ETL tasks
WAREHOUSE_PATH = OUTPUT_DIR / f"warehouse_{LEARNER}.duckdb"

# ── Source files and the columns we EXPECT in each ────────────────────────────
# Checking the header before reading is our "schema" observability check:
# if someone renames order_amount -> total, the extract task fails loudly
# instead of silently loading wrong data (Part F, experiment 6).
ORDER_COLUMNS = [
    "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
    "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
    "channel", "delivery_minutes",
]

SOURCE_FILES = {
    # logical name : (file name, expected columns, primary key column)
    "orders":    ("orders.csv", ORDER_COLUMNS, "order_id"),
    "payments":  ("payments.csv",
                  ["payment_id", "order_id", "payment_ts", "payment_mode",
                   "amount_paid", "payment_status", "gateway_ref"], "payment_id"),
    "customers": ("customers.csv",
                  ["customer_id", "full_name", "email", "phone", "city", "area",
                   "pincode", "signup_date", "loyalty_tier", "is_active"], "customer_id"),
    "products":  ("products.csv",
                  ["product_id", "product_name", "category", "sub_category", "brand",
                   "mrp", "cost_price", "unit", "is_perishable", "launch_date"], "product_id"),
    "stores":    ("stores.csv",
                  ["store_id", "store_name", "zone", "area", "pincode", "open_date",
                   "manager_name", "capacity_orders_per_day"], "store_id"),
}

# The next-day file has the same columns as orders.csv
INCREMENT_FILE = "orders_2026-10-01_increment.csv"

# ── Business rules agreed with the client (Section 3 of ASSIGNMENT.md) ─────────
VALID_ORDER_STATUS = ["DELIVERED", "CANCELLED", "RETURNED"]
VALID_PAYMENT_MODES = ["UPI", "Card", "Cash on Delivery", "Wallet"]
VALID_PAYMENT_STATUS = ["SUCCESS", "FAILED", "REFUNDED"]
VALID_LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
VALID_ZONES = ["North", "South", "East", "West"]

# Messy spellings we have seen in the data -> the one correct spelling.
# Keys are compared after .strip().lower(), so "UPI ", "upi" and "Upi" all match "upi".
# NOTE: "Cashh" is deliberately NOT mapped. It could mean Cash on Delivery,
# but we don't guess with money - it goes to quarantine and is a client question.
PAYMENT_MODE_MAP = {
    "upi": "UPI",
    "card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "wallet": "Wallet",
}

MAX_DELIVERY_MINUTES = 120      # above this the value is not believable (hours, not minutes)
SLA_MINUTES = 15                # "within 15 minutes" KPI
AMOUNT_TOLERANCE = 0.01         # rupees; allowed rounding difference when comparing amounts
STALE_AFTER_HOURS = 24          # latest order older than this at run time -> Warning
VOLUME_DROP_PCT = 90            # row count dropped by more than this vs previous run -> Warning

TS_FORMAT = "%Y-%m-%d %H:%M:%S"  # the ONLY accepted timestamp format for order_ts / payment_ts
DATE_FORMAT = "%Y-%m-%d"         # the ONLY accepted date format for signup/launch/open dates
TIMEZONE = "Asia/Kolkata"        # NammaMart's timestamps are local India time


def sql_list(values):
    """Turn ["A", "B"] into the SQL text  'A', 'B'  for use inside IN (...)."""
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def sql_params():
    """
    The constants above, as text, for the {{placeholders}} in the ELT .sql files.
    Example: {{valid_order_status}} in SQL becomes  'DELIVERED', 'CANCELLED', 'RETURNED'
    """
    return {
        "valid_order_status": sql_list(VALID_ORDER_STATUS),
        "valid_payment_modes": sql_list(VALID_PAYMENT_MODES),
        "valid_payment_status": sql_list(VALID_PAYMENT_STATUS),
        "valid_loyalty_tiers": sql_list(VALID_LOYALTY_TIERS),
        "valid_zones": sql_list(VALID_ZONES),
        "max_delivery_minutes": str(MAX_DELIVERY_MINUTES),
        "sla_minutes": str(SLA_MINUTES),
        "amount_tolerance": str(AMOUNT_TOLERANCE),
        "ts_format": TS_FORMAT,
        "date_format": DATE_FORMAT,
    }
