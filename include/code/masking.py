"""
include/code/masking.py
----------------------------
Hide personal data (PII) before it reaches any reporting table.

Why MASK instead of DELETE?
  - The masked value is still useful: ops can see "pr***@example.com" and
    "98******21" to recognise a customer on a support call, without being
    able to contact or identify them from the dashboard.
  - customer_id stays as the join key, so analytics still work.
  - The unmasked original stays only in the raw layer (ELT) / source file,
    which only a small, approved group should be able to read.

The SQL versions of these functions live in include/sql/elt/stg_customers.sql
and produce exactly the same output, so ETL and ELT tables look identical.
"""


def mask_email(email):
    """
    "priya.hegde199@example.com" -> "pr***@example.com"
    Anything that is not a real-looking email (blank, "N/A", no "@") -> "***"
    """
    email = (email or "").strip()
    if "@" not in email:
        return "***"
    local_part, domain = email.split("@", 1)
    return local_part[:2] + "***@" + domain


def mask_phone(phone):
    """
    "9774349021" -> "97******21"   (keep first 2 and last 2 digits)
    Too short to mask safely (< 5 characters) -> "***"
    """
    phone = (phone or "").strip()
    if len(phone) < 5:
        return "***"
    return phone[:2] + "*" * (len(phone) - 4) + phone[-2:]


def mask_name(full_name):
    """
    "Priya Hegde" -> "Priya H."   (first name + initial of the last word)
    A one-word name is kept as is.
    Enough for a "top customers" list, not enough to identify the person.
    """
    parts = (full_name or "").split()
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return parts[0] + " " + parts[-1][0] + "."
