"""
include/code/csv_utils.py
------------------------------
Tiny helpers to read and write CSV files with Python's built-in `csv` module.
No pandas needed.

A "row" everywhere in this project is a plain dictionary, for example:
    {"order_id": "ORD000001", "quantity": "2", ...}
Every value starts as TEXT (a string), exactly as it was in the file.
We convert to numbers/dates only when we need them.
"""

import csv
from pathlib import Path


class SchemaError(Exception):
    """Raised when a file's header doesn't have the columns we expect."""


def read_csv_rows(path, expected_columns=None):
    """
    Read a CSV file and return a list of dictionaries (one per row).

    - Adds "_row_num" (1 = first data row) so every row can be traced back
      to its exact line in the source file. This is our lineage hook.
    - If expected_columns is given, checks the header first and raises
      SchemaError if any column is missing. Fail loudly, never silently.
    """
    path = Path(path)
    if not path.exists():
        # FileNotFoundError makes the Airflow task fail -> retries -> failed (experiment 4)
        raise FileNotFoundError(f"Source file not found: {path}")

    # utf-8-sig also removes the invisible BOM character Excel sometimes adds
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []

        if expected_columns:
            missing = [c for c in expected_columns if c not in header]
            if missing:
                raise SchemaError(
                    f"{path.name}: missing expected column(s) {missing}. "
                    f"Header found: {header}"
                )

        rows = []
        for i, row in enumerate(reader, start=1):
            row["_row_num"] = str(i)
            rows.append(row)
    return rows


def write_csv_rows(path, rows, columns=None):
    """
    Write a list of dictionaries to a CSV file (creates folders if needed).
    If columns is not given, uses the keys of the first row.
    Returns the path as a string (handy for passing between Airflow tasks).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if columns is None:
        columns = list(rows[0].keys()) if rows else []

    with open(path, "w", newline="", encoding="utf-8") as f:
        # extrasaction="ignore" -> keys not in `columns` are simply not written
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: ("" if row.get(c) is None else row.get(c)) for c in columns})
    return str(path)


def safe_name(text):
    """
    Airflow run_ids look like  manual__2026-10-12T03:30:00+00:00
    ':' and '+' are not allowed in Windows file names, so replace them.
    """
    return "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in str(text))
