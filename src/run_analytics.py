"""
This script handles Part C of the assignment. It takes the cleaned sales data
and answers the business questions using SQL.

What it does:
    1. Creates a SQLite database (warehouse/sales.db) with a "sales" table
    2. Loads cleaned_sales.csv from cloud_storage_bucket/ into that table
    3. Runs each query in sql/sales_analytics.sql
    4. Saves every result as a CSV file in outputs/query_results/

"""

from __future__ import annotations

import csv
import logging
import re
import sqlite3
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUCKET_OBJECT = PROJECT_ROOT / "cloud_storage_bucket" / "sales" / "2026" / "cleaned_sales.csv"
SQL_FILE = PROJECT_ROOT / "sql" / "sales_analytics.sql"
DB_PATH = PROJECT_ROOT / "warehouse" / "sales.db"
RESULTS_DIR = PROJECT_ROOT / "outputs" / "query_results"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("analytics")


def read_sql_blocks(sql_file: Path = SQL_FILE) -> dict[str, str]:
    """We split the SQL file into {name: sql} using '-- name: <name>' markers."""
    text = sql_file.read_text()
    parts = re.split(r"^--\s*name:\s*(\w+)\s*$", text, flags=re.MULTILINE)
    # parts = [preamble, name1, body1, name2, body2, ...]
    return {parts[i]: parts[i + 1].strip() for i in range(1, len(parts), 2)}


def load_sales(conn: sqlite3.Connection, csv_path: Path = BUCKET_OBJECT) -> int:
    """ We insert the cleaned CSV into the sales table inside one transaction."""
    with csv_path.open(newline="") as f:
        rows = [
            (r["transaction_id"], r["transaction_date"], r["branch"], r["product"],
             r["category"], int(r["quantity"]), int(float(r["unit_price"])),
             int(float(r["total_amount"])))
            for r in csv.DictReader(f)
        ]
    with conn: 
        conn.executemany("INSERT INTO sales VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    log.info("LOAD     | %d rows inserted into sales from %s", len(rows),
             csv_path.relative_to(PROJECT_ROOT))
    return len(rows)


def build_database(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """ we create the database, the sales table, and load the data."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    blocks = read_sql_blocks()
    conn.executescript(blocks["create_sales_table"])
    loaded = load_sales(conn)
    (count,) = conn.execute("SELECT COUNT(*) FROM sales").fetchone()
    assert count == loaded, f"row count mismatch: {count} in table vs {loaded} in file"
    return conn


def run_queries(conn: sqlite3.Connection, out_dir: Path = RESULTS_DIR) -> dict[str, pd.DataFrame]:
    """ we run every query block (except DDL) and export each result to CSV."""
    out_dir.mkdir(parents=True, exist_ok=True)
    results = {}
    for name, sql in read_sql_blocks().items():
        if name == "create_sales_table":
            continue
        df = pd.read_sql_query(sql, conn)
        df.to_csv(out_dir / f"{name}.csv", index=False)
        results[name] = df
        log.info("QUERY    | %-35s -> %d row(s) -> %s.csv", name, len(df), name)
    return results


def main() -> None:
    conn = build_database()
    results = run_queries(conn)
    conn.close()
    pd.set_option("display.float_format", "{:,.2f}".format)
    for name, df in results.items():
        print(f"\n=== {name} ===")
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()