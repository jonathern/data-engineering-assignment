"""
This script handles Parts A and B of the assignment. 
It takes the raw sales file from the branches, cleans it, and saves the clean version into a folder
that acts like cloud storage.

It works in three steps:
    1. Extract   - read sales.csv
    2. Transform - remove duplicates, deal with missing values, fix the dates,
                   drop negative quantities and add a total_amount column
    3. Load      - save cleaned_sales.csv locally and into cloud_storage_bucket/

How the folder acts like cloud storage:
    - cloud_storage_bucket/ plays the part of a cloud bucket, like Amazon S3
      or Google Cloud Storage.
    - Files are grouped by year (sales/2026/), the same way cloud data is
      usually organised so it is quicker to search.
    - raw/ keeps the original file as it arrived, sales/ holds the clean data,
      and quarantine/ keeps the rows we rejected so they can be checked later.
    - Each saved file gets a small _manifest.json with its size, number of
      rows and upload time, like the details a cloud service records.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

### Configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "sales.csv"
DEFAULT_PROCESSED = PROJECT_ROOT / "data" / "processed" / "cleaned_sales.csv"
DEFAULT_BUCKET = PROJECT_ROOT / "cloud_storage_bucket"

EXPECTED_COLUMNS = [
    "transaction_id", "transaction_date", "branch", "product",
    "category", "quantity", "unit_price",
]

## the columns without which a sale cannot be trusted or valued, we reject the row.
CRITICAL_COLUMNS = ["transaction_id", "transaction_date", "quantity", "unit_price"]

# Descriptive columns -> keep the sale, label the gap.
DESCRIPTIVE_COLUMNS = ["branch", "product", "category"]

#since we have a number of date formats, we need to cater for all of them
DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("etl")

#  The ETL process 

# EXTRACT
def extract(path: Path) -> pd.DataFrame:
    """Read the raw CSV as text so nothing is silently coerced on the way in."""
    # FIX: keep_default_na=False prevents legitimate "NA" or "N/A" strings from 
    # being silently converted to float NaN by pandas.
    df = pd.read_csv(path, dtype=str, keep_default_na=False, skipinitialspace=True)
    
    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Input file is missing required columns: {sorted(missing)}")
    
    log.info("EXTRACT  | %d rows read from %s", len(df), path.name)
    return df[EXPECTED_COLUMNS]


## TRANSFORM helpers
def parse_date(value) -> pd.Timestamp:
    """we try each known format in turn; return NaT if none matches."""
    if pd.isna(value):
        return pd.NaT
    text = str(value).strip()
    for format in DATE_FORMATS:
        try:
            return pd.Timestamp(datetime.strptime(text, format))
        except ValueError:
            continue
    return pd.NaT


def _reject(rejected: list[pd.DataFrame], rows: pd.DataFrame, reason: str,
            original: pd.DataFrame) -> None:
    """we quarantine rows exactly as they were received"""
    if not rows.empty:
        rows = original.loc[rows.index].copy()
        rows["rejection_reason"] = reason
        rejected.append(rows)
        # FIX: Handle NaN transaction IDs gracefully in logs
        ids = rows["transaction_id"].fillna("MISSING_ID").astype(str).tolist()
        log.info("TRANSFORM| rejected %d row(s): %s -> %s", len(rows), reason, ", ".join(ids))


def transform(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    The cleaning process of the raw data.

    This function returns (clean_df, rejected_df, quality_report).
    Rejected rows are not thrown away; rather they are returned with a reason so
    they can be quarantined and audited.
    """
    if raw.empty:
        log.info("TRANSFORM| Input dataframe is empty. Returning empty results.")
        empty_rejected = pd.DataFrame(columns=EXPECTED_COLUMNS + ["rejection_reason"])
        return raw.copy(), empty_rejected, {"rows_in": 0, "rows_out": 0, "rows_rejected": 0}

    df = raw.copy()
    original = raw.copy()          # untouched values, used for the quarantine file
    rejected: list[pd.DataFrame] = []
    report = {"rows_in": len(df)}

    # 1. Normalize text by trimming whitespaces and turning empty strings into NaN.
    for col in df.columns:
        df[col] = df[col].str.strip().replace("", pd.NA)

    # 2. Removing duplicates 
    # (a) Remove exact duplicate rows
    exact_dupes = df.duplicated(keep="first")
    _reject(rejected, df[exact_dupes], "exact duplicate record", original)
    df = df[~exact_dupes]
    
    # (b) Same transaction_id but different content. 
    # FIX: Use keep=False to reject ALL duplicates, preventing the scenario where 
    # an invalid first row is kept and a valid second row is discarded.
    id_dupes = df["transaction_id"].notna() & df.duplicated("transaction_id", keep=False)
    _reject(rejected, df[id_dupes], "duplicate transaction_id", original)
    df = df[~id_dupes]
    
    report["duplicates_removed"] = int(exact_dupes.sum() + id_dupes.sum())

    # 3. Converting types (invalid numbers become NaN and are caught below)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    # 4. Handle missing values 
    # Reject rows missing critical fields (imputing would invent revenue).
    missing_critical = df[CRITICAL_COLUMNS].isna().any(axis=1)
    _reject(rejected, df[missing_critical], "missing critical value (id/date/quantity/price)", original)
    df = df[~missing_critical]
    report["missing_critical_removed"] = int(missing_critical.sum())
    
    # For descriptive fields, keep the sale and label it.
    filled = int(df[DESCRIPTIVE_COLUMNS].isna().sum().sum())
    df[DESCRIPTIVE_COLUMNS] = df[DESCRIPTIVE_COLUMNS].fillna("Unknown")
    report["descriptive_values_filled"] = filled

    # 5. Standardize transaction_date 
    # FIX: Use vectorized pandas to_datetime with format="mixed" for massive performance gains.
    parsed = pd.to_datetime(df["transaction_date"], format="mixed", dayfirst=True, errors="coerce")
    
    report["dates_reformatted"] = int(
        (parsed.notna() & (parsed.dt.strftime("%Y-%m-%d") != df["transaction_date"])).sum()
    )
    bad_dates = parsed.isna()
    _reject(rejected, df[bad_dates], "unparseable transaction_date", original)
    df = df[~bad_dates].copy()
    df["transaction_date"] = parsed[~bad_dates]
    report["bad_dates_removed"] = int(bad_dates.sum())

    # 6. Removing invalid quantities / prices 
    bad_qty = df["quantity"] <= 0
    _reject(rejected, df[bad_qty], "non-positive quantity", original)
    df = df[~bad_qty]
    
    bad_price = df["unit_price"] <= 0
    _reject(rejected, df[bad_price], "non-positive unit_price", original)
    df = df[~bad_price]
    
    report["invalid_quantity_removed"] = int(bad_qty.sum())
    report["invalid_price_removed"] = int(bad_price.sum())

    # 7. Derive total_amount 
    df["quantity"] = df["quantity"].astype("int64")
    # Prices are whole Uganda shillings, so keep integers when possible.
    if (df["unit_price"] % 1 == 0).all():
        df["unit_price"] = df["unit_price"].astype("int64")
    df["total_amount"] = df["quantity"] * df["unit_price"]

    df = df.sort_values(["transaction_date", "transaction_id"]).reset_index(drop=True)

    # 8. Final quality gate
    assert df["transaction_id"].is_unique, "transaction_id not unique after cleaning"
    assert df.notna().all().all(), "nulls remain after cleaning"
    assert (df["quantity"] > 0).all(), "non-positive quantity remains"

    rejected_df = (pd.concat(rejected, ignore_index=True) if rejected
                   else pd.DataFrame(columns=EXPECTED_COLUMNS + ["rejection_reason"]))
    
    report["rows_out"] = len(df)
    report["rows_rejected"] = len(rejected_df)
    log.info("TRANSFORM| %d clean rows, %d rejected", len(df), len(rejected_df))
    
    return df, rejected_df, report


# LOAD (a simulation of object storage)
def _md5(path: Path) -> str:
    """Calculate MD5 hash of a file in chunks to prevent OOM on large files."""
    # FIX: Chunked reading maintains a constant, low memory footprint.
    md5_hash = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def put_object(df: pd.DataFrame, bucket: Path, key: str) -> Path:
    """
    Write a DataFrame to `bucket/key` like an object-store PUT: 
    the whole object is written to a temp file and atomically renamed,
    then a metadata sidecar (_manifest.json) is written beside it.
    """
    target = bucket / key
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    
    out = df.copy()
    if "transaction_date" in out and pd.api.types.is_datetime64_any_dtype(out["transaction_date"]):
        out["transaction_date"] = out["transaction_date"].dt.strftime("%Y-%m-%d")
    out.to_csv(tmp, index=False)
    tmp.replace(target)

    manifest = {
        "bucket": bucket.name,
        "key": key,
        "content_type": "text/csv",
        "size_bytes": target.stat().st_size,
        "row_count": len(df),
        "md5_etag": _md5(target),
        "uploaded_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    
    # FIX: Tie manifest name to the specific file to prevent collisions 
    # if partitioning granularity changes (e.g., by month/day).
    manifest_path = target.parent / f"{target.stem}_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    
    log.info("LOAD     | s3-style PUT  %s/%s  (%d rows)", bucket.name, key, len(df))
    return target


def load(clean: pd.DataFrame, rejected: pd.DataFrame, raw_path: Path,
         bucket: Path, processed_path: Path) -> dict:
    # A local copy of the deliverable cleaned_sales.csv
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    out = clean.copy()
    if "transaction_date" in out and pd.api.types.is_datetime64_any_dtype(out["transaction_date"]):
        out["transaction_date"] = out["transaction_date"].dt.strftime("%Y-%m-%d")
    out.to_csv(processed_path, index=False)
    log.info("LOAD     | wrote %s", processed_path.relative_to(PROJECT_ROOT))

    keys = {}
    
    # Raw / landing zone: the file exactly as the branch sent it.
    year_now = datetime.now().year
    raw_key = f"raw/sales/{year_now}/{raw_path.name}"
    (bucket / raw_key).parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(raw_path, bucket / raw_key)
    keys["raw"] = raw_key

    # Curated zone partitioned by the year of the transaction.
    for year, part in clean.groupby(clean["transaction_date"].dt.year):
        key = f"sales/{year}/cleaned_sales.csv"
        put_object(part, bucket, key)
        keys[f"curated_{year}"] = key

    # Quarantine zone for rejected rows (kept for audit, never deleted).
    if not rejected.empty:
        key = f"quarantine/sales/{year_now}/rejected_sales.csv"
        put_object(rejected, bucket, key)
        keys["quarantine"] = key
        
    return keys


# ORCHESTRATION
def run(input_path: Path = DEFAULT_INPUT, bucket: Path = DEFAULT_BUCKET,
        processed_path: Path = DEFAULT_PROCESSED) -> dict:
    log.info("ETL run started ")
    raw = extract(input_path)
    clean, rejected, report = transform(raw)
    report["objects_written"] = load(clean, rejected, input_path, bucket, processed_path)
    
    (bucket / "_pipeline_run_report.json").write_text(json.dumps(report, indent=2))
    
    # Log summary excluding the verbose objects_written dict
    summary = {k: v for k, v in report.items() if k != "objects_written"}
    log.info("ETL run finished: %s", summary)
    return report


def main() -> None:
    p = argparse.ArgumentParser(description="Sales ETL pipeline")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to raw input CSV")
    p.add_argument("--bucket", type=Path, default=DEFAULT_BUCKET, help="Path to simulated cloud bucket")
    p.add_argument("--processed", type=Path, default=DEFAULT_PROCESSED, help="Path to local processed output")
    a = p.parse_args()
    
    run(a.input.resolve(), a.bucket.resolve(), a.processed.resolve())


if __name__ == "__main__":
    main()