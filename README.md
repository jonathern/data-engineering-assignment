# Sales ETL Pipeline

**Course:** Data Engineering and Cloud Computing \
**Group members:** Charles Muganga (B35391), Jonathan Kateega (B34976) \
**Repository link:** [Github](https://github.com/jonathern/data-engineering-assignment)

## Table of Contents
- [What this project does](#what-this-project-does)
- [What's in each folder](#whats-in-each-folder)
- [How to run it](#how-to-run-it)
- [How we cleaned the data](#how-we-cleaned-the-data)
- [How the folder acts like cloud storage](#how-the-folder-acts-like-cloud-storage)
- [Results](#results)
- [Challenges we faced](#challenges-we-faced)
- [Tools used](#tools-used)

## What this project does

A company has three branches: Kampala, Mbarara and Jinja. Each branch sends in a file of its sales, but the files have problems: some sales appear twice, some values are missing, dates are written in different ways, and one sale has a negative quantity.

This project builds a simple **ETL pipeline** to fix that:

1. **Extract:** read the raw sales file.
2. **Transform:** clean it up.
3. **Load:** save the clean data into a folder that acts like cloud storage.

We then load the clean data into a small database and use **SQL** to answer four business questions.

## What's in each folder

```
etl_sales_pipeline/
├── data/
│   ├── raw/sales.csv                  the original sales file from the branches
│   └── processed/cleaned_sales.csv    the cleaned file
├── src/
│   ├── etl_pipeline.py                cleans the data and saves it (Parts A and B)
│   └── run_analytics.py               loads the data into SQLite and runs the queries (Part C)
├── sql/
│   ├── sales_analytics.sql            creates the table and holds the four queries
│   └── load_with_sqlite_cli.sql       the same work, using only the sqlite3 command line
├── notebooks/
│   ├── 01_extract_transform_load.ipynb    walks through the cleaning, with evidence
│   └── 02_sql_analytics.ipynb             shows each query, its answer and charts
├── cloud_storage_bucket/              our pretend cloud storage
├── outputs/
│   ├── query_results/                 each query's answer saved as a CSV file
│   └── charts/                        charts and a picture of the query results
├── docs/REPORT.md                     the 1–2 page write-up
└── requirements.txt                   the Python libraries needed
```

**Scripts and notebooks have different jobs.** The scripts in `src/` do the actual work and can run on their own. The notebooks explain that work: they explore the data, call the scripts' functions and show the results. The notebooks never repeat the cleaning or query code, so every rule is written in only one place.

## How to run it

You need Python 3.10 or newer. Open a terminal in the main project folder, then:

**1. Install the libraries (only needed once):**
```
pip install -r requirements.txt
```

**2. Run the pipeline:**
```
python src/etl_pipeline.py
python src/run_analytics.py
```
The first command cleans the data and fills `cloud_storage_bucket/`. The second builds the database and saves the query results in `outputs/query_results/`. Always run them in this order.

**Or use the notebooks instead:**
```
jupyter notebook
```
Open the `notebooks` folder and run `01` first, then `02`. In each one, use **Run All**.

**Or, if you have the sqlite3 command-line tool**, you can do Part C with SQL alone. Run `etl_pipeline.py` first, then:
```
sqlite3 warehouse/sales.db < sql/load_with_sqlite_cli.sql
```

Some folders, like `warehouse/` and `outputs/query_results/`, are created automatically the first time you run the scripts, so don't worry if they're missing at first.

## How we cleaned the data

The raw file has **105 rows**. After cleaning there are **102**.

| Problem | Which row | What we did |
|---|---|---|
| The same sale appears twice | T010 | Kept the first copy and removed the second |
| Quantity is missing | T028 | Removed the row (see below) |
| Dates in different formats, like `03/01/2026` and `2026/01/04` | T002, T003, T004, T008 | Changed all dates to one format: `YYYY-MM-DD` |
| Negative quantity (-2) | T101 | Removed the row, since you can't sell a negative amount |
| No total for each sale | All rows | Added a `total_amount` column (quantity × unit price) |

**Why we removed T028 instead of guessing its quantity.** If we filled in an average, we would be inventing a laptop sale worth millions of shillings that may never have happened, and every revenue total would be wrong. Our rule is simple:
- If a number we need for revenue is missing, we remove the row.
- If only a description is missing (branch, product or category), we keep the row and write "Unknown", because the sale itself is still real.

**How we read the dates.** A date like `03/01/2026` could mean 3 January or 1 March. The transaction IDs go up roughly one per day, and T001 is 3 January, so T002 must be 3 January too. This tells us the branches write the day first, so we read the dates that way.

**Removed rows aren't lost.** All three are saved in `cloud_storage_bucket/quarantine/`, together with the reason each one was removed, so a branch can correct them and send them again.

## How the folder acts like cloud storage

`cloud_storage_bucket/` plays the part of a real cloud storage service, like Amazon S3 or Google Cloud Storage:

- **`raw/`** keeps the original file exactly as it arrived, in case we ever need to clean it again.
- **`sales/2026/`** holds the clean data, grouped by year. Grouping files by year is common in the cloud, because a question about one year then only needs to read that year's files.
- **`quarantine/`** keeps the rows we removed, so they can be checked.
- **`_manifest.json`** files record the details of each saved file: its size, how many rows it has, and when it was saved. Cloud services keep the same kind of information.

## Results

| Question | Answer |
|---|---|
| a) Total revenue for each branch | Kampala UGX 61,395,000 · Mbarara UGX 59,410,000 · Jinja UGX 51,435,000 |
| b) Total revenue for each category | Electronics UGX 109,850,000 · Furniture UGX 42,170,000 · Accessories UGX 20,220,000 |
| c) Top 5 products by sales | Laptop (52.50M), Monitor (36.55M), Office Chair (23.45M), Printer (20.80M), Desk (18.72M) |
| d) Average transaction value | UGX 1,688,627.45 |

Total revenue across all 102 sales is **UGX 172,240,000**.

**What the numbers tell us:**
- Kampala earned the most, even though Jinja sold more items, because Kampala sold more expensive electronics.
- Electronics brought in almost two-thirds of all revenue.
- Laptops alone brought in about 30% of the total.

## Challenges we faced

Building a robust pipeline meant solving a few tricky data and engineering problems:

- **Preventing silent data corruption:** By default, tools like pandas will quietly turn legitimate text like `"NA"` (e.g., a branch named "North America") into missing `NaN` values. We had to carefully configure the extraction step to treat all incoming data as raw text first, preventing accidental data loss.
- **Safely handling duplicates:** Simply "keeping the first" duplicate transaction ID can be dangerous. If the first row is corrupted (e.g., missing a quantity) and the second is valid, automation would discard the good data. We updated the logic to quarantine *all* duplicate IDs for manual audit, prioritizing data integrity over blind automation.
- **Performance at scale:** Our initial custom date-parsing loop worked for small files, but would be a major bottleneck for millions of rows. We replaced it with pandas' vectorized `to_datetime` engine, which handles multiple date formats instantly and efficiently.
- **Memory efficiency:** To simulate real cloud storage, we generate MD5 checksums for every saved file. Reading large files into memory all at once causes crashes, so we wrote the hashing function to process files in small, safe chunks (64KB at a time).

## Tools used

- **Python** with **pandas**, for reading and cleaning the data
- **SQLite**, a small database that needs no setup, for the SQL queries
- **Jupyter Notebook**, for explaining each step
- **Matplotlib**, for the charts
