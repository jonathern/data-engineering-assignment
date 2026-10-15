-- 
-- Another way to do Part C, using only the sqlite3 command-line tool instead
-- of Python. It does the same job as run_analytics.py: creates the table,
-- loads the clean data, runs the four queries and saves each result as CSV.
--
-- How to run it (from the main project folder):
--     sqlite3 warehouse/sales.db < sql/load_with_sqlite_cli.sql
--
-- You need sqlite3 version 3.32 or newer. Check with: sqlite3 --version
-- Lines starting with a dot (like .import) are sqlite3 commands, not SQL.
-- 


-- Step 1: Create the sales table (same design as in sales_analytics.sql).
-- We delete the old table first so the data is never loaded twice.
DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    transaction_id   TEXT    PRIMARY KEY,
    transaction_date TEXT    NOT NULL,
    branch           TEXT    NOT NULL,
    product          TEXT    NOT NULL,
    category         TEXT    NOT NULL,
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    unit_price       INTEGER NOT NULL CHECK (unit_price > 0),
    total_amount     INTEGER NOT NULL CHECK (total_amount = quantity * unit_price)
);


-- Step 2: Load the clean data from our simulated cloud bucket.
-- "--skip 1" skips the header row of the CSV file.
.import --csv --skip 1 cloud_storage_bucket/sales/2026/cleaned_sales.csv sales


-- Step 3: Run each query and save its result to a CSV file.
-- ".output <file>" sends the next result into that file.
.headers on
.mode csv

-- Question a) Total revenue for each branch
.output outputs/query_results/q_a_revenue_per_branch.csv
SELECT branch, COUNT(*) AS transactions, SUM(total_amount) AS total_revenue
FROM sales GROUP BY branch ORDER BY total_revenue DESC;

-- Question b) Total revenue for each product category
.output outputs/query_results/q_b_revenue_per_category.csv
SELECT category, COUNT(*) AS transactions, SUM(total_amount) AS total_revenue
FROM sales GROUP BY category ORDER BY total_revenue DESC;

-- Question c) The 5 products that brought in the most money
.output outputs/query_results/q_c_top5_products.csv
SELECT product, SUM(quantity) AS units_sold, SUM(total_amount) AS total_sales
FROM sales GROUP BY product ORDER BY total_sales DESC LIMIT 5;

-- Question d) The average value of a transaction
.output outputs/query_results/q_d_average_transaction_value.csv
SELECT COUNT(*) AS transactions, ROUND(AVG(total_amount), 2) AS average_transaction_value
FROM sales;

-- Send output back to the screen.
.output stdout