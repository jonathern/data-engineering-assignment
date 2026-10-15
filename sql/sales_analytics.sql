-- 
-- This file handles Part C of the assignment. It creates the "sales" table
-- and holds the SQL queries that answer the business questions.
--
-- Note: each section starts with a line like "-- name: q_a_revenue_per_branch".
-- run_analytics.py and the 02 notebook use these lines to find each query
-- and save its result as a CSV file with the same name. Please don't change
-- or remove them.
-- 


-- name: create_sales_table
-- Step 1: Create the sales table.
-- We delete the old table first, so running the pipeline again starts fresh
-- and never loads the same data twice.
DROP TABLE IF EXISTS sales;

CREATE TABLE sales (
    transaction_id   TEXT    PRIMARY KEY,                 -- each sale appears only once
    transaction_date TEXT    NOT NULL,                    -- stored as YYYY-MM-DD
    branch           TEXT    NOT NULL,
    product          TEXT    NOT NULL,
    category         TEXT    NOT NULL,
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    unit_price       INTEGER NOT NULL CHECK (unit_price > 0),   -- in Uganda shillings
    total_amount     INTEGER NOT NULL CHECK (total_amount = quantity * unit_price)
);
-- The CHECK rules are a second safety net: the database will refuse any row
-- with a zero or negative quantity or price, or a wrong total.

-- Indexes make searching and grouping by these columns faster.
CREATE INDEX idx_sales_branch   ON sales (branch);
CREATE INDEX idx_sales_category ON sales (category);
CREATE INDEX idx_sales_date     ON sales (transaction_date);


-- Step 2: Load cleaned_sales.csv into the table.
-- This is done by run_analytics.py (or by load_with_sqlite_cli.sql if you
-- are using the sqlite3 command line instead).


-- name: row_count_check
-- Quick check that everything loaded: this should show 102 rows,
-- the same number as in cleaned_sales.csv.
SELECT COUNT(*) AS rows_loaded FROM sales;


-- name: q_a_revenue_per_branch
-- Question a) What is the total revenue for each branch?
-- We also show each branch's share of total revenue as a percentage.
SELECT
    branch,
    COUNT(*)                                   AS transactions,
    SUM(quantity)                              AS units_sold,
    SUM(total_amount)                          AS total_revenue,
    ROUND(100.0 * SUM(total_amount)
          / (SELECT SUM(total_amount) FROM sales), 2) AS revenue_share_pct
FROM sales
GROUP BY branch
ORDER BY total_revenue DESC;


-- name: q_b_revenue_per_category
-- Question b) What is the total revenue for each product category?
SELECT
    category,
    COUNT(*)                                   AS transactions,
    SUM(quantity)                              AS units_sold,
    SUM(total_amount)                          AS total_revenue,
    ROUND(100.0 * SUM(total_amount)
          / (SELECT SUM(total_amount) FROM sales), 2) AS revenue_share_pct
FROM sales
GROUP BY category
ORDER BY total_revenue DESC;


-- name: q_c_top5_products
-- Question c) Which 5 products brought in the most money?
SELECT
    product,
    category,
    SUM(quantity)       AS units_sold,
    SUM(total_amount)   AS total_sales
FROM sales
GROUP BY product, category
ORDER BY total_sales DESC
LIMIT 5;


-- name: q_d_average_transaction_value
-- Question d) How much is an average transaction worth?
-- We also show the smallest and largest transactions for context.
SELECT
    COUNT(*)                     AS transactions,
    SUM(total_amount)            AS total_revenue,
    ROUND(AVG(total_amount), 2)  AS average_transaction_value,
    MIN(total_amount)            AS smallest_transaction,
    MAX(total_amount)            AS largest_transaction
FROM sales;


-- name: extra_monthly_revenue_by_branch
-- Extra question (not required): how did each branch's revenue change
-- from month to month? Note that April only has data up to the 14th.
SELECT
    strftime('%Y-%m', transaction_date) AS month,
    branch,
    COUNT(*)                            AS transactions,
    SUM(total_amount)                   AS total_revenue
FROM sales
GROUP BY month, branch
ORDER BY month, branch;