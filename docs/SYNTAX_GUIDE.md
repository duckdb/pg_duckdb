# pg_duckdb Syntax Guide

This guide provides a quick reference for the most common SQL patterns used with `pg_duckdb`.

## Create a table

```sql
-- This is a standard PostgreSQL table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    product_name TEXT,
    amount NUMERIC,
    order_date DATE
);

INSERT INTO orders (product_name, amount, order_date)
VALUES ('Laptop', 1200.00, '2024-07-01'),
       ('Keyboard', 75.50, '2024-07-01'),
       ('Mouse', 25.00, '2024-07-02');
```


---

## 1. Querying standard PostgreSQL tables

For analytical queries on your existing PostgreSQL tables, use **standard SQL**. No special syntax is needed. `pg_duckdb` automatically accelerates these queries.

```sql
-- Standard SELECT on a PostgreSQL table
SELECT
    category,
    AVG(price) as avg_price,
    COUNT(*) as item_count
FROM
    products -- This is a regular PostgreSQL table
GROUP BY
    category
ORDER BY
    avg_price DESC;
```

---

## 2. Querying external files (Parquet, CSV, etc.)

To query files from a data lake (e.g., S3, local storage), use the `read_*` functions. You must use the `r['column_name']` syntax to access columns.

```sql
-- Query a single Parquet file
SELECT
    r['product_id'],
    r['review_text']
FROM
    read_parquet('s3://my-bucket/reviews.parquet') r -- 'r' is a required alias
LIMIT 100;

-- Query multiple CSV files using a glob pattern
SELECT
    r['timestamp'],
    r['event_type'],
    COUNT(*) as event_count
FROM
    read_csv('s3://my-datalake/logs/2024-*.csv') r
GROUP BY
    r['timestamp'],
    r['event_type'];
```

---

## 3. Hybrid queries (joining PostgreSQL and external data)

You can seamlessly join PostgreSQL tables with external data sources in a single query.

```sql
-- Join a local PostgreSQL 'customers' table with a remote Parquet file of 'orders'
SELECT
    c.customer_name,
    c.signup_date,
    SUM(r['order_total']) AS total_spent
FROM
    customers c -- This is a PostgreSQL table
JOIN
    read_parquet('s3://my-bucket/orders/*.parquet') r ON c.customer_id = r['customer_id']
WHERE
    c.status = 'active'
GROUP BY
    c.customer_name,
    c.signup_date
ORDER BY
    total_spent DESC;
```

---

## 4. Creating DuckDB-backed tables

To persist the results of an analytical query, you can create a new table that uses DuckDB's fast columnar storage. Use the `USING duckdb` clause.

```sql
-- Create a new table 'sales_summary' with DuckDB storage
CREATE TABLE sales_summary USING duckdb AS
SELECT
    r['region'],
    r['product_category'],
    SUM(r['sales_amount']) AS total_sales
FROM
    read_parquet('s3://my-datalake/sales_data/year=2024/**/*.parquet') r
GROUP BY
    r['region'],
    r['product_category'];

-- Query the newly created analytical table
SELECT * FROM sales_summary WHERE region = 'North America';
```

---

## 5. When to use `duckdb.query()`

The `duckdb.query()` function is an **advanced feature** and is **not needed for most queries**. You should only use it when you need to run a query that uses DuckDB-specific syntax that is not valid in standard PostgreSQL.

**Example (Using DuckDB's `PIVOT` statement):**

```sql
-- This query uses DuckDB's PIVOT syntax, so it must be wrapped in duckdb.query()
SELECT * FROM duckdb.query($$
    PIVOT sales_summary
    ON product_category
    USING SUM(total_sales)
    GROUP BY region;
$$);
```

For all standard analytical queries, prefer the direct SQL syntax shown in the sections above.
