# Transactions in pg_duckdb

pg_duckdb supports multi-statement transactions with specific rules to ensure ACID guarantees and data consistency.

## Transaction Rules

### Allowed Operations

**Within the same transaction, you can:**

1. **Read from both PostgreSQL and DuckDB tables:**
   ```sql
   BEGIN;
   SELECT COUNT(*) FROM postgres_table;
   SELECT COUNT(*) FROM duckdb_table;
   SELECT * FROM read_parquet('s3://bucket/file.parquet');
   COMMIT;
   ```

2. **Write to PostgreSQL tables only:**
   ```sql
   BEGIN;
   INSERT INTO postgres_table SELECT * FROM another_postgres_table;
   UPDATE postgres_table SET status = 'processed';
   COMMIT;
   ```

3. **Write to DuckDB tables only:**
   ```sql
   BEGIN;
   CREATE TABLE duckdb_table_new USING duckdb AS SELECT * FROM read_parquet('s3://data.parquet');
   INSERT INTO duckdb_table_new VALUES (1, 'test');
   DROP TABLE duckdb_table_old;
   COMMIT;
   ```

4. **DuckDB DDL operations (1.0.0+):**
   ```sql
   BEGIN;
   -- Create DuckDB tables
   CREATE TABLE analytics USING duckdb AS 
     SELECT region, COUNT(*) as sales_count 
     FROM read_csv('s3://sales/*.csv') 
     GROUP BY region;
   
   -- ALTER TABLE operations on DuckDB tables
   ALTER TABLE analytics ADD COLUMN created_at TIMESTAMP DEFAULT NOW();
   ALTER TABLE analytics RENAME COLUMN sales_count TO total_sales;
   
   -- COPY operations with DuckDB tables
   COPY analytics TO 's3://output/analytics.parquet';
   COPY analytics FROM 's3://backup/analytics_restore.parquet';
   
   -- Multiple DDL operations in sequence
   CREATE TABLE temp_staging USING duckdb AS SELECT * FROM analytics WHERE total_sales > 100;
   DROP TABLE analytics;
   ALTER TABLE temp_staging RENAME TO analytics;
   COMMIT;
   ```

### Restricted Operations

**The following is NOT allowed in the same transaction:**

```sql
-- This will fail:
BEGIN;
INSERT INTO postgres_table VALUES (1, 'data');
INSERT INTO duckdb_table VALUES (2, 'more_data');  -- Error!
COMMIT;
```

**Mixed DDL operations:**
```sql
-- This will fail:
BEGIN;
CREATE TABLE postgres_table (id int);
CREATE TABLE duckdb_table USING duckdb (id int, name text);  -- Error!
COMMIT;
```

## Advanced: Unsafe Mixed Transactions

For advanced users who understand the risks, mixed transactions can be enabled:

```sql
BEGIN;
SET LOCAL duckdb.unsafe_allow_mixed_transactions TO true;
-- Now mixed operations are allowed (at your own risk)
INSERT INTO postgres_table VALUES (1, 'data');
INSERT INTO duckdb_table VALUES (2, 'more_data');
COMMIT;
```

### Warning: Data Consistency Risks

**This setting is dangerous** and can lead to:
- **Partial commits**: DuckDB operations might succeed while PostgreSQL operations fail
- **Data loss**: Operations might be committed in one system but not the other
- **Inconsistent state**: Your application might see inconsistent data

**Example of potential data loss:**
```sql
BEGIN;
SET LOCAL duckdb.unsafe_allow_mixed_transactions TO true;
-- This could delete the DuckDB table but fail to create the PostgreSQL table
CREATE TABLE pg_backup AS SELECT * FROM duckdb_table;  -- Might fail
DROP TABLE duckdb_table;                               -- Might succeed
COMMIT;
-- Result: Data lost if pg_backup creation failed!
```

## New Features in 1.0.0

### Enhanced DDL Support

DuckDB tables now support comprehensive DDL operations within transactions:

```sql
BEGIN;
-- Create table with complex types
CREATE TABLE user_profiles USING duckdb AS
SELECT 
    user_id,
    {'name': first_name, 'email': email} AS profile,
    ARRAY[interest1, interest2, interest3] AS interests,
    MAP(['last_login', 'signup_date'], [last_seen, created_at]) AS timestamps
FROM user_data;

-- Add and modify columns
ALTER TABLE user_profiles ADD COLUMN status VARCHAR DEFAULT 'active';
ALTER TABLE user_profiles RENAME COLUMN profile TO user_info;
ALTER TABLE user_profiles DROP COLUMN interests;
COMMIT;
```

### COPY Operations with DuckDB Tables

Copy data to and from DuckDB tables using various formats:

```sql
BEGIN;
-- Create table from external data
CREATE TABLE sales_data USING duckdb AS
SELECT * FROM read_parquet('s3://data-lake/sales/**/*.parquet');

-- Export to different formats
COPY sales_data TO 's3://exports/sales_summary.parquet';
COPY (SELECT region, SUM(amount) FROM sales_data GROUP BY region) 
TO 's3://exports/regional_summary.csv' (FORMAT CSV, HEADER);

-- Backup and restore operations
COPY sales_data TO '/backup/sales_backup.parquet';
COMMIT;

-- Later: restore from backup
BEGIN;
CREATE TABLE sales_restored USING duckdb;
COPY sales_restored FROM '/backup/sales_backup.parquet';
COMMIT;
```

### EXPLAIN Support with JSON Format

Analyze query execution plans with enhanced EXPLAIN capabilities:

```sql
-- Get execution plan in JSON format
EXPLAIN (FORMAT JSON) 
SELECT r['customer_id'], COUNT(*) as order_count
FROM read_parquet('s3://orders/*.parquet') r
GROUP BY r['customer_id'];

-- Analyze complex joins
EXPLAIN (FORMAT JSON)
SELECT c.name, SUM(o.amount) as total
FROM customers c
JOIN (SELECT * FROM read_csv('s3://orders.csv')) o ON c.id = o.customer_id
GROUP BY c.name;
```

## Best Practices

1. **Separate transactions**: Use separate transactions for PostgreSQL and DuckDB writes
   ```sql
   -- Good: Separate transactions
   BEGIN;
   INSERT INTO postgres_table SELECT * FROM source_table;
   COMMIT;
   
   BEGIN;
   INSERT INTO duckdb_table SELECT * FROM read_parquet('s3://data.parquet');
   COMMIT;
   ```

2. **Use CTEs for complex operations:**
   ```sql
   -- Good: Single read transaction with CTE
   BEGIN;
   WITH combined_data AS (
       SELECT * FROM postgres_table
       UNION ALL
       SELECT * FROM read_parquet('s3://external.parquet')
   )
   SELECT region, SUM(amount) 
   FROM combined_data 
   GROUP BY region;
   COMMIT;
   ```

3. **Leverage DuckDB for ETL:**
   ```sql
   -- Good: DuckDB-only ETL transaction
   BEGIN;
   CREATE TEMP TABLE processed_data USING duckdb AS
   SELECT 
       customer_id,
       SUM(amount) as total_spend,
       COUNT(*) as order_count
   FROM read_parquet('s3://raw-data/*.parquet')
   WHERE date >= '2024-01-01'
   GROUP BY customer_id;
   
   -- Export results
   COPY processed_data TO 's3://processed/customer_summary.parquet';
   COMMIT;
   ```

## Transaction Isolation

- **PostgreSQL tables**: Follow standard PostgreSQL isolation levels
- **DuckDB tables**: Use DuckDB's transaction semantics
- **Mixed reads**: Snapshot isolation applied per system

## Troubleshooting

**Common error messages:**

- `"cannot write to both DuckDB and PostgreSQL in the same transaction"`: Separate your writes into different transactions
- `"DDL operations cannot be mixed"`: Avoid mixing DDL operations across systems

**Performance tips:**
- Use `COPY` for bulk data movement between systems
- Leverage temporary tables for intermediate results
- Consider materializing complex joins in DuckDB tables
