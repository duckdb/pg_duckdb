# Types

`pg_duckdb` supports many [data types](https://www.postgresql.org/docs/current/datatype.html) that exist in both Postgres and DuckDB. The following data types are currently supported for use in queries:

## Basic Types

- **Integer types**: `smallint`, `integer`, `bigint`, `tinyint` (from DuckDB)
- **Floating point types**: `real`, `double precision`
- **Numeric types**: `numeric`/`decimal` (with limitations, see below), `varint` (1.0.0+)
- **String types**: `text`, `varchar`, `char`, `bpchar`
- **Binary types**: `bytea`, `blob`
- **Boolean**: `boolean`
- **UUID**: `uuid`

## Date/Time Types

- **Date**: `date`
- **Time**: `time`, `timetz` (from 1.0.0+)
- **Timestamp**: `timestamp`, `timestamptz`
- **Interval**: `interval`
- **High-precision timestamps**: `timestamp_ns`, `timestamp_ms`, `timestamp_s`

## Bit String Types (1.0.0+)

- **Fixed-length bit strings**: `bit(n)` - stores exactly n bits (PostgreSQL only)
  ```sql
  -- PostgreSQL bit strings (not supported in DuckDB context)
  SELECT '101'::bit(3);
  ```

- **Variable-length bit strings**: `varbit(n)` or `bit varying(n)` - stores up to n bits (PostgreSQL only)
  ```sql
  -- PostgreSQL variable bit strings (not supported in DuckDB context)
  SELECT '10110'::varbit(10);
  SELECT '1010'::bit varying;
  ```

**Note**: Bit string types are PostgreSQL-specific and not available in DuckDB execution context. Use them only in PostgreSQL tables or queries that don't involve DuckDB.

## Complex Types

- **JSON**: `json`, `jsonb`
- **Arrays**: Single and multi-dimensional arrays for all basic types
- **Domain types**: Custom domain types based on supported base types

## Advanced DuckDB Types (1.0.0+)

> **Important**: Advanced DuckDB types (`STRUCT`, `MAP`, `UNION`, `VARINT`) require a DuckDB execution context. Use them within `duckdb.query()` function calls or in DuckDB table operations. Direct usage in PostgreSQL `CREATE TABLE` statements or regular `SELECT` queries may not work.

**Usage Patterns:**
- ✅ `SELECT * FROM duckdb.query('SELECT {''key'': ''value''} AS my_struct')`
- ✅ `CREATE TEMP TABLE foo USING duckdb AS SELECT * FROM duckdb.query('...')`
- ❌ `SELECT {'key': 'value'} AS my_struct` (PostgreSQL context)
- ❌ `CREATE TABLE foo AS SELECT {'key': 'value'}` (PostgreSQL context)

### VARINT Type

Variable-precision integers that can store arbitrarily large numbers without overflow:

```sql
-- Store very large integers (requires DuckDB execution context)
SELECT * FROM duckdb.query($$
  SELECT 123456789012345678901234567890::VARINT
$$);

-- Automatic conversion from string
SELECT * FROM duckdb.query($$
  SELECT '999999999999999999999999999999'::VARINT
$$);

-- Arithmetic operations maintain precision
SELECT * FROM duckdb.query($$
  SELECT (10::VARINT ^ 50)
$$);
```

**PostgreSQL Mapping**: `VARINT` values are converted to the PostgreSQL `NUMERIC` type for compatibility.

### STRUCT Type

Structured data with named fields, similar to JSON objects but with type safety:

```sql
-- Create STRUCT with named fields (requires DuckDB execution context)
SELECT * FROM duckdb.query($$
  SELECT {'name': 'Alice', 'age': 30, 'active': true}::STRUCT(name VARCHAR, age INTEGER, active BOOLEAN)
$$);

-- Access STRUCT fields
SELECT * FROM duckdb.query($$
  SELECT person.name, person.age 
  FROM (SELECT {'name': 'Bob', 'age': 25} AS person) t
$$);

-- STRUCT in table creation (requires DuckDB context)
CREATE TEMP TABLE employees USING duckdb AS
SELECT * FROM duckdb.query($$
  SELECT {'id': 1, 'info': {'name': 'John', 'dept': 'Engineering'}} AS employee_data
$$);
```

**PostgreSQL Mapping**: `STRUCT` values are converted to a PostgreSQL text representation for storage and display.

### MAP Type

Key-value mappings with type safety for both keys and values:

```sql
-- Create MAP with string keys and integer values (requires DuckDB execution context)
SELECT * FROM duckdb.query($$
  SELECT MAP(['key1', 'key2', 'key3'], [10, 20, 30])
$$);

-- Access MAP values
SELECT * FROM duckdb.query($$
  SELECT my_map['key1'] FROM (SELECT MAP(['key1', 'key2'], [100, 200]) AS my_map) t
$$);

-- MAP with complex types
SELECT * FROM duckdb.query($$
  SELECT MAP(['config', 'data'], [{'enabled': true}, {'count': 42}])
$$);

-- Create table with MAP column
CREATE TEMP TABLE settings USING duckdb AS
SELECT * FROM duckdb.query($$
  SELECT MAP(['theme', 'language'], ['dark', 'en']) AS user_preferences
$$);
```

**PostgreSQL Mapping**: `MAP` values are converted to a PostgreSQL text representation.

### UNION Type

Tagged union types for storing values of different types in a single column:

```sql
-- Create UNION values using built-in functions (requires DuckDB execution context)
SELECT * FROM duckdb.query($$
  SELECT union_value(string := 'hello') AS text_union,
         union_value(number := 42) AS number_union,
         union_value(flag := true) AS boolean_union
$$);

-- Extract values from UNION
SELECT * FROM duckdb.query($$
  SELECT union_extract(my_union, 'string') AS text_value
  FROM (SELECT union_value(string := 'world') AS my_union) t
$$);

-- Check UNION tag
SELECT * FROM duckdb.query($$
  SELECT union_tag(my_union) AS value_type
  FROM (SELECT union_value(number := 123) AS my_union) t
$$);
```

> **Note**: `UNION` functions have specific parameter requirements. Use the `union_value(tag_name := value)` syntax rather than separate tag and value parameters.

**PostgreSQL Mapping**: `UNION` values are converted to a PostgreSQL text representation showing the tag and value.

### ARRAY vs LIST Types

-   **DuckDB `LIST`**: A native DuckDB collection type with flexible nesting.
    -   Elements can be of any type, including other `LIST`s.
    -   Size is not fixed.
    -   Optimized for analytical operations.
-   **PostgreSQL `ARRAY`**: PostgreSQL's native array type.
    -   Rectangular (all sub-arrays at the same level must have the same size).
    -   Multi-dimensional with fixed dimensions.
    -   Integrates with PostgreSQL array operators.

```sql
-- DuckDB LIST (flexible nesting) - requires DuckDB execution context
SELECT * FROM duckdb.query($$
  SELECT [1, 2, 3] AS my_list,
         [[1, 2], [3, 4, 5]] AS nested_list  -- Different sub-list sizes OK
$$);

-- PostgreSQL ARRAY (rectangular structure) - works in PostgreSQL context
SELECT ARRAY[1, 2, 3] AS my_array,
       ARRAY[ARRAY[1, 2], ARRAY[3, 4]] AS matrix;  -- Must be rectangular

-- Both are supported and automatically converted between systems
CREATE TEMP TABLE mixed_arrays USING duckdb AS
SELECT * FROM duckdb.query($$
  SELECT [1, 2, 3] AS duckdb_list
$$)
UNION ALL
SELECT ARRAY[1, 2, 3]::TEXT AS duckdb_list;  -- Convert PostgreSQL array for compatibility
```

**Conversion Notes**:
-   DuckDB `LIST`s are converted to PostgreSQL `ARRAY`s when possible.
-   PostgreSQL `ARRAY`s are converted to DuckDB `LIST`s.
-   Conversion may fail if the data does not meet the target system's constraints.

## Known Limitations

The type support in `pg_duckdb` is not yet complete. The following are known issues that you might run into. Feel free to contribute pull requests to fix these limitations.

1.  **`ENUM` types**: Not supported yet (PR in progress).
2.  **`NUMERIC` precision limits**: The DuckDB `DECIMAL` type does not support the wide range of values that the Postgres `NUMERIC` type does. To avoid errors when converting between the two, `NUMERIC` is converted to `DOUBLE PRECISION` internally if DuckDB does not support the required precision. This may cause precision loss. You can use `VARINT` for arbitrary-precision integers or enable lossy conversion with `SET duckdb.convert_unsupported_numeric_to_double = true`.
3.  **Timestamp precision**: The DuckDB `TIMESTAMP_NS` type is truncated to microseconds when converted to the Postgres `TIMESTAMP` type, which loses precision in the output. Operations on a `TIMESTAMP_NS` value, such as sorting, grouping, or comparing, will use the full precision.
4.  **`JSONB` conversion**: `JSONB` columns are converted to `JSON` columns when reading from DuckDB because DuckDB does not have a `JSONB` type.
5.  **JSON functions**: Many Postgres `JSON` and `JSONB` functions and operators are not implemented in DuckDB. Instead, you can use DuckDB's JSON functions and operators. See the [DuckDB documentation](https://duckdb.org/docs/data/json/json_functions) for more information.
6.  **`TINYINT` conversion**: The DuckDB `TINYINT` type is converted to a `SMALLINT` type in Postgres for better compatibility.
7.  **Complex type representation**: Advanced DuckDB types (`STRUCT`, `MAP`, `UNION`) are stored as text in PostgreSQL for compatibility. While functional, this limits PostgreSQL-side operations on these types.
8.  **`ARRAY`/`LIST` conversion challenges**: Conversion between PostgreSQL multi-dimensional arrays and DuckDB nested `LIST`s can have issues because each system has different constraints:
    -   **PostgreSQL**: Allows different arrays in a column to have different dimensions (e.g., `[1]` and `[[1], [2]]`).
    -   **DuckDB**: Requires consistent nesting levels but allows different element counts (e.g., `[[1], [1, 2]]`).
    Conversion works when data meets both systems' constraints. If you encounter issues, you can fix the column metadata:
    ```sql
    -- Configure column as a 3-dimensional text array
    ALTER TABLE my_table ALTER COLUMN my_column SET DATA TYPE TEXT[][][];
    ```
9.  **Domain type constraints**: For `DOMAIN` types, constraints are validated by PostgreSQL during `INSERT` operations, not by DuckDB. During `SELECT` operations, domain types are converted to their base types for DuckDB processing.
10. **`VARINT` operations**: While `VARINT` supports arbitrary precision, some PostgreSQL operators may not work directly with the converted `NUMERIC` representation. Use a DuckDB execution context for complex `VARINT` operations.

## Special Types

`pg_duckdb` introduces a few special Postgres types. You should not create these types explicitly, and normally you do not need to know about their existence, but they may appear in error messages from Postgres. These are explained below.

### `duckdb.row`

The `duckdb.row` type is returned by functions like `read_parquet`, `read_csv`, `scan_iceberg`, etc. Depending on the arguments of these functions they can return rows with different columns and types. Postgres doesn't support such functions well at this point in time, so for now we return a custom type from them. To then be able to get the actual columns out of these rows you have to use the "square bracket indexing" syntax, similarly to how you would get field

```sql
SELECT r['id'], r['name'] FROM read_parquet('file.parquet') r WHERE r['age'] > 21;
```

Using `SELECT *` will result in the columns of this row being expanded, so your query result will never have a column that has `duckdb.row` as its type:

```sql
SELECT * FROM read_parquet('file.parquet');
```

#### Limitations in CTEs and Subqueries

Due to limitations in Postgres, there are some limitations when using a function that returns a `duckdb.row` in a CTE or subquery. The main problem is that `pg_duckdb` cannot automatically assign useful aliases to the selected columns from the row. So, while this query without a CTE/subquery returns the `r['company']` column as `company`:

```sql
SELECT r['company']
FROM duckdb.query($$ SELECT 'DuckDB Labs' company $$) r;
--    company
-- ─────────────
--  DuckDB Labs
```

The same query in a subquery or CTE will return the column simply as `r`:

```sql
WITH mycte AS (
    SELECT r['company']
    FROM duckdb.query($$ SELECT 'DuckDB Labs' company $$) r
)
SELECT * FROM mycte;
--       r
-- ─────────────
--  DuckDB Labs
```

This is easy to work around by adding an explicit alias to the column in the CTE/subquery:

```sql
WITH mycte AS (
    SELECT r['company'] AS company
    FROM duckdb.query($$ SELECT 'DuckDB Labs' company $$) r
)
SELECT * FROM mycte;
--    company
-- ─────────────
--  DuckDB Labs
```

Another limitation that can be similarly confusing is that when using `SELECT *` inside the CTE/subquery, and you want to reference a specific column outside the CTE/subquery, then you still need to use the `r['colname']` syntax instead of simply `colname`. So while this works as expected:
```sql
WITH mycte AS (
    SELECT *
    FROM duckdb.query($$ SELECT 'DuckDB Labs' company $$) r
)
SELECT * FROM mycte;
--    company
-- ─────────────
--  DuckDB Labs
```

The following query will throw an error:

```sql
WITH mycte AS (
    SELECT *
    FROM duckdb.query($$ SELECT 'DuckDB Labs' company $$) r
)
SELECT * FROM mycte WHERE company = 'DuckDB Labs';
-- ERROR:  42703: column "company" does not exist
-- LINE 5: SELECT * FROM mycte WHERE company = 'DuckDB Labs';
--                                   ^
-- HINT:  If you use DuckDB functions like read_parquet, you need to use the r['colname'] syntax to use columns. If you're already doing that, maybe you forgot to to give the function the r alias.
```

This is easy to work around by using the `r['colname']` syntax like so:

```sql
> WITH mycte AS (
    SELECT *
    FROM duckdb.query($$ SELECT 'DuckDB Labs' company $$) r
)
SELECT * FROM mycte WHERE r['company'] = 'DuckDB Labs';
--    company
-- ─────────────
--  DuckDB Labs
```

### `duckdb.unresolved_type`

The `duckdb.unresolved_type` type is a type that is used to make Postgres understand an expression for which the type is not known at query parse time. This is the type of any of the columns extracted from a `duckdb.row` using the `r['mycol']` syntax. Many operators and aggregates will return a `duckdb.unresolved_type` when one of the sides of the operator is of the type `duckdb.unresolved_type`, for instance `r['age'] + 10`.

Once the query gets executed by DuckDB the actual type will be filled in by DuckDB. So, a query result will never contain a column that has `duckdb.unresolved_type` as its type. And generally you shouldn't even realize that this type even exists.

You might get errors that say that functions or operators don't exist for the `duckdb.unresolved_type`, such as:

```txt
ERROR:  function some_func(duckdb.unresolved_type) does not exist
LINE 6:  some_func(r['somecol']) as somecol
```

In such cases a simple workaround is often to add an explicit cast to the type that the function accepts, such as `some_func(r['somecol']::text) as somecol`. If this happens for builtin Postgres functions, please open an issue on the `pg_duckdb` repository. That way we can consider adding explicit support for these functions.

### `duckdb.json`

The `duckdb.json` type is used as arguments to DuckDB JSON functions. This type exists so that these functions can take values of `json`, `jsonb` and `duckdb.unresolved_type`.
