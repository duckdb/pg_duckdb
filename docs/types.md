# Types

pg_duckdb supports many [data types](https://www.postgresql.org/docs/current/datatype.html) that exist in both Postgres and DuckDB. The following data types are currently supported for use in queries:

## Basic Types

- **Integer types**: `smallint`, `integer`, `bigint`, `tinyint` (DuckDB)
- **Floating point types**: `real`, `double precision`
- **Numeric types**: `numeric`/`decimal` (with limitations, see below)
- **String types**: `text`, `varchar`, `char`, `bpchar`
- **Binary types**: `bytea`, `blob`
- **Boolean**: `boolean`
- **UUID**: `uuid`

## Date/Time Types

- **Date**: `date`
- **Time**: `time`, `timetz`
- **Timestamp**: `timestamp`, `timestamptz`
- **Interval**: `interval`
- **High-precision timestamps**: `timestamp_ns`, `timestamp_ms`, `timestamp_s`

## Complex Types

- **JSON**: `json`, `jsonb`
- **Arrays**: Single and multi-dimensional arrays for all basic types
- **Bit strings**: `bit`, `varbit` (fixed and variable-length bit arrays)
- **Domain types**: Custom domain types based on supported base types

## Advanced DuckDB Types (1.0.0+)

- **STRUCT**: Structured data with named fields
  ```sql
  SELECT {'name': 'Alice', 'age': 30}::STRUCT(name VARCHAR, age INTEGER);
  ```

- **MAP**: Key-value pairs
  ```sql
  SELECT MAP(['key1', 'key2'], ['value1', 'value2']);
  ```

- **UNION**: Tagged union types for variant data
  ```sql
  SELECT UNION_VALUE(tag := 'string', value := 'hello');
  ```

- **LIST**: DuckDB's native list type (converted to PostgreSQL arrays)

## Known limitations

The type support in `pg_duckdb` is not yet complete (and might never be). The
following are known issues that you might run into. Feel free to contribute PRs
to fix these limitations:

1. `enum` types are not supported (PR in progress)
2. The DuckDB `decimal` type doesn't support the wide range of values that the Postgres `numeric` type does. To avoid errors when converting between the two, `numeric` is converted to `double precision` internally if `DuckDB` does not support the required precision. Obviously this might cause precision loss of the values.
3. The DuckDB `timestamp_ns` type gets truncated to microseconds when it is converted to the Postgres `timestamp` type, which loses precision in the output. Operations on a `timestamp_ns` value, such as sorting/grouping/comparing, will use the full precision.
4. `jsonb` columns are converted to `json` columns when reading from DuckDB. This is because DuckDB does not have a `jsonb` type.
5. Many Postgres `json` and `jsonb` functions and operators are not implemented in DuckDB. Instead you can use DuckDB json functions and operators. See the [DuckDB documentation](https://duckdb.org/docs/data/json/json_functions) for more information on these functions.
6. The DuckDB `tinyint` type is converted to a `smallint` type in Postgres (as of recent versions) for better compatibility.
7. Conversion between in Postgres multi-dimensional arrays and DuckDB nested `LIST`s in DuckDB can run into various problems, because neither database supports the thing that the other supports exactly. Specifically in Postgres it's allowed for different arrays in a column to have a different number of dimensions, e.g. `[1]` and `[[1], [2]]` can both occur in the same column. In DuckDB that's not allowed, i.e. the amount of nesting should always be the same. On the other hand, in DuckDB it's valid for different lists at the same nest-level to contain a different number of elements, e.g. `[[1], [1, 2]]`. This is not allowed in Postgres. So conversion between these types is only possible when the arrays follow the subset. Another possible problem that you can run into is that pg_duckdb uses the Postgres column metadata to determine the number of dimensions that an array has. Since Postgres doesn't complain when you add arrays of different dimensions, it's possible that the number of dimensions in the column metadata does not match the actual number of dimensions. To solve this you need to alter the column type:
    ```sql
    -- This configures the column to be a 3-dimensional array of text
    ALTER TABLE s ALTER COLUMN a SET DATA TYPE text[][][];
    ```
8. For the `domain` type, during INSERT operations, the domain constraints are validated by PostgreSQL rather than DuckDB. When executing SELECT operations on `domain` type fields, the type is converted to its corresponding base type for DuckDB processing.

## Special types

pg_duckdb introduces a few special Postgres types. You shouldn't create these types explicitly and normally you don't need to know about their existence, but they might show up in error messages from Postgres. These are explained below:

### `duckdb.row`

The `duckdb.row` type is returned by functions like `read_parquet`, `read_csv`, `scan_iceberg`, etc. Depending on the arguments of these functions they can return rows with different columns and types. Postgres doesn't support such functions well at this point in time, so for now we return a custom type from them. To then be able to get the actual columns out of these rows you have to use the "square bracket indexing" syntax, similarly to how you would get field

```sql
SELECT r['id'], r['name'] FROM read_parquet('file.parquet') r WHERE r['age'] > 21;
```

Using `SELECT *` will result in the columns of this row being expanded, so your query result will never have a column that has `duckdb.row` as its type:

```sql
SELECT * FROM read_parquet('file.parquet');
```

#### Limitations in CTEs and Subqueries returning

Due to limitations in Postgres, there are some limitations when using a function that returns a `duckdb.row` in a CTE or subquery. The main problem is that pg_duckdb cannot automatically assign useful aliases to the selected columns from the row. So while this query without a CTE/subquery returns the `r[company]` column as `company`:

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
