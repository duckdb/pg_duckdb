# Types

Able to read many [data types](https://www.postgresql.org/docs/current/datatype.html) that exist in both Postgres and DuckDB. The following data types are currently supported for use in queries:

- Integer types (`integer`, `bigint`, etc.)
- Floating point types (`real`, `double precision`)
- `numeric` (might get converted to `double precision` internally see known limitations below for details)
- `text`/`varchar`/`bpchar`
- `bytea`/`blob`
- `timestamp`/`timstampz`/`date`/`interval`/`timestamp_ns`/`timestamp_ms`/`timestamp_s`
- `boolean`
- `uuid`
- `json`/`jsonb`
- `arrays` for all of the above types

## Known limitations

The type support in `pg_duckdb` is not yet complete (and might never be). The
following are known issues that you might run into. Feel free to contribute PRs
to fix these limitations:

1. `enum` types are not supported (PR is progress)
2. The DuckDB `decimal` type doesn't support the wide range of values that the Postgres `numeric` type does. To avoid errors when converting between the two, `numeric` is converted to `double precision` internally if `DuckDB` does not support the required precision. Obviously this might cause precision loss of the values.
3. The DuckDB `STRUCT` type is not supported
4. The DuckDB `timestamp_ns` type gets truncated to microseconds when it is converted to the Postgres `timestamp` type, which loses precision in the output. Operations on a `timestamp_ns` value, such as sorting/grouping/comparing, will use the full precision.
5. `jsonb` columns are converted to `json` columns when reading from DuckDB. This is because DuckDB does not have a `jsonb` type.
6. Many Postgres `json` and `jsonb` functions and operators are not implemented in DuckDB. Instead you can use DuckDB json functions and operators. See the [DuckDB documentation](https://duckdb.org/docs/data/json/json_functions) for more information on these functions.
7. The DuckDB `tinyint` type is converted to a `char` type in Postgres. This is because Postgres does not have a `tinyint` type. This causes it to be displayed as a hex code instead of a regular number.

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

### `duckdb.unresolved_type`

The `duckdb.unresolved_type` type is a type that is used to make Postgres understand an expression for which the type is not known at query parse time. This is the type of any of the columns extracted from a `duckdb.row` using the `r['mycol']` syntax. Many operators and aggregates will return a `duckdb.unresolved_type` when one of the sides of the operator is of the type `duckdb.unresolved_type`, for instance `r['age'] + 10`.

Once the query gets executed by DuckDB the actual type will be filled in by DuckDB. So, a query result will never contain a column that has `duckdb.unresolved_type` as its type. And generally you shouldn't even realize that this type even exists. So, if you get errors involving this type, please report an issue.

### `duckdb.json`

The `duckdb.json` type is used as arguments to DuckDB JSON functions. This type exists so that these functions can take values of `json`, `jsonb` and `duckdb.unresolved_type`.
