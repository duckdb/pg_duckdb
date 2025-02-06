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
2. DuckDB its `decimal` type doesn't support the wide range of values that Postgres its `numeric` type does. To avoid errors when converting between the two, `numeric` is converted to `double precision` internally if `DuckDB` does not support the required precision. Obviously this might cause precision loss of the values.
3. DuckDB its `STRUCT` type is not supported
4. DuckDB its `timestamp_ns` type gets truncated to microseconds when its converted to Postgres its `timestamp` type. So this loses precision in the output. Operations on a `timestamp_ns` value, such as sorting/grouping/comparing, will use the full precision though.
5. `jsonb` columns are converted to `json` columns when reading from DuckDB. This is because DuckDB does not have a `jsonb` type.
6. Many of Postgres its `json` and `jsonb` functions and operators are not implemented in DuckDB. Instead you can use DuckDB its json functions and operators. See the [DuckDB documentation](https://duckdb.org/docs/data/json/json_functions) for more information on these functions.
7. DuckDB its `tinyint` type is converted to a `char` type in Postgres. This is because Postgres does not have a `tinyint` type. This does cause it to be displayed as a hex code instead of a regular number.
