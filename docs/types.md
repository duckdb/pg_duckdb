# Types

Able to read many [data types](https://www.postgresql.org/docs/current/datatype.html) that exist in both Postgres and DuckDB. The following data types are currently supported for use in queries: numeric

- Integer types (`integer`, `bigint`, etc.)
- Floating point types (`real`, `double precision`)
- `numeric` (might get converted to `double precision` internally see known limitations below for details)
- `text`/`varchar`/`bpchar`
- `binary`
- `timestamp`/`timstampz`/`date`
- `boolean`
- `uuid`
- `json`
- `arrays` for some of the above types

## Known limitations

The type support in `pg_duckdb` is not yet complete (and might never be). The
following are known issues that you might run into. Feel free to contribute PRs
to fix these limitations:

1. Arrays don't work yet for all of the supported types (PR in progress)
2. `enum` types are not supported (PR is progress)
2. Comparisons to literals in the queries are not supported for all data types
   yet. (Work in progress, no PR yet)
3. `jsonb` is not supported
4. DuckDB its `decimal` type doesn't support the wide range of values that Postgres its `numeric` type does. To avoid errors when converting between the two, `numeric` is converted to `double precision` internally if `DuckDB` does not support the required precision. Obviously this might cause precision loss of the values.
