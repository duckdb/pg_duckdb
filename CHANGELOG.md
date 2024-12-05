# 0.2.0 (2024-11-10)

## Added

- Support for reading Delta Lake storage using the `duckdb.delta_scan(...)` function. [#403]
- Support for reading JSON using the `duckdb.read_json(...)` function. [#405]
- Support for multi-statement transactions. [#433]
- Support reading from Azure Blob storage. [#478]
- Support many more array types, such as `float` , `numeric` and `uuid` arrays. [#282]
- Support for PostgreSQL 14. [#397]
- Manage cached files using the `duckdb.cache_info()` and `duckdb.cache_delete()` functions. [#434]
- Add `scope` column to `duckdb.secrets` table. [#461]
- Allow configuring the default MotherDuck database. [#470]

## Changed

- Improve performance of heap reading. [#366]
- Bump DuckDB version to 1.1.3. [#400]

## Fixed

- Throw a clear error when reading partitioned tables (reading from partitioned tables is not supported yet). [#412]
- Fixed crash when using `CREATE SCHEMA AUTHORIZATION`. [#423]
- Fix queries inserting into DuckDB tables with `DEFAULT` values. [#448]
- Fixed assertion failure involving recursive CTEs. [#436]
- Only allow setting `duckdb.motherduck_postgres_database` in `postgresql.conf`. [#476]

[#403]: https://github.com/duckdb/pg_duckdb/pull/403
[#405]: https://github.com/duckdb/pg_duckdb/pull/405
[#433]: https://github.com/duckdb/pg_duckdb/pull/433
[#478]: https://github.com/duckdb/pg_duckdb/pull/478
[#282]: https://github.com/duckdb/pg_duckdb/pull/282
[#397]: https://github.com/duckdb/pg_duckdb/pull/397
[#434]: https://github.com/duckdb/pg_duckdb/pull/434
[#461]: https://github.com/duckdb/pg_duckdb/pull/461
[#470]: https://github.com/duckdb/pg_duckdb/pull/470
[#366]: https://github.com/duckdb/pg_duckdb/pull/366
[#400]: https://github.com/duckdb/pg_duckdb/pull/400
[#412]: https://github.com/duckdb/pg_duckdb/pull/412
[#423]: https://github.com/duckdb/pg_duckdb/pull/423
[#448]: https://github.com/duckdb/pg_duckdb/pull/448
[#436]: https://github.com/duckdb/pg_duckdb/pull/436
[#476]: https://github.com/duckdb/pg_duckdb/pull/476

# 0.1.0 (2024-10-24)

Initial release.
