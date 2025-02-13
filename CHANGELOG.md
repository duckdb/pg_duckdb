# 0.3.1 (2025-02-13)

## Fixed

- Fixed CI so docker images are built and pushed to Docker Hub for tags. ([#589])

[#589]: https://github.com/duckdb/pg_duckdb/pull/589

# 0.3.0 (2025-02-13)

## Added

- Support using Postgres indexes and reading from partitioned tables. ([#477])
- The `AS (id bigint, name text)` syntax is no longer supported when using `read_parquet`, `iceberg_scan`, etc. The new syntax is as follows: ([#531])

  ```sql
  SELECT * FROM read_parquet('file.parquet');
  SELECT r['id'], r['name'] FROM read_parquet('file.parquet') r WHERE r['age'] > 21;
  ```

- Add a `duckdb.query` function which allows using DuckDB query syntax in Postgres. ([#531])
- Support the `approx_count_distinct` DuckDB aggregate. ([#499])
- Support the `bytea` (aka blob), `uhugeint`,`jsonb`, `timestamp_ns`, `timestamp_ms`, `timestamp_s` & `interval` types. ([#511], [#525], [#513], [#534], [(#573)])
- Support DuckDB [json functions and aggregates](https://duckdb.org/docs/data/json/json_functions.html). ([#546])
- Add support for the `duckdb.allow_community_extensions` setting.
- We have an official logo! 🎉 ([#575])

## Changed

- Update to DuckDB 1.2.0. ([#548])
- Allow executing `duckdb.raw_query`, `duckdb.cache_info`, `duckdb.cache_delete` and `duckdb.recycle_db` as non-superusers. ([#572])
- Only sync MotherDuck catalogs when there is DuckDB query activity. ([#582])

## Fixed

- Correctly parse parameter lists in `COPY` commands. This allows using `PARTITION_BY` as one of the `COPY` options. ([#465])
- Correctly read cache metadata for files larger than 4GB. ([#494])
- Fix bug in parameter handling for prepared statements and PL/pgSQL functions. ([#491])
- Fix comparisons and operators on the `timestamp with timezone` field by enabling DuckDB its `icu` extension by default. ([#512])
- Allow using `read_parquet` functions when not using superuser privileges. ([#550])
- Fix some case insensitivity issues when reading from Postgres tables. ([#563])
- Fix case where cancel requests (e.g. triggered by pressing Ctrl+C in `psql`) would be ignored ([#548], [#584], [#587])

[#477]: https://github.com/duckdb/pg_duckdb/pull/477
[#531]: https://github.com/duckdb/pg_duckdb/pull/531
[#499]: https://github.com/duckdb/pg_duckdb/pull/499
[#511]: https://github.com/duckdb/pg_duckdb/pull/511
[#525]: https://github.com/duckdb/pg_duckdb/pull/525
[#513]: https://github.com/duckdb/pg_duckdb/pull/513
[#534]: https://github.com/duckdb/pg_duckdb/pull/534
[#573]: https://github.com/duckdb/pg_duckdb/pull/573
[#546]: https://github.com/duckdb/pg_duckdb/pull/546
[#575]: https://github.com/duckdb/pg_duckdb/pull/575
[#548]: https://github.com/duckdb/pg_duckdb/pull/548
[#572]: https://github.com/duckdb/pg_duckdb/pull/572
[#582]: https://github.com/duckdb/pg_duckdb/pull/582
[#465]: https://github.com/duckdb/pg_duckdb/pull/465
[#494]: https://github.com/duckdb/pg_duckdb/pull/494
[#491]: https://github.com/duckdb/pg_duckdb/pull/491
[#512]: https://github.com/duckdb/pg_duckdb/pull/512
[#550]: https://github.com/duckdb/pg_duckdb/pull/550
[#563]: https://github.com/duckdb/pg_duckdb/pull/563
[#584]: https://github.com/duckdb/pg_duckdb/pull/584
[#587]: https://github.com/duckdb/pg_duckdb/pull/587

# 0.2.0 (2024-12-10)

## Added

- Support for reading Delta Lake storage using the `duckdb.delta_scan(...)` function. ([#403])
- Support for reading JSON using the `duckdb.read_json(...)` function. ([#405])
- Support for multi-statement transactions. ([#433])
- Support reading from Azure Blob storage. ([#478])
- Support many more array types, such as `float` , `numeric` and `uuid` arrays. ([#282])
- Support for PostgreSQL 14. ([#397])
- Manage cached files using the `duckdb.cache_info()` and `duckdb.cache_delete()` functions. ([#434])
- Add `scope` column to `duckdb.secrets` table. ([#461])
- Allow configuring the default MotherDuck database using the `duckdb.motherduck_default_database` setting. ([#470])
- Automatically install and load known DuckDB extensions when queries use them. So, `duckdb.install_extension()` is usually not necessary anymore. ([#484])

## Changed

- Improve performance of heap reading. ([#366])
- Bump DuckDB version to 1.1.3. ([#400])

## Fixed

- Throw a clear error when reading partitioned tables (reading from partitioned tables is not supported yet). ([#412])
- Fixed crash when using `CREATE SCHEMA AUTHORIZATION`. ([#423])
- Fix queries inserting into DuckDB tables with `DEFAULT` values. ([#448])
- Fixed assertion failure involving recursive CTEs. ([#436])
- Only allow setting `duckdb.motherduck_postgres_database` in `postgresql.conf`. ([#476])
- Much better separation between C and C++ code, to avoid memory leaks and crashes (many PRs).

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
[#484]: https://github.com/duckdb/pg_duckdb/pull/484

# 0.1.0 (2024-10-24)

Initial release.
