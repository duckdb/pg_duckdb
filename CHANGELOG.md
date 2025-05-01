# 1.0.0 (2025-05-14)

## Added

- Add support for statically compiling the duckdb library into the pg_duckdb extension. (#618)
- Add support for `DOMAIN`, `VARINT`, `TIME`, `TIMETZ`, `BIT`, `VARBIT`, `UNION`, `MAP`, `STRUCT` types. (#532, #626, #627, #628, #636, #678, #689, #669)
- Add support for installing community extensions. (#647)
- Add support for DDL on DuckDB tables in transactions. (#632)
- Make `duckdb.unresolved_type` support `min`, `date_trunc`, `length`, `regexp_replace`, `LIKE`, `ILIKE`, `SIMILAR TO`. (#643)
- Add cast from `duckdb.unresolved_type` to `bytea`. (#643)
- Add support for the DuckDB functions `strftime`, `strptime`, `epoch`, `epoch_ms`, `epoch_us`, `epoch_ns`, `time_bucket`. (#643)
- Add support for using MotherDuck in multiple Postgres databases. (#544, #545)
- Add ALTER TABLE support for DuckDB tables. (#652)
- Add support to `COPY ... TO` and `COPY ... FROM` for DuckDB tables. (#665)
- Add support for `EXPLAIN (FORMAT JSON)` for DuckDB queries. (#654)
- Add support for single dimension `ARRAY` types from DuckDB, before only `LIST` was supported. (#655)
- Add support for `TABLESAMPLE`. (#559)
- Add `duckdb.extension_directory`, `duckdb.temporary_directory` and `duckdb.max_temporary_directory_size` settings. (#704)
- Add source locations to error messages. (#758)

## Changed

- Update to DuckDB 1.3.0. (#754)
- Change the way MotherDuck is configured. It's not done anymore through the Postgres configuration file. Instead, you should now enable MotherDuck using `CALL duckdb.enable_motherduck(...)` or equivalent `CREATE SERVER` and `CREATE USER MAPPING` commands. (#668)
- Change the way secrets are added to DuckDB. You'll need to recreate your secrets using the new method `duckdb.create_simple_secret` or `duckdb.create_azure_secret` functions. Internally secrets are now stored `SERVER` and `USER MAPPING` for the `duckdb` foreign data wrapper. (#697)
- Disallow DuckDB execution inside functions. This feature had issues and is intended to be re-enabled in a future release. (#764)
- Don't convert Postgres NUMERICs with a precision that's unsupported in DuckDB to double by default. Instead it will throw an error. If you want the lossy conversion to DOUBLE to happen, you can enable `duckdb.convert_unsupported_numeric_to_double`. (#795)
- Remove custom HTTP caching logic. (#644)
- When creating a table in a `ddb$` schema that table now uses the `duckdb` table access method by default. (#650)
- Do not allow creating non-`duckdb` tables in a `ddb$` schema. (#650)
- When creating MotherDuck tables from Postgres, automatically make them be created by the table creation. Before you had to set the ROLE manually before issuing the CREATE TABLE command. (#650)
- Add automated tests for MotherDuck integration. (#649)
- Sync the Postgres timezone to DuckDB when initializing the DuckDB connection. This makes some date parsing/formatting behave better. (#643)
- Support `FORMAT JSON` for `COPY` commands. (#665)
- Force `COPY` to use DuckDB execution when using `duckdb.force_execution`. (#665)
- Automatically use DuckDB execution for COPY when file extensions are used for filetypes that DuckDB understands (`.parquet`, `.json`, `.ndjson`, `jsonl`, `.gz`, `.zst`). (#665)
- Return `TEXT` columns instead of `VARCHAR` columns when using DuckDB execution. (#583)

## Fixed

- Fix possible crash when querying two Postgres tables in the same query. (#604)
- Fix crash when loading the `postgres` extension for DuckDB  (a.k.a. postgres_scanner) into pg_duckdb (#607)
- Do not set the `max_memory` in Postgres if `duckdb.max_memory`/`duckdb.memory_limit` is set to the empty string. (#614)
- Handle PG columns with arrays with 0 dimensions correctly. We now assume such an array has a single dimension. (#616)
- Fix valgrind issue in `DatumToString`. (#639)
- Fix read of uninitialized memory when using DuckDB functions. (#638)
- Fix escaping of MotherDuck schema names when syncing them. (#650)
- Fix crash that could happen when EXPLAINing a prepared statement in certain cases. (#660)
- Fix memory leak that could happen on query failure. (#663)
- Add boundary checks when converting DuckDB date/timestamps to PG date/timestamps. DuckDB and Postgres don't support the exact same range of dates/timestamps, so now pg_duckdb only supports the intersection of these two ranges. (#653)
- Fail nicely when syncing MotherDuck tables result in too long names being synced. (#680) TODO: FIX FOR TABLES CURRENTLY ONLY DONE FOR SCHEMAS
- Disallow installing `pg_duckdb` in databases with different encoding than `UTF8`. (#703)
- Fix crashes or data corruption that could occur when using `CREATE TABLE AS` and materialized views if DuckDB execution and Postgres execution did not agree on the types that a query would return. (#706)
- Fix various issues when using functions that returned `duckdb.row` (like `read_csv` & `read_parquet`) in a CTE. (#718)
- Fix a crash when using a `CREATE TABLE AS` statement in a `plpgqsl` function (#735)
- Throw error when trying to change DuckDB settings after the DuckDB connection has been initialized. (#743)
- Fix crash for CREATE TABLE ... AS EXECUTE (#757)
- Handle issues when DuckDB query would return different types between planning and execution phase. (#759)
- Disallow DuckDB tables as a partition. This wasn't supported, and would fail in weird ways when attempted. Now a clear error is thrown. (#778)
- Fix memory leak when reading LIST/JSON/JSONB columns from Postgres tables. (#784)


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
- Allow executing `duckdb.raw_query`, `duckdb.cache_info`, `duckdb.cache_delete` and `duckdb.recycle_ddb` as non-superusers. ([#572])
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
