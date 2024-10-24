# Compilation

To build pg_duckdb, you need:

* Postgres 15-17
* Ubuntu 22.04-24.04 or MacOS
* Standard set of build tools for building Postgres extensions
* [Build tools that are required to build DuckDB](https://duckdb.org/docs/dev/building/build_instructions)
* For full details on required dependencies you can check out our [Github Action](../.github/workflows/build_and_test.yaml).

To build and install, run:

```sh
make install
```

Add `pg_duckdb` to the `shared_preload_libraries` in your `postgresql.conf` file:

```ini
shared_preload_libraries = 'pg_duckdb'
```

Next, create the `pg_duckdb` extension:

```sql
CREATE EXTENSION pg_duckdb;
```

TODO: List instructions to build 'from scratch', including all required dependencies with a `brew`/`apt install`, on at least:

* Ubuntu 24.04
* MacOS
