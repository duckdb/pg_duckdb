# pg_duckdb Settings

Many of these settings are simply used to configure specific [DuckDB settings](https://duckdb.org/docs/configuration/overview.html). If there's a setting from DuckDB that you'd like to see added, please open an issue/PR.

### `duckdb.force_execution`

Force queries to use DuckDB execution. This is only necessary when accessing only Postgres tables in a query. As soon as you use a DuckDB-only features, then DuckDB execution will be used automatically. DuckDB-only features are reading from DuckDB/MotherDuck tables, using DuckDB functions (like `read_parquet`), or `COPY` to remote storage (`s3://` etc).

Default: `false`

Access: General

## MotherDuck

MotherDuck support is optional, and can be enabled an configured using these settings. Check out our [MotherDuck documentation](motherduck.md) for more information.

### `duckdb.motherduck_enabled`

If MotherDuck support should be enabled. `auto` means enabled if the `duckdb.motherduck_token` is set.

Default: `MotherDuckEnabled::MOTHERDUCK_AUTO`

Access: Needs to be in the `postgresql.conf` file and requires a restart

### `duckdb.motherduck_token`

The token to use for MotherDuck

Default: `""`

Access: Needs to be in the `postgresql.conf` file and requires a restart

### `duckdb.motherduck_postgres_database`

Which database to enable MotherDuck support in

Default: `"postgres"`

Access: Needs to be in the `postgresql.conf` file and requires a restart

### `duckdb.motherduck_default_database`

Which MotherDuck database to use as the default database, i.e. the one that
gets merged with Postgres schemas instead of getting dedicated `ddb$` prefixed
schemas. The empty string means that pg_duckdb should use the default database
set by MotherDuck, which is currently always `my_db`.

Default: `""`

Access: Needs to be in the `postgresql.conf` file and requires a restart

## Security

### `duckdb.postgres_role`

Which Postgres role should be allowed to use DuckDB execution, use the secrets and create MotherDuck tables. Defaults to superusers only. If this is configured, but the role does not exist when running `CREATE EXTENSION pg_duckdb`, it will be created automatically. This role will have access to DuckDB secrets and data in MotherDuck (tables, secrets, etc).

Default: `""`

Access: Needs to be in the `postgresql.conf` file and requires a restart

### `duckdb.disabled_filesystems`

Disable specific file systems preventing access. This setting only applies to non-superusers. Superusers can always access all file systems. Unless you completely trust the user in `duckdb.posgres_role`, it is recommended to disable `LocalFileSystem`. Otherwise they can trivially read and write any file on the machine that the Postgres process can.

Default: `"LocalFileSystem"`

Access: Superuser-only

### `duckdb.autoinstall_known_extensions`

Whether known extensions are allowed to be automatically installed when a DuckDB query depends on them.

Default: `true`

Access: Superuser-only

### `duckdb.autoload_known_extensions`

Whether known extensions are allowed to be automatically loaded when a DuckDB query depends on them.

Default: `true`

### `duckdb.allow_community_extensions`

Disable installing community extensions.

Default: `false`

Access: Superuser-only

### `duckdb.enable_external_access` (experimental)

Allow the DuckDB to access external access (e.g., HTTP, S3, etc.). This setting is not tested very well yet and disabling it may break unintended `pg_duckdb` functionality.

Default: `true`

Access: Superuser-only

## Resource management

Since any connection that uses DuckDB will have its own DuckDB instance, these settings are per-connection. When using `pg_duckdb` in many concurrent connections it can be a good idea to set some of these more conservatively than their defaults.

### `duckdb.max_memory` / `duckdb.memory_limit`

The maximum memory DuckDB can use within a single Postgres connection. This is somewhat comparable to Postgres its `work_mem` setting.

Default: `"4GB"`

Access: Superuser-only

### `duckdb.threads` / `duckdb.worker_threads`

Maximum number of DuckDB threads per Postgres connection. `-1` means to use DuckDB its default, which is the number of CPU cores on the machine.

Default: `-1`

Access: Superuser-only

### `duckdb.max_workers_per_postgres_scan`

Maximum number of PostgreSQL workers used for a single Postgres scan. This is similar to Postgres its `max_parallel_workers_per_gather` setting.

Default: `2`

Access: General

## Developer settings

### `duckdb.allow_unsigned_extensions`

Allow DuckDB to load extensions with invalid or missing signatures. Mostly useful for development of DuckDB extensions.

Default: `false`

Access: Superuser-only
