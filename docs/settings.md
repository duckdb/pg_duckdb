# pg_duckdb Settings

Many of these settings are simply used to configure specific [DuckDB settings](https://duckdb.org/docs/configuration/overview.html). If there's a setting from DuckDB that you'd like to see added, please open an issue/PR.

### `duckdb.force_execution`

Force queries to use DuckDB execution. This is only necessary when accessing only Postgres tables in a query. As soon as you access a DuckDB table or DuckDB function (like `read_parquet`), DuckDB execution will be used automatically.

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

Access: General

## Security

### `duckdb.postgres_role`

Which Postgres role should be allowed to use DuckDB execution, use the secrets and create MotherDuck tables. Defaults to superusers only. If this is configured, but the role does not exist when running `CREATE EXTENSION pg_duckdb`, it will be created automatically. This role will have access to DuckDB secrets and data in MotherDuck (tables, secrets, etc).

Default: `""`

Access: Needs to be in the `postgresql.conf` file and requires a restart

### `duckdb.disabled_filesystems`

Disable specific file systems preventing access. This setting only applies to non-superusers. Superusers can always access all file systems. Unless you completely trust the user in `duckdb.posgres_role`, it is recommended to disable `LocalFileSystem`. Otherwise they can trivially read and write any file on the machine that the Postgres process can.

Default: `"LocalFileSystem"`

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

Maximum number of DuckDB threads per Postgres connection.

Default: `-1`

Access: Superuser-only

### `duckdb.max_threads_per_postgres_scan` (experimental)

Maximum number of DuckDB threads used for a single Postgres scan on heap tables (Postgres its regular storage format). In early testing, setting this to `1` has shown to be faster in most cases (for now). So changing this setting to a higher value than the default is currently not recommended.

Default: `1`

Access: General

## Developer settings

### `duckdb.allow_unsigned_extensions`

Allow DuckDB to load extensions with invalid or missing signatures. Mostly useful for development of DuckDB extensions.

Default: `false`

Access: Superuser-only
