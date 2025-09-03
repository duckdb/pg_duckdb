# Settings

`pg_duckdb` settings are configured in the `postgresql.conf` file. Many of these settings are used to configure specific [DuckDB settings](https://duckdb.org/docs/configuration/overview.html). If there's a setting from DuckDB that you'd like to see added, please open an issue or pull request.

## General

### `duckdb.force_execution`

Forces queries to use DuckDB execution. This is only necessary when accessing only Postgres tables in a query. As soon as you use a DuckDB-only feature, DuckDB execution will be used automatically. DuckDB-only features include reading from DuckDB/MotherDuck tables, using DuckDB functions (like `read_parquet`), or `COPY` to remote storage (e.g., `s3://`).

- **Default**: `false`
- **Access**: General

## Security

### `duckdb.postgres_role`

Specifies the Postgres role that is allowed to use DuckDB execution, manage secrets, and create MotherDuck tables. Defaults to superusers only. If this is configured, but the role does not exist when running `CREATE EXTENSION pg_duckdb`, it will be created automatically. This role will have access to DuckDB secrets and data in MotherDuck.

- **Default**: `""`
- **Access**: Requires restart

### `duckdb.disabled_filesystems`

Disables specific file systems, preventing access for all users, including superusers. For non-superusers who are not members of both the `pg_read_server_files` and `pg_write_server_files` roles, the `LocalFileSystem` is always disabled. If you add `LocalFileSystem` to this setting, superusers will also be unable to access the local file system through DuckDB.

- **Default**: `""`
- **Access**: Superuser-only

### `duckdb.autoinstall_known_extensions`

Determines whether known extensions are allowed to be automatically installed when a DuckDB query depends on them.

- **Default**: `true`
- **Access**: Superuser-only

### `duckdb.autoload_known_extensions`

Determines whether known extensions are allowed to be automatically loaded when a DuckDB query depends on them.

- **Default**: `true`
- **Access**: Superuser-only

### `duckdb.allow_community_extensions`

Determines whether community extensions can be installed.

- **Default**: `false`
- **Access**: Superuser-only

### `duckdb.enable_external_access` (Experimental)

Allows DuckDB to access external resources (e.g., HTTP, S3). This setting is not yet well-tested, and disabling it may break unintended `pg_duckdb` functionality.

- **Default**: `true`
- **Access**: Superuser-only

## Resource Management

Since any connection that uses DuckDB will have its own DuckDB instance, these settings are per-connection. When using `pg_duckdb` in many concurrent connections, it can be a good idea to set some of these more conservatively than their defaults.

### `duckdb.max_memory` / `duckdb.memory_limit`

The maximum memory DuckDB can use within a single Postgres connection, comparable to Postgres's `work_mem` setting. When set to an empty string, this will use DuckDB's default, which is 80% of RAM.

- **Default**: `"4GB"`
- **Access**: Superuser-only

### `duckdb.threads` / `duckdb.worker_threads`

The maximum number of DuckDB threads per Postgres connection. A value of `-1` uses DuckDB's default, which is the number of CPU cores on the machine.

- **Default**: `-1`
- **Access**: Superuser-only

### `duckdb.max_workers_per_postgres_scan`

The maximum number of PostgreSQL workers used for a single Postgres scan, similar to Postgres's `max_parallel_workers_per_gather` setting.

- **Default**: `2`
- **Access**: General

## Data Type Conversion

### `duckdb.convert_unsupported_numeric_to_double`

Converts `NUMERIC` types with unsupported precision/scale to `DOUBLE` instead of throwing an error. DuckDB supports `NUMERIC`/`DECIMAL` with precision 1-38 and scale 0-38 (where scale ≤ precision). For `NUMERIC`s outside these limits, this setting controls the behavior.

- **When `true`**: Unsupported `NUMERIC`s are converted to `DOUBLE` (may cause precision loss).
- **When `false`**: Unsupported `NUMERIC`s cause an error.

- **Default**: `false`
- **Access**: General

## File System and Storage

### `duckdb.temporary_directory`

Sets the directory where DuckDB writes temporary files. By default, DuckDB uses a directory under the PostgreSQL data directory (`DataDir/pg_duckdb/temp`). This can be useful for pointing to faster storage (e.g., an SSD) or managing disk space more effectively.

- **Default**: `"DataDir/pg_duckdb/temp"`
- **Access**: Superuser-only

### `duckdb.max_temp_directory_size`

The maximum amount of data that can be stored in DuckDB's temporary directory. This setting helps prevent runaway queries from consuming all available disk space. When set to an empty string, no limit is enforced.

- **Examples**: `"10GB"`, `"500MB"`, `"2TB"`
- **Default**: `""` (no limit)
- **Access**: Superuser-only

### `duckdb.extension_directory`

Sets the directory where DuckDB stores its extensions. By default, extensions are stored under the PostgreSQL data directory (`DataDir/pg_duckdb/extensions`). This is useful for managing extension storage or sharing extensions across multiple PostgreSQL instances.

- **Default**: `"DataDir/pg_duckdb/extensions"`
- **Access**: Superuser-only

## Developer Settings

### `duckdb.allow_unsigned_extensions`

Allows DuckDB to load extensions with invalid or missing signatures. This is mostly useful for the development of DuckDB extensions.

- **Default**: `false`
- **Access**: Superuser-only
