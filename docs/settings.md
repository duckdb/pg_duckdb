# pg_duckdb Settings

TODO:

* properly list all settings
* what the setting does in more detail
* what the default is and why
* when is it recommended to change this setting

## Temporary list of settings

```c
DefineCustomVariable("duckdb.force_execution", "Force queries to use DuckDB execution", &duckdb_force_execution);

DefineCustomVariable("duckdb.enable_external_access", "Allow the DuckDB to access external state.",
                     &duckdb_enable_external_access, PGC_SUSET);

DefineCustomVariable("duckdb.allow_unsigned_extensions",
                     "Allow DuckDB to load extensions with invalid or missing signatures",
                     &duckdb_allow_unsigned_extensions, PGC_SUSET);

DefineCustomVariable("duckdb.max_memory", "The maximum memory DuckDB can use (e.g., 1GB)", &duckdb_maximum_memory,
                     PGC_SUSET);
DefineCustomVariable("duckdb.memory_limit",
                     "The maximum memory DuckDB can use (e.g., 1GB), alias for duckdb.max_memory",
                     &duckdb_maximum_memory, PGC_SUSET);

DefineCustomVariable("duckdb.disabled_filesystems",
                     "Disable specific file systems preventing access (e.g., LocalFileSystem)",
                     &duckdb_disabled_filesystems, PGC_SUSET);

DefineCustomVariable("duckdb.threads", "Maximum number of DuckDB threads per Postgres backend.",
                     &duckdb_maximum_threads, -1, 1024, PGC_SUSET);
DefineCustomVariable("duckdb.worker_threads",
                     "Maximum number of DuckDB threads per Postgres backend, alias for duckdb.threads",
                     &duckdb_maximum_threads, -1, 1024, PGC_SUSET);

DefineCustomVariable("duckdb.max_threads_per_postgres_scan",
                     "Maximum number of DuckDB threads used for a single Postgres scan",
                     &duckdb_max_threads_per_postgres_scan, 1, 64);

DefineCustomVariable("duckdb.postgres_role",
                     "Which postgres role should be allowed to use DuckDB execution, use the secrets and create "
                     "MotherDuck tables. Defaults to superusers only",
                     &duckdb_postgres_role, PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

DefineCustomVariable("duckdb.motherduck_enabled",
                     "If motherduck support should enabled. 'auto' means enabled if motherduck_token is set",
                     &duckdb_motherduck_enabled, motherduck_enabled_options, PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

DefineCustomVariable("duckdb.motherduck_token", "The token to use for MotherDuck", &duckdb_motherduck_token,
                     PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

DefineCustomVariable("duckdb.motherduck_postgres_database", "Which database to enable MotherDuck support in",
                     &duckdb_motherduck_postgres_database);
```
