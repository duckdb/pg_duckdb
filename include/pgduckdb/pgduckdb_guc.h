#pragma once

extern bool duckdb_force_execution;
extern bool duckdb_unsafe_allow_mixed_transactions;
extern bool duckdb_log_pg_explain;
extern int duckdb_maximum_threads;
extern char *duckdb_maximum_memory;
extern char *duckdb_disabled_filesystems;
extern bool duckdb_enable_external_access;
extern bool duckdb_allow_community_extensions;
extern bool duckdb_allow_unsigned_extensions;
extern bool duckdb_autoinstall_known_extensions;
extern bool duckdb_autoload_known_extensions;
extern int duckdb_max_workers_per_postgres_scan;
extern char *duckdb_postgres_role;
extern char *duckdb_motherduck_session_hint;
extern char *duckdb_max_temp_directory_size;
