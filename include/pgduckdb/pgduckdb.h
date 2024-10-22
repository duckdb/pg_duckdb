#pragma once

/* Values for the backslash_quote GUC */
typedef enum {
	MOTHERDUCK_OFF,
	MOTHERDUCK_ON,
	MOTHERDUCK_AUTO,
} MotherDuckEnabled;

// pgduckdb.c
extern bool duckdb_force_execution;
extern int duckdb_maximum_threads;
extern char *duckdb_maximum_memory;
extern char *duckdb_disabled_filesystems;
extern bool duckdb_enable_external_access;
extern bool duckdb_allow_unsigned_extensions;
extern int duckdb_max_threads_per_postgres_scan;
extern char *duckdb_motherduck_postgres_database;
extern int duckdb_motherduck_enabled;
extern char *duckdb_motherduck_token;
extern char *duckdb_postgres_role;
extern "C" void _PG_init(void);

// pgduckdb_hooks.c
void DuckdbInitHooks(void);
