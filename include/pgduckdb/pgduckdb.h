#pragma once

// pgduckdb.c
extern bool duckdb_execution;
extern int duckdb_max_threads_per_query;
extern char *duckdb_background_worker_db;
extern char *duckdb_motherduck_token;
extern char *duckdb_motherduck_postgres_user;
extern "C" void _PG_init(void);

// pgduckdb_hooks.c
void DuckdbInitHooks(void);
