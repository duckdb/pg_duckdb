#pragma once

// pgduckdb.c
extern bool duckdb_execution;
extern int duckdb_max_threads_per_query;
extern char *duckdb_default_db;
extern "C" void _PG_init(void);

// pgduckdb_hooks.c
void DuckdbInitHooks(void);
