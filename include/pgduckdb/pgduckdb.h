#pragma once

// pgduckdb.c
extern bool duckdb_execution;
extern int duckdb_max_threads_per_query;
extern "C" void _PG_init(void);

// pgduckdb_hooks.c
extern void duckdb_init_hooks(void);