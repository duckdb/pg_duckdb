#pragma once

#include "postgres.h"

// pgduckdb.c
extern bool duckdb_execution;
extern int duckdb_max_threads_per_query;
extern PGDLLEXPORT char *duckdb_default_db;
extern "C" void _PG_init(void);

// pgduckdb_hooks.c
void DuckdbInitHooks(void);
