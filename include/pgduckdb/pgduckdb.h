#pragma once

/* Values for the backslash_quote GUC */
typedef enum {
	MOTHERDUCK_OFF,
	MOTHERDUCK_ON,
	MOTHERDUCK_AUTO,
} MotherDuckEnabled;

// pgduckdb.cpp
extern "C" void _PG_init(void);

void DuckdbInitGUC();

// pgduckdb_hooks.cpp
void DuckdbInitHooks();

// pgduckdb_node.cpp
void DuckdbInitNode();

// pgduckdb_background_worker.cpp
void DuckdbInitBackgroundWorker();

namespace pgduckdb {
// pgduckdb_xact.cpp
void RegisterDuckdbXactCallback();
} // namespace pgduckdb
