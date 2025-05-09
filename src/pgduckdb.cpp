#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_guc.hpp"

#include "pgduckdb/pgduckdb_duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
}

#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"

extern "C" {
PG_MODULE_MAGIC;

void
_PG_init(void) {
	if (!process_shared_preload_libraries_in_progress) {
		ereport(ERROR, (errmsg("pg_duckdb needs to be loaded via shared_preload_libraries"),
		                errhint("Add pg_duckdb to shared_preload_libraries.")));
	}

	pgduckdb::InitGUC();
	DuckdbInitHooks();
	DuckdbInitNode();
	pgduckdb::InitBackgroundWorkersShmem();
	pgduckdb::RegisterDuckdbXactCallback();
}
} // extern "C"
