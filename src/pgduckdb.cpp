extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/guc.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_node.hpp"

static void DuckdbInitGUC(void);

bool duckdb_execution = false;
int duckdb_max_threads_per_query = 1;

extern "C" {
PG_MODULE_MAGIC;
void
_PG_init(void) {
	if (!process_shared_preload_libraries_in_progress) {
		ereport(ERROR, (errmsg("pg_duckdb needs to be loaded via shared_preload_libraries"),
		                errhint("Add pg_duckdb to shared_preload_libraries.")));
	}
	DuckdbInitGUC();
	DuckdbInitHooks();
	DuckdbInitNode();
}
} // extern "C"

/* clang-format off */
static void
DuckdbInitGUC(void) {

    DefineCustomBoolVariable("duckdb.execution",
                             gettext_noop("Is DuckDB query execution enabled."),
                             NULL,
                             &duckdb_execution,
                             false,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomIntVariable("duckdb.max_threads_per_query",
                            gettext_noop("DuckDB max no. threads per query."),
                            NULL,
                            &duckdb_max_threads_per_query,
                            1,
                            1,
                            64,
                            PGC_USERSET,
                            0,
                            NULL,
                            NULL,
                            NULL);

}
