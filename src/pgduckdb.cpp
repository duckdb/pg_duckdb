extern "C" {
#include "postgres.h"
#include "utils/guc.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

static void duckdb_init_guc(void);

bool duckdb_execution = false;
int duckdb_max_threads_per_query = 1;

extern "C" {
PG_MODULE_MAGIC;

void
_PG_init(void) {
	duckdb_init_guc();
	duckdb_init_hooks();
	duckdb_init_node();
}
}

/* clang-format off */
static void
duckdb_init_guc(void) {

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
