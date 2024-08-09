extern "C" {
#include "postgres.h"
#include "utils/guc.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

static void DuckdbInitGUC(void);

bool duckdb_execution = false;
int duckdb_max_threads_per_query = 1;
char *duckdb_default_db = NULL;

extern "C" {
PG_MODULE_MAGIC;

void
_PG_init(void) {
	DuckdbInitGUC();
	DuckdbInitHooks();
	DuckdbInitNode();
}
}

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

    DefineCustomStringVariable("duckdb.default_db",
                            gettext_noop("Which database to USE as the default database in DuckDB"),
                            NULL,
                            &duckdb_default_db,
                            "",
                            PGC_USERSET,
                            0,
                            NULL,
                            NULL,
                            NULL);
}
