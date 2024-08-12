#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/guc.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_background_worker.hpp"

static void DuckdbInitGUC(void);

bool duckdb_execution = false;
int duckdb_max_threads_per_query = 1;
char *duckdb_background_worker_db = NULL;
char *duckdb_motherduck_token = NULL;
char *duckdb_motherduck_postgres_user = NULL;

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
	DuckdbInitBackgroundWorker();
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

    DefineCustomStringVariable("duckdb.background_worker_db",
                            gettext_noop("Which database run the backgorund worker in"),
                            NULL,
                            &duckdb_background_worker_db,
                            "postgres",
                            PGC_USERSET,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("duckdb.motherduck_token",
                            gettext_noop("The token to use for MotherDuck"),
                            NULL,
                            &duckdb_motherduck_token,
                            "",
                            PGC_POSTMASTER,
                            GUC_SUPERUSER_ONLY,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("duckdb.motherduck_postgres_user",
                            gettext_noop("As which Postgres user to create the MotherDuck tables in Postgres, empty string means the bootstrap superuser"),
                            NULL,
                            &duckdb_motherduck_postgres_user,
                            "",
                            PGC_POSTMASTER,
                            GUC_SUPERUSER_ONLY,
                            NULL,
                            NULL,
                            NULL);
}
