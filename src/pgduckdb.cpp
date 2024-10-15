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
char *duckdb_background_worker_db = strdup("postgres");
char *duckdb_motherduck_token = NULL;
char *duckdb_motherduck_postgres_user = NULL;

int duckdb_maximum_threads = -1;
char *duckdb_maximum_memory = NULL;
char *duckdb_disabled_filesystems = NULL;
bool duckdb_enable_external_access = true;
bool duckdb_allow_unsigned_extensions = false;

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

static void
DefineCustomVariable(const char *name, const char *short_desc, bool *var) {
	DefineCustomBoolVariable(name, gettext_noop(short_desc), NULL, var, *var, PGC_USERSET, 0, NULL, NULL, NULL);
}

static void
DefineCustomVariable(const char *name, const char *short_desc, char **var) {
	DefineCustomStringVariable(name, gettext_noop(short_desc), NULL, var, *var, PGC_USERSET, 0, NULL, NULL, NULL);
}

template <typename T>
static void
DefineCustomVariable(const char *name, const char *short_desc, T *var, T min, T max) {
	/* clang-format off */
	void (*func)(
			const char *name,
			const char *short_desc,
			const char *long_desc,
			T *valueAddr,
			T bootValue,
			T minValue,
			T maxValue,
			GucContext context,
			int flags,
			GucIntCheckHook check_hook,
			GucIntAssignHook assign_hook,
			GucShowHook show_hook
	);
	/* clang-format on */
	if constexpr (std::is_integral_v<T>) {
		func = DefineCustomIntVariable;
	} else if constexpr (std::is_floating_point_v<T>) {
		func = DefineCustomRealVariable;
	} else {
		static_assert("Unsupported type");
	}
	func(name, gettext_noop(short_desc), NULL, var, *var, min, max, PGC_USERSET, 0, NULL, NULL, NULL);
}

static void
DuckdbInitGUC(void) {
	DefineCustomVariable("duckdb.execution", "Is DuckDB query execution enabled.", &duckdb_execution);

	DefineCustomVariable("duckdb.enable_external_access", "Allow the DuckDB to access external state.",
	                     &duckdb_enable_external_access);

	DefineCustomVariable("duckdb.allow_unsigned_extensions",
	                     "Allow DuckDB to load extensions with invalid or missing signatures",
	                     &duckdb_allow_unsigned_extensions);

	DefineCustomVariable("duckdb.max_memory", "The maximum memory DuckDB can use (e.g., 1GB)", &duckdb_maximum_memory);

	DefineCustomVariable("duckdb.disabled_filesystems",
	                     "Disable specific file systems preventing access (e.g., LocalFileSystem)",
	                     &duckdb_disabled_filesystems);

	DefineCustomVariable("duckdb.threads", "DuckDB max number of threads.", &duckdb_maximum_threads, -1, 1024);

	DefineCustomVariable("duckdb.max_threads_per_query", "DuckDB max number of thread(s) per query.",
	                     &duckdb_max_threads_per_query, 1, 64);

	DefineCustomVariable("duckdb.background_worker_db", "Which database run the backgorund worker in",
	                     &duckdb_background_worker_db);

	/* clang-format off */
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
	/* clang-format on */
}
