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
char *duckdb_motherduck_token = strdup("");
char *duckdb_motherduck_postgres_user = strdup("");

int duckdb_maximum_threads = -1;
char *duckdb_maximum_memory = NULL;
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
DefineCustomVariable(const char *name, const char *short_desc, bool *var, GucContext context = PGC_USERSET,
                     int flags = 0) {
	DefineCustomBoolVariable(name, gettext_noop(short_desc), NULL, var, *var, context, flags, NULL, NULL, NULL);
}

static void
DefineCustomVariable(const char *name, const char *short_desc, char **var, GucContext context = PGC_USERSET,
                     int flags = 0) {
	DefineCustomStringVariable(name, gettext_noop(short_desc), NULL, var, *var, context, flags, NULL, NULL, NULL);
}

template <typename T>
static void
DefineCustomVariable(const char *name, const char *short_desc, T *var, T min, T max, GucContext context = PGC_USERSET,
                     int flags = 0) {
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
	func(name, gettext_noop(short_desc), NULL, var, *var, min, max, context, flags, NULL, NULL, NULL);
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

	DefineCustomVariable("duckdb.threads", "Maximum number of DuckDB threads per Postgres backend.",
	                     &duckdb_maximum_threads, -1, 1024);

	DefineCustomVariable("duckdb.max_threads_per_query",
	                     "Maximum number of DuckDB threads used for a single Postgres scan",
	                     &duckdb_max_threads_per_query, 1, 64);

	DefineCustomVariable("duckdb.background_worker_db", "Which database run the background worker in",
	                     &duckdb_background_worker_db);

	DefineCustomVariable("duckdb.motherduck_token", "The token to use for MotherDuck", &duckdb_motherduck_token,
	                     PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

	DefineCustomVariable("duckdb.motherduck_postgres_user",
	                     "As which Postgres user to create the MotherDuck tables in Postgres, empty string means the "
	                     "bootstrap superuser",
	                     &duckdb_motherduck_postgres_user, PGC_POSTMASTER, GUC_SUPERUSER_ONLY);
}
