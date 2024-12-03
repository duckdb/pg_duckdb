#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/guc.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"

static void DuckdbInitGUC(void);

bool duckdb_force_execution = false;
int duckdb_max_threads_per_postgres_scan = 1;
int duckdb_motherduck_enabled = MotherDuckEnabled::MOTHERDUCK_AUTO;
char *duckdb_motherduck_token = strdup("");
char *duckdb_motherduck_postgres_database = strdup("postgres");
char *duckdb_motherduck_default_database = strdup("");
char *duckdb_postgres_role = strdup("");

int duckdb_maximum_threads = -1;
char *duckdb_maximum_memory = strdup("4GB");
char *duckdb_disabled_filesystems = strdup("LocalFileSystem");
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
	pgduckdb::RegisterDuckdbXactCallback();
}
} // extern "C"

static void
DefineCustomVariable(const char *name, const char *short_desc, bool *var, GucContext context = PGC_USERSET,
                     int flags = 0, GucBoolCheckHook check_hook = NULL, GucBoolAssignHook assign_hook = NULL,
                     GucShowHook show_hook = NULL) {
	DefineCustomBoolVariable(name, gettext_noop(short_desc), NULL, var, *var, context, flags, check_hook, assign_hook,
	                         show_hook);
}

static void
DefineCustomVariable(const char *name, const char *short_desc, char **var, GucContext context = PGC_USERSET,
                     int flags = 0, GucStringCheckHook check_hook = NULL, GucStringAssignHook assign_hook = NULL,
                     GucShowHook show_hook = NULL) {
	DefineCustomStringVariable(name, gettext_noop(short_desc), NULL, var, *var, context, flags, check_hook, assign_hook,
	                           show_hook);
}

static void
DefineCustomVariable(const char *name, const char *short_desc, int *var, const struct config_enum_entry *options,
                     GucContext context = PGC_USERSET, int flags = 0, GucEnumCheckHook check_hook = NULL,
                     GucEnumAssignHook assign_hook = NULL, GucShowHook show_hook = NULL) {
	DefineCustomEnumVariable(name, gettext_noop(short_desc), NULL, var, *var, options, context, flags, check_hook,
	                         assign_hook, show_hook);
}

template <typename T>
static void
DefineCustomVariable(const char *name, const char *short_desc, T *var, T min, T max, GucContext context = PGC_USERSET,
                     int flags = 0, GucIntCheckHook check_hook = NULL, GucIntAssignHook assign_hook = NULL,
                     GucShowHook show_hook = NULL) {
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
	func(name, gettext_noop(short_desc), NULL, var, *var, min, max, context, flags, check_hook, assign_hook, show_hook);
}

/*
 * Although only "true", "false", and "auto" are documented, we
 * accept all the likely variants of "true" and "false" that Postgres users are
 * used to.
 */
/* clang-format off */
static const struct config_enum_entry motherduck_enabled_options[] = {
    {"auto", MOTHERDUCK_AUTO, false},
    {"on", MOTHERDUCK_ON, true},
    {"off", MOTHERDUCK_OFF, true},
    {"true", MOTHERDUCK_ON, false},
    {"false", MOTHERDUCK_OFF, false},
    {"yes", MOTHERDUCK_ON, true},
    {"no", MOTHERDUCK_OFF, true},
    {"1", MOTHERDUCK_ON, true},
    {"0", MOTHERDUCK_OFF, true},
    {NULL, 0, false}
};
/* clang-format on */

static void
DuckdbInitGUC(void) {
	DefineCustomVariable("duckdb.force_execution", "Force queries to use DuckDB execution", &duckdb_force_execution);

	DefineCustomVariable("duckdb.enable_external_access", "Allow the DuckDB to access external state.",
	                     &duckdb_enable_external_access, PGC_SUSET);

	DefineCustomVariable("duckdb.allow_unsigned_extensions",
	                     "Allow DuckDB to load extensions with invalid or missing signatures",
	                     &duckdb_allow_unsigned_extensions, PGC_SUSET);

	DefineCustomVariable("duckdb.max_memory", "The maximum memory DuckDB can use (e.g., 1GB)", &duckdb_maximum_memory,
	                     PGC_SUSET);
	DefineCustomVariable("duckdb.memory_limit",
	                     "The maximum memory DuckDB can use (e.g., 1GB), alias for duckdb.max_memory",
	                     &duckdb_maximum_memory, PGC_SUSET);

	DefineCustomVariable("duckdb.disabled_filesystems",
	                     "Disable specific file systems preventing access (e.g., LocalFileSystem)",
	                     &duckdb_disabled_filesystems, PGC_SUSET);

	DefineCustomVariable("duckdb.threads", "Maximum number of DuckDB threads per Postgres backend.",
	                     &duckdb_maximum_threads, -1, 1024, PGC_SUSET);
	DefineCustomVariable("duckdb.worker_threads",
	                     "Maximum number of DuckDB threads per Postgres backend, alias for duckdb.threads",
	                     &duckdb_maximum_threads, -1, 1024, PGC_SUSET);

	DefineCustomVariable("duckdb.max_threads_per_postgres_scan",
	                     "Maximum number of DuckDB threads used for a single Postgres scan",
	                     &duckdb_max_threads_per_postgres_scan, 1, 64);

	DefineCustomVariable("duckdb.postgres_role",
	                     "Which postgres role should be allowed to use DuckDB execution, use the secrets and create "
	                     "MotherDuck tables. Defaults to superusers only",
	                     &duckdb_postgres_role, PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

	DefineCustomVariable("duckdb.motherduck_enabled",
	                     "If motherduck support should enabled. 'auto' means enabled if motherduck_token is set",
	                     &duckdb_motherduck_enabled, motherduck_enabled_options, PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

	DefineCustomVariable("duckdb.motherduck_token", "The token to use for MotherDuck", &duckdb_motherduck_token,
	                     PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

	DefineCustomVariable("duckdb.motherduck_postgres_database", "Which database to enable MotherDuck support in",
	                     &duckdb_motherduck_postgres_database);

	DefineCustomVariable("duckdb.motherduck_default_database", "Which database in MotherDuck to designate as default (in place of my_db)",
						 &duckdb_motherduck_default_database);
}
