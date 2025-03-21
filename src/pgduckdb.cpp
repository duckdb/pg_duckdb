#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "postmaster/bgworker_internals.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_fdw.hpp"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"

static void DuckdbInitGUC(void);

bool duckdb_force_execution = false;
bool duckdb_unsafe_allow_mixed_transactions = false;
bool duckdb_log_pg_explain = false;
int duckdb_max_workers_per_postgres_scan = 2;
char *duckdb_motherduck_token = strdup("");
char *duckdb_motherduck_postgres_database = strdup("postgres");
char *duckdb_motherduck_default_database = strdup("");
char *duckdb_motherduck_session_hint = strdup("");
char *duckdb_motherduck_background_catalog_refresh_inactivity_timeout = strdup("");
char *duckdb_postgres_role = strdup("");

int duckdb_maximum_threads = -1;
char *duckdb_maximum_memory = strdup("4GB");
char *duckdb_disabled_filesystems = strdup("LocalFileSystem");
bool duckdb_enable_external_access = true;
bool duckdb_allow_community_extensions = false;
bool duckdb_allow_unsigned_extensions = false;
bool duckdb_autoinstall_known_extensions = true;
bool duckdb_autoload_known_extensions = true;

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
	pgduckdb::InitBackgroundWorkersShmem();
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

static void
DuckdbInitGUC(void) {
	DefineCustomVariable("duckdb.force_execution", "Force queries to use DuckDB execution", &duckdb_force_execution);

	DefineCustomVariable("duckdb.unsafe_allow_mixed_transactions",
	                     "Allow mixed transactions between DuckDB and Postgres",
	                     &duckdb_unsafe_allow_mixed_transactions);

	DefineCustomVariable("duckdb.log_pg_explain", "Logs the EXPLAIN plan of a Postgres scan at the NOTICE log level",
	                     &duckdb_log_pg_explain);

	DefineCustomVariable("duckdb.enable_external_access", "Allow the DuckDB to access external state.",
	                     &duckdb_enable_external_access, PGC_SUSET);

	DefineCustomVariable("duckdb.allow_community_extensions", "Disable installing community extensions",
	                     &duckdb_allow_community_extensions, PGC_SUSET);

	DefineCustomVariable("duckdb.allow_unsigned_extensions",
	                     "Allow DuckDB to load extensions with invalid or missing signatures",
	                     &duckdb_allow_unsigned_extensions, PGC_SUSET);

	DefineCustomVariable(
	    "duckdb.autoinstall_known_extensions",
	    "Whether known extensions are allowed to be automatically installed when a DuckDB query depends on them",
	    &duckdb_autoinstall_known_extensions, PGC_SUSET);

	DefineCustomVariable(
	    "duckdb.autoload_known_extensions",
	    "Whether known extensions are allowed to be automatically loaded when a DuckDB query depends on them",
	    &duckdb_autoload_known_extensions, PGC_SUSET);

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

	DefineCustomVariable("duckdb.max_workers_per_postgres_scan",
	                     "Maximum number of PostgreSQL workers used for a single Postgres scan",
	                     &duckdb_max_workers_per_postgres_scan, 0, MAX_PARALLEL_WORKER_LIMIT);

	DefineCustomVariable("duckdb.postgres_role",
	                     "Which postgres role should be allowed to use DuckDB execution, use the secrets and create "
	                     "MotherDuck tables. Defaults to superusers only",
	                     &duckdb_postgres_role, PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

	DefineCustomVariable("duckdb.motherduck_token", "The token to use for MotherDuck", &duckdb_motherduck_token,
	                     PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

	DefineCustomVariable("duckdb.motherduck_postgres_database", "Which database to enable MotherDuck support in",
	                     &duckdb_motherduck_postgres_database, PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

	DefineCustomVariable("duckdb.motherduck_default_database",
	                     "Which database in MotherDuck to designate as default (in place of my_db)",
	                     &duckdb_motherduck_default_database, PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

	DefineCustomVariable("duckdb.motherduck_session_hint", "The session hint to use for MotherDuck connections",
	                     &duckdb_motherduck_session_hint);

	DefineCustomVariable("duckdb.motherduck_background_catalog_refresh_inactivity_timeout",
	                     "When to stop syncing of the motherduck catalog when no activity has taken place",
	                     &duckdb_motherduck_background_catalog_refresh_inactivity_timeout, PGC_POSTMASTER,
	                     GUC_SUPERUSER_ONLY);
}
