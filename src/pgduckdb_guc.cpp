#include <type_traits>

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/guc_hooks.h"
#include "miscadmin.h" // DataDir
#include "lib/stringinfo.h"
#include "postmaster/bgworker_internals.h"
#include "pgduckdb/vendor/pgtz.hpp"
}

namespace pgduckdb {

namespace {
char *
MakeDirName(const char *name) {
	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "%s/pg_duckdb/%s", DataDir, name);
	return buf.data;
}

template <typename T>
bool
GucCheckDuckDBNotInitdHook(T *, void **, GucSource) {
	if (pgduckdb::DuckDBManager::IsInitialized()) {
		GUC_check_errmsg("Cannot set this variable after DuckDB has been initialized. Reconnect to Postgres or use "
		                 "`duckdb.recycle_ddb()` to reset "
		                 "the DuckDB instance.");
		return false;
	}
	return true;
}

template <typename T>
using GucTypeCheckHook = bool (*)(T *, void **, GucSource);

template <typename T>
using GucTypeAssignHook = void (*)(T, void *);

void
DefineCustomVariable(const char *name, const char *short_desc, bool *var, GucContext context = PGC_USERSET,
                     int flags = 0, GucBoolCheckHook check_hook = NULL, GucBoolAssignHook assign_hook = NULL,
                     GucShowHook show_hook = NULL) {
	DefineCustomBoolVariable(name, gettext_noop(short_desc), NULL, var, *var, context, flags, check_hook, assign_hook,
	                         show_hook);
}

void
DefineCustomVariable(const char *name, const char *short_desc, char **var, GucContext context = PGC_USERSET,
                     int flags = 0, GucStringCheckHook check_hook = NULL, GucStringAssignHook assign_hook = NULL,
                     GucShowHook show_hook = NULL) {
	DefineCustomStringVariable(name, gettext_noop(short_desc), NULL, var, *var, context, flags, check_hook, assign_hook,
	                           show_hook);
}

template <typename T>
void
DefineCustomVariable(const char *name, const char *short_desc, T *var, T min, T max, GucContext context = PGC_USERSET,
                     int flags = 0, GucTypeCheckHook<T> check_hook = NULL, GucTypeAssignHook<T> assign_hook = NULL,
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
			GucTypeCheckHook<T> check_hook,
			GucTypeAssignHook<T> assign_hook,
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

void
DefineCustomDuckDBVariable(const char *name, const char *short_desc, bool *var, GucContext context = PGC_USERSET,
                           int flags = 0, GucBoolAssignHook assign_hook = NULL, GucShowHook show_hook = NULL) {
	DefineCustomVariable(name, short_desc, var, context, flags, GucCheckDuckDBNotInitdHook, assign_hook, show_hook);
}

void
DefineCustomDuckDBVariable(const char *name, const char *short_desc, char **var, GucContext context = PGC_USERSET,
                           int flags = 0, GucStringAssignHook assign_hook = NULL, GucShowHook show_hook = NULL) {
	DefineCustomVariable(name, short_desc, var, context, flags, GucCheckDuckDBNotInitdHook, assign_hook, show_hook);
}

template <typename T>
void
DefineCustomDuckDBVariable(const char *name, const char *short_desc, T *var, T min, T max,
                           GucContext context = PGC_USERSET, int flags = 0) {
	DefineCustomVariable(name, short_desc, var, min, max, context, flags, GucCheckDuckDBNotInitdHook<T>,
	                     (GucTypeAssignHook<T>)NULL, NULL);
}
} // namespace

bool duckdb_force_execution = false;
bool duckdb_unsafe_allow_mixed_transactions = false;
bool duckdb_convert_unsupported_numeric_to_double = false;
bool duckdb_log_pg_explain = false;
int duckdb_threads_for_postgres_scan = 2;
int duckdb_max_workers_per_postgres_scan = 2;
char *duckdb_motherduck_session_hint = strdup("");
char *duckdb_postgres_role = strdup("");
bool duckdb_force_motherduck_views = false;

int duckdb_maximum_threads = -1;
char *duckdb_maximum_memory = strdup("4GB");
char *duckdb_disabled_filesystems = strdup("");
bool duckdb_enable_external_access = true;
bool duckdb_allow_community_extensions = false;
bool duckdb_allow_unsigned_extensions = false;
bool duckdb_autoinstall_known_extensions = true;
bool duckdb_autoload_known_extensions = true;
char *duckdb_temporary_directory = MakeDirName("temp");
char *duckdb_extension_directory = MakeDirName("extensions");
char *duckdb_max_temp_directory_size = strdup("");
char *duckdb_default_collation = strdup("C");

void
InitGUC() {
	/* pg_duckdb specific GUCs */
	DefineCustomVariable("duckdb.force_execution", "Force queries to use DuckDB execution", &duckdb_force_execution);

	DefineCustomVariable("duckdb.unsafe_allow_mixed_transactions",
	                     "Allow mixed transactions between DuckDB and Postgres",
	                     &duckdb_unsafe_allow_mixed_transactions);

	DefineCustomVariable("duckdb.convert_unsupported_numeric_to_double",
	                     "Convert NUMERIC types of unsupported precision to DOUBLE",
	                     &duckdb_convert_unsupported_numeric_to_double);

	DefineCustomVariable("duckdb.log_pg_explain", "Logs the EXPLAIN plan of a Postgres scan at the NOTICE log level",
	                     &duckdb_log_pg_explain);

	DefineCustomVariable("duckdb.threads_for_postgres_scan",
	                     "Maximum number of DuckDB threads used for a single Postgres scan",
	                     &duckdb_threads_for_postgres_scan, 1, MAX_PARALLEL_WORKER_LIMIT);
	DefineCustomVariable("duckdb.max_workers_per_postgres_scan",
	                     "Maximum number of PostgreSQL workers used for a single Postgres scan",
	                     &duckdb_max_workers_per_postgres_scan, 0, MAX_PARALLEL_WORKER_LIMIT);

	DefineCustomVariable("duckdb.postgres_role",
	                     "Which postgres role should be allowed to use DuckDB execution, use the secrets and create "
	                     "MotherDuck tables. Defaults to superusers only",
	                     &duckdb_postgres_role, PGC_POSTMASTER, GUC_SUPERUSER_ONLY);

	DefineCustomVariable("duckdb.force_motherduck_views",
	                     "Force all views to be created in MotherDuck, even if they don't use MotherDuck tables",
	                     &duckdb_force_motherduck_views);

	/* GUCs acting on DuckDB instance */
	DefineCustomDuckDBVariable("duckdb.enable_external_access", "Allow the DuckDB to access external state.",
	                           &duckdb_enable_external_access, PGC_SUSET);

	DefineCustomDuckDBVariable("duckdb.allow_community_extensions", "Disable installing community extensions",
	                           &duckdb_allow_community_extensions, PGC_SUSET);

	DefineCustomDuckDBVariable("duckdb.allow_unsigned_extensions",
	                           "Allow DuckDB to load extensions with invalid or missing signatures",
	                           &duckdb_allow_unsigned_extensions, PGC_SUSET);

	DefineCustomDuckDBVariable(
	    "duckdb.autoinstall_known_extensions",
	    "Whether known extensions are allowed to be automatically installed when a DuckDB query depends on them",
	    &duckdb_autoinstall_known_extensions, PGC_SUSET);

	DefineCustomDuckDBVariable(
	    "duckdb.autoload_known_extensions",
	    "Whether known extensions are allowed to be automatically loaded when a DuckDB query depends on them",
	    &duckdb_autoload_known_extensions, PGC_SUSET);

	DefineCustomDuckDBVariable("duckdb.max_memory", "The maximum memory DuckDB can use (e.g., 1GB)",
	                           &duckdb_maximum_memory, PGC_SUSET);
	DefineCustomDuckDBVariable("duckdb.memory_limit",
	                           "The maximum memory DuckDB can use (e.g., 1GB), alias for duckdb.max_memory",
	                           &duckdb_maximum_memory, PGC_SUSET);

	DefineCustomDuckDBVariable(
	    "duckdb.temporary_directory",
	    "Set the directory to which DuckDB write temp files, alias for duckdb.temporary_directory",
	    &duckdb_temporary_directory, PGC_SUSET);

	DefineCustomDuckDBVariable(
	    "duckdb.max_temp_directory_size",
	    "The maximum amount of data stored inside DuckDB's 'temp_directory' (when set) (e.g., 1GB), "
	    "alias for duckdb.max_temp_directory_size",
	    &duckdb_max_temp_directory_size, PGC_SUSET);

	DefineCustomDuckDBVariable(
	    "duckdb.extension_directory",
	    "Set the directory to where DuckDB stores extensions in, alias for duckdb.extension_directory",
	    &duckdb_extension_directory, PGC_SUSET);

	DefineCustomDuckDBVariable("duckdb.threads", "Maximum number of DuckDB threads per Postgres backend.",
	                           &duckdb_maximum_threads, -1, 1024, PGC_SUSET);
	DefineCustomDuckDBVariable("duckdb.worker_threads",
	                           "Maximum number of DuckDB threads per Postgres backend, alias for duckdb.threads",
	                           &duckdb_maximum_threads, -1, 1024, PGC_SUSET);

	DefineCustomDuckDBVariable("duckdb.default_collation",
	                           "The default collation to use for DuckDB queries, e.g., 'en_us'",
	                           &duckdb_default_collation, PGC_SUSET);

	DefineCustomDuckDBVariable("duckdb.motherduck_session_hint", "The session hint to use for MotherDuck connections",
	                           &duckdb_motherduck_session_hint);

	DefineCustomDuckDBVariable("duckdb.disabled_filesystems",
	                           "Disable specific file systems preventing access (e.g., LocalFileSystem)",
	                           &duckdb_disabled_filesystems, PGC_SUSET);
}

extern "C" {

#if PG_VERSION_NUM >= 140000 && PG_VERSION_NUM < 160000
/*
 * the bare comparison function for GUC names
 */
static int
guc_name_compare(const char *namea, const char *nameb) {
	/*
	 * The temptation to use strcasecmp() here must be resisted, because the
	 * array ordering has to remain stable across setlocale() calls. So, build
	 * our own with a simple ASCII-only downcasing.
	 */
	while (*namea && *nameb) {
		char cha = *namea++;
		char chb = *nameb++;

		if (cha >= 'A' && cha <= 'Z')
			cha += 'a' - 'A';
		if (chb >= 'A' && chb <= 'Z')
			chb += 'a' - 'A';
		if (cha != chb)
			return cha - chb;
	}
	if (*namea)
		return 1; /* a is longer */
	if (*nameb)
		return -1; /* b is longer */
	return 0;
}

/*
 * comparator for qsorting and bsearching guc_variables array
 */
static int
guc_var_compare(const void *a, const void *b) {
	const char *namea = **(const char **const *)a;
	const char *nameb = **(const char **const *)b;

	return guc_name_compare(namea, nameb);
}

/*
 * To allow continued support of obsolete names for GUC variables, we apply
 * the following mappings to any unrecognized name.  Note that an old name
 * should be mapped to a new one only if the new variable has very similar
 * semantics to the old.
 */
static const char *const map_old_guc_names[] = {"sort_mem", "work_mem", "vacuum_mem", "maintenance_work_mem", NULL};

static struct config_generic *
find_option(const char *name, bool, bool skip_errors, int elevel) {
	const char **key = &name;
	struct config_generic **res;
	int i;
	auto *guc_variables = get_guc_variables();

	Assert(name);

	/*
	 * By equating const char ** with struct config_generic *, we are assuming
	 * the name field is first in config_generic.
	 */
	res = (struct config_generic **)bsearch((void *)&key, (void *)guc_variables, GetNumConfigOptions(),
	                                        sizeof(struct config_generic *), guc_var_compare);
	if (res)
		return *res;

	/*
	 * See if the name is an obsolete name for a variable.  We assume that the
	 * set of supported old names is short enough that a brute-force search is
	 * the best way.
	 */
	for (i = 0; map_old_guc_names[i] != NULL; i += 2) {
		if (guc_name_compare(name, map_old_guc_names[i]) == 0)
			return find_option(map_old_guc_names[i + 1], false, skip_errors, elevel);
	}

	/* Unknown name */
	if (!skip_errors)
		ereport(elevel,
		        (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("unrecognized configuration parameter \"%s\"", name)));
	return NULL;
}
#endif

#if PG_VERSION_NUM < 170000
struct config_generic *
get_config_handle(const char *name) {
	struct config_generic *gen = find_option(name, false, false, 0);

	if (gen && ((gen->flags & GUC_CUSTOM_PLACEHOLDER) == 0))
		return gen;

	return NULL;
}
#endif

static void
DuckAssignTimezone_Cpp(const char *tz) {
	if (IsExtensionRegistered()) {
		if (!DuckDBManager::IsInitialized()) {
			return;
		}

		// update duckdb tz
		auto connection = pgduckdb::DuckDBManager::GetConnection(false);
		pgduckdb::DuckDBQueryOrThrow(*connection, "SET TimeZone =" + duckdb::KeywordHelper::WriteQuoted(tz));
		elog(DEBUG2, "[PGDuckDB] Set DuckDB option: 'TimeZone'=%s", tz);
	}
}

static void
DuckAssignTimezone(const char *newval, void *extra) {
	assign_timezone(newval, extra);

	// assign_timezone have not update guc_variables, so we can't use GetConfigOption
	auto *tz = *((pg_tz **)extra);
	InvokeCPPFunc(DuckAssignTimezone_Cpp, tz->TZname);
}
}

/*
 * Initialize GUC hooks.
 *  some guc shoule be set to duckdb instance, such as timezone
 *  is there any other guc should be set to duckdb instance?
 */
void
InitGUCHooks() {
	// timezone
	{
		if (auto *tz = (struct config_string *)get_config_handle("TimeZone"); tz != nullptr) {
			tz->assign_hook = DuckAssignTimezone;
		}
	}
}

} // namespace pgduckdb
