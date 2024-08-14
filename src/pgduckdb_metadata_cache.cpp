extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "commands/extension.h"
#include "nodes/bitmapset.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
}

#include "pgduckdb/vendor/pg_list.hpp"

namespace pgduckdb {
struct {
	/*
	 * Does the cache contain valid data, i.e. is it initialized? Or is it
	 * stale and does it need to be refreshed? If this is false none of the
	 * other fields should be read.
	 */
	bool valid;
	/*
	 * Is the pg_duckdb extension installed? If this is false all the other
	 * fields (except valid) should be ignored. It is totally fine to have
	 * valid=true and installed=false, this happens when the extension is not
	 * installed and we cached that information.
	 */
	bool installed;
	/* A list of Postgres OIDs of functions that can only be executed by DuckDB */
	List *duckdb_only_functions;
} cache = {};

bool callback_is_configured = false;
/* The hash value of the "duckdb" schema name */
uint32 schema_hash_value;

/*
 * This function is called for every pg_namespace tuple that is invalidated. We
 * only invalidate our cache if the "duckdb" schema is invalidated, because
 * that means that the extension was created or dropped (see comment in
 * IsExtensionRegistered for details).
 */
static void
InvalidateCaches(Datum arg, int cache_id, uint32 hash_value) {
	if (hash_value != schema_hash_value) {
		return;
	}
	if (!cache.valid) {
		return;
	}
	cache.valid = false;
	list_free(cache.duckdb_only_functions);
}

static Oid
GetFunctionOid(const char *name, oidvector *args, Oid schema_oid) {
	HeapTuple tuple =
	    SearchSysCache3(PROCNAMEARGSNSP, CStringGetDatum(name), PointerGetDatum(args), ObjectIdGetDatum(schema_oid));

	if (!tuple) {
		// Should never happen, but let's check to be sure
		elog(ERROR, "could not find function %s", name);
	}

	Form_pg_proc proc = (Form_pg_proc)GETSTRUCT(tuple);
	Oid result = proc->oid;

	ReleaseSysCache(tuple);
	return result;
}

/*
 * Builds the list of Postgres OIDs of functions that can only be executed by
 * DuckDB. The resulting list is stored in cache.duckdb_only_functions.
 */
static void
BuildDuckdbOnlyFunctions() {
	/* This function should only be called during cache initialization */
	Assert(!cache.valid);
	Assert(!cache.duckdb_only_functions);

	Oid text_oid = TEXTOID;
	Oid text_array_oid = TEXTARRAYOID;
	oidvector *text_oidvec = buildoidvector(&text_oid, 1);
	oidvector *text_array_oidvec = buildoidvector(&text_array_oid, 1);
	Oid public_schema_oid = get_namespace_oid("public", false);

	Oid read_parquet_text_oid = GetFunctionOid("read_parquet", text_oidvec, public_schema_oid);
	Oid read_parquet_text_array_oid = GetFunctionOid("read_parquet", text_array_oidvec, public_schema_oid);
	Oid read_csv_text_oid = GetFunctionOid("read_csv", text_oidvec, public_schema_oid);
	Oid read_csv_text_array_oid = GetFunctionOid("read_csv", text_array_oidvec, public_schema_oid);
	Oid iceberg_scan_oid = GetFunctionOid("read_parquet", text_oidvec, public_schema_oid);

	MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	cache.duckdb_only_functions = list_make5_oid(read_parquet_text_oid, read_parquet_text_array_oid, read_csv_text_oid,
	                                             read_csv_text_array_oid, iceberg_scan_oid);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Returns true if the pg_duckdb extension is installed (using CREATE
 * EXTENSION). This also initializes our metadata cache if it is not already
 * initialized.
 */
bool
IsExtensionRegistered() {
	if (cache.valid) {
		return cache.installed;
	}

	if (!callback_is_configured) {
		/*
		 * The first time this is run for the backend we need to register a
		 * callback which invalidates the cache. It would be best if we could
		 * invalidate this on DDL that changes the extension, i.e.
		 * CREATE/ALTER/DROP EXTENSION. Sadly, this is currently not possible
		 * because there is no syscache for the pg_extension table. Instead we
		 * subscribe to the syscache of the pg_namespace table for the duckdb
		 * schema. This is not perfect, as it doesn't cover extension updates,
		 * but for now this is acceptable.
		 */
		callback_is_configured = true;
		schema_hash_value = GetSysCacheHashValue1(NAMESPACENAME, CStringGetDatum("duckdb"));

		CacheRegisterSyscacheCallback(NAMESPACENAME, InvalidateCaches, (Datum)0);
	}

	cache.installed = get_extension_oid("pg_duckdb", true) != InvalidOid;
	if (cache.installed) {
		/* If the extension is installed we can build the rest of the cache */
		BuildDuckdbOnlyFunctions();
	}
	cache.valid = true;

	return cache.installed;
}

/*
 * Returns true if the function with the given OID is a function that can only
 * be executed by DuckDB.
 */
bool
IsDuckdbOnlyFunction(Oid function_oid) {
	Assert(cache.valid);

	foreach_oid(duckdb_only_oid, cache.duckdb_only_functions) {
		if (duckdb_only_oid == function_oid) {
			return true;
		}
	}
	return false;
}

} // namespace pgduckdb
