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
	bool valid;
	bool installed;
	List *duckdb_only_functions;
} cache = {};

bool callback_is_configured = false;
uint32 schema_hash_value;

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

static void
BuildDuckdbOnlyFunctions() {
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

bool
IsExtensionRegistered() {
	if (cache.valid) {
		return cache.installed;
	}

	if (!callback_is_configured) {
		callback_is_configured = true;
		schema_hash_value = GetSysCacheHashValue1(NAMESPACENAME, CStringGetDatum("duckdb"));

		CacheRegisterSyscacheCallback(NAMESPACENAME, InvalidateCaches, (Datum)0);
	}

	cache.installed = get_extension_oid("pg_duckdb", true) != InvalidOid;
	if (cache.installed) {
		BuildDuckdbOnlyFunctions();
	}
	cache.valid = true;

	return cache.installed;
}

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
