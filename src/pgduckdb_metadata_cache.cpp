extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "commands/extension.h"
#include "nodes/bitmapset.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
}

#include "pgduckdb/pgduckdb.h"
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
	 * An ever increasing counter that is incremented every time the cache is
	 * revalidated.
	 */
	uint64 version;
	/*
	 * Is the pg_duckdb extension installed? If this is false all the other
	 * fields (except valid) should be ignored. It is totally fine to have
	 * valid=true and installed=false, this happens when the extension is not
	 * installed and we cached that information.
	 */
	bool installed;
	/* The Postgres OID of the pg_duckdb extension. */
	Oid extension_oid;
	/* The OID of the duckdb Table Access Method */
	Oid table_am_oid;
	/* The OID of the duckdb.motherduck_postrges_user */
	Oid motherduck_postgres_user_oid;
	/*
	 * A list of Postgres OIDs of functions that can only be executed by DuckDB.
	 * XXX: We're iterating over this list in IsDuckdbOnlyFunction. If this list
	 * ever becomes large we should consider using a different datastructure
	 * instead (e.g. a hash table). For now using a list is fine though.
	 */
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
	if (cache.installed) {
		list_free(cache.duckdb_only_functions);
		cache.duckdb_only_functions = NIL;
		cache.extension_oid = InvalidOid;
		cache.table_am_oid = InvalidOid;
	}
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
	Assert(cache.extension_oid != InvalidOid);

	/*
	 * We search the system cache for functions with these specific names. It's
	 * possible that other functions with the same also exist, so we check if
	 * each of the found functions is actually part of our extension before
	 * caching its OID as a DuckDB-only function.
	 */
	const char *function_names[] = {"read_parquet", "read_csv", "iceberg_scan"};

	for (int i = 0; i < lengthof(function_names); i++) {
		CatCList *catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(function_names[i]));

		for (int j = 0; j < catlist->n_members; j++) {
			HeapTuple tuple = &catlist->members[j]->tuple;
			Form_pg_proc function = (Form_pg_proc)GETSTRUCT(tuple);
			if (getExtensionOfObject(ProcedureRelationId, function->oid) != cache.extension_oid) {
				continue;
			}

			/* The cache needs to outlive the current transaction so store the list in TopMemoryContext */
			MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
			cache.duckdb_only_functions = lappend_oid(cache.duckdb_only_functions, function->oid);
			MemoryContextSwitchTo(oldcontext);
		}

		ReleaseSysCacheList(catlist);
	}
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

	cache.extension_oid = get_extension_oid("pg_duckdb", true);
	cache.installed = cache.extension_oid != InvalidOid;
	cache.version++;
	if (cache.installed) {
		/* If the extension is installed we can build the rest of the cache */
		BuildDuckdbOnlyFunctions();
		cache.table_am_oid = GetSysCacheOid1(AMNAME, Anum_pg_am_oid, CStringGetDatum("duckdb"));

		if (duckdb_motherduck_postgres_user[0] != '\0') {
			cache.motherduck_postgres_user_oid =
			    GetSysCacheOid1(AUTHNAME, Anum_pg_authid_oid, CStringGetDatum(duckdb_motherduck_postgres_user));
		} else {
			cache.motherduck_postgres_user_oid = BOOTSTRAP_SUPERUSERID;
		}
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

uint64
CacheVersion() {
	Assert(cache.valid);
	return cache.version;
}

Oid
ExtensionOid() {
	Assert(cache.valid);
	return cache.extension_oid;
}

Oid
DuckdbTableAmOid() {
	Assert(cache.valid);
	return cache.table_am_oid;
}

Oid
IsDuckdbTable(Form_pg_class relation) {
	Assert(cache.valid);
	return relation->relam == pgduckdb::DuckdbTableAmOid();
}

Oid
IsDuckdbTable(Relation relation) {
	Assert(cache.valid);
	return IsDuckdbTable(relation->rd_rel);
}

Oid
IsMotherDuckTable(Form_pg_class relation) {
	Assert(cache.valid);
	return IsDuckdbTable(relation) && relation->relpersistence == RELPERSISTENCE_PERMANENT;
}

Oid
IsMotherDuckTable(Relation relation) {
	Assert(cache.valid);
	return IsMotherDuckTable(relation->rd_rel);
}

bool
IsMotherDuckEnabled() {
	return duckdb_motherduck_token[0] != '\0';
}

Oid
MotherDuckPostgresUser() {
	Assert(cache.valid);
	return cache.motherduck_postgres_user_oid;
}

} // namespace pgduckdb
