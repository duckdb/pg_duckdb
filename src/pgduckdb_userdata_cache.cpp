extern "C" {
#include "postgres.h"

#include "miscadmin.h"
#include "catalog/pg_user_mapping.h"
#include "commands/extension.h"
#include "foreign/foreign.h"
#include "access/htup_details.h"
#include "utils/inval.h"
#include "utils/syscache.h"
}

#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_userdata_cache.hpp"
#include "pgduckdb/pgduckdb_fdw.hpp"

namespace pgduckdb {
namespace {

struct {
	/* First, some metadata for the cache itself */
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

	/* Anything below is the actual cache content */

	Oid motherduck_foreign_server_oid;
	Oid motherduck_postgres_role_oid;
	Oid motherduck_user_mapping_oid;
} cache = {};

bool callback_is_configured = false;

void
InvalidateCache(Datum, int, uint32) {
	InvalidateUserDataCache();
}

} // namespace

void
InvalidateUserDataCache() {
	if (!cache.valid) {
		return;
	}

	cache.valid = false;

	cache.motherduck_foreign_server_oid = InvalidOid;
	cache.motherduck_postgres_role_oid = InvalidOid;
	cache.motherduck_user_mapping_oid = InvalidOid;
}

void
LoadMotherDuckCache() {
	Assert(!cache.valid); // shouldn't be called if the cache is already valid

	auto server_oid = pgduckdb::FindMotherDuckForeignServerOid();
	cache.motherduck_foreign_server_oid = server_oid;

	if (server_oid == InvalidOid) {
		return;
	}

	auto server = GetForeignServer(server_oid);
	cache.motherduck_postgres_role_oid = server->owner; // TODO - make configurable

	{
		auto tp = SearchSysCache2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(GetUserId()), ObjectIdGetDatum(server_oid));
		if (HeapTupleIsValid(tp)) {
			cache.motherduck_user_mapping_oid = ((Form_pg_user_mapping)GETSTRUCT(tp))->oid;
			ReleaseSysCache(tp);
		}
	}
}

void
InitUserDataCache() {
	if (cache.valid) {
		return;
	}

	if (!callback_is_configured) {
		callback_is_configured = true;
		CacheRegisterSyscacheCallback(USERMAPPINGOID, InvalidateCache, (Datum)0);
	}

	LoadMotherDuckCache();

	cache.valid = true;

	// Now that the cache is valid we can start the background worker
	// if needed. Note that the function needs a valid cache to work.
	StartBackgroundWorkerIfNeeded();
}

bool
IsMotherDuckEnabled() {
	InitUserDataCache();
	return cache.motherduck_foreign_server_oid != InvalidOid && cache.motherduck_user_mapping_oid != InvalidOid;
}

Oid
MotherDuckPostgresUser() {
	Assert(cache.valid);
	return cache.motherduck_postgres_role_oid;
}

Oid
GetMotherduckForeignServerOid() {
	Assert(cache.valid);
	return cache.motherduck_foreign_server_oid;
}

Oid
GetMotherDuckUserMappingOid() {
	Assert(cache.valid);
	return cache.motherduck_user_mapping_oid;
}

} // namespace pgduckdb
