extern "C" {
#include "postgres.h"

#include "commands/extension.h"
#include "utils/inval.h"
#include "utils/syscache.h"
}

namespace pgduckdb {
struct {
	bool valid;
	bool installed;
} cache = {};

bool callback_is_configured = false;
uint32 schema_hash_value;

static void
InvalidateCaches(Datum arg, int cache_id, uint32 hash_value) {
	if (hash_value != schema_hash_value) {
		return;
	}
	cache.valid = false;
}

bool
IsExtensionRegistered() {
	if (cache.valid) {
		return cache.installed;
	}

	cache.installed = get_extension_oid("pg_duckdb", true) != InvalidOid;
	cache.valid = true;

	if (!callback_is_configured) {
		callback_is_configured = true;
		schema_hash_value = GetSysCacheHashValue1(NAMESPACENAME, CStringGetDatum("duckdb"));

		CacheRegisterSyscacheCallback(NAMESPACENAME, InvalidateCaches, (Datum)0);
	}

	return cache.installed;
}

} // namespace pgduckdb
