extern "C" {
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
}

#include "pgduckdb/pgduckdb_metadata_cache.hpp"

extern "C" {
char *
pgduckdb_function_name(Oid function_oid) {
	if (!pgduckdb::IsDuckdbOnlyFunction(function_oid)) {
		return nullptr;
	}
	auto func_name = get_func_name(function_oid);
	return psprintf("system.main.%s", quote_identifier(func_name));
}
}
