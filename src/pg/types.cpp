#include "pgduckdb/pg/types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
extern "C" {
#include "postgres.h"
#include "utils/lsyscache.h"
}

namespace pgduckdb::pg {

bool
IsArrayType(Oid type_oid) {
	// inlined type_is_array
	return PostgresFunctionGuard(get_element_type, type_oid) != InvalidOid;
}

} // namespace pgduckdb::pg
