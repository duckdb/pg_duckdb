extern "C" {
#include "postgres.h"
#include "utils/rel.h"
}

namespace pgduckdb {
bool IsExtensionRegistered();
bool IsDuckdbOnlyFunction(Oid function_oid);
Oid DuckdbTableAmOid();
bool IsMotherDuckEnabled();
Oid MotherDuckPostgresUser();
Oid IsDuckdbTable(Relation relation);
Oid IsMotherDuckTable(Relation relation);
} // namespace pgduckdb
