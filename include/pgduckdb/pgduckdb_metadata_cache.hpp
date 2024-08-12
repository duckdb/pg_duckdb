extern "C" {
#include "postgres.h"
}

namespace pgduckdb {
bool IsExtensionRegistered();
bool IsDuckdbOnlyFunction(Oid function_oid);
Oid DuckdbTableAmOid();
bool IsMotherDuckEnabled();
Oid MotherDuckPostgresUser();
} // namespace pgduckdb
