#pragma once

#include "pgduckdb/pg_declarations.hpp"

namespace pgduckdb {
bool IsExtensionRegistered();
bool IsDuckdbOnlyFunction(Oid function_oid);
uint64 CacheVersion();
Oid ExtensionOid();
Oid DuckdbTableAmOid();
bool IsMotherDuckEnabled();
bool IsMotherDuckEnabledAnywhere();
bool IsMotherDuckPostgresDatabase();
Oid MotherDuckPostgresUser();
Oid IsDuckdbTable(Form_pg_class relation);
Oid IsDuckdbTable(Relation relation);
Oid IsMotherDuckTable(Form_pg_class relation);
Oid IsMotherDuckTable(Relation relation);
Oid IsDuckdbExecutionAllowed();
} // namespace pgduckdb
