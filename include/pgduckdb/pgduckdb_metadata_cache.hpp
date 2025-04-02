#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {
bool IsExtensionRegistered();
bool IsDuckdbOnlyFunction(Oid function_oid);
uint64_t CacheVersion();
Oid ExtensionOid();
Oid SchemaOid();
Oid DuckdbRowOid();
Oid DuckdbStructOid();
Oid DuckdbUnresolvedTypeOid();
Oid DuckdbUnionOid();
Oid DuckdbMapOid();
Oid DuckdbJsonOid();
Oid DuckdbTableAmOid();
Oid IsDuckdbTable(Form_pg_class relation);
Oid IsDuckdbTable(Relation relation);
Oid IsMotherDuckTable(Form_pg_class relation);
Oid IsMotherDuckTable(Relation relation);
Oid IsDuckdbExecutionAllowed();
void RequireDuckdbExecution();
} // namespace pgduckdb
