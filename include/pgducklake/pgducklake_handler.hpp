#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

bool IsDuckLakeTable(Relation relation);
bool IsDuckLakeTable(Oid oid);

} // namespace pgduckdb
