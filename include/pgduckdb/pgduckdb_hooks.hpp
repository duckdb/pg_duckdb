#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {
bool IsAllowedStatement(Query *query, bool throw_error = false);
bool IsCatalogTable(Relation rel);
bool NeedsDuckdbExecution(Query *query);
bool ShouldTryToUseDuckdbExecution(Query *query);
} // namespace pgduckdb
