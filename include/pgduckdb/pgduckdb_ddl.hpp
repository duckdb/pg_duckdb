#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {
/* True if we are executing ALTER ... command */
extern bool in_duckdb_alter_table;
} // namespace pgduckdb

void DuckdbTruncateTable(Oid relation_oid);
void DuckdbInitUtilityHook();
