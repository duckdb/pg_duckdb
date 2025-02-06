#pragma once

#include "pgduckdb/pg/declarations.hpp"

/* True if we are executing ALTER ... command */
extern bool in_duckdb_alter_table;
void DuckdbTruncateTable(Oid relation_oid);
void DuckdbInitUtilityHook();
