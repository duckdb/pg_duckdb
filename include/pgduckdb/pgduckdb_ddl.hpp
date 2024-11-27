#pragma once

#include "pgduckdb/pg/declarations.hpp"

void DuckdbTruncateTable(Oid relation_oid);
void DuckdbInitUtilityHook();
