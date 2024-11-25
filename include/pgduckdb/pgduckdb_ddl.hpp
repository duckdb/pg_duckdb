#pragma once

#include "pgduckdb/pg/declarations.hpp"

void DuckdbHandleDDL(Node *ParseTree);
void DuckdbTruncateTable(Oid relation_oid);
