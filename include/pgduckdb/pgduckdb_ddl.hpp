#pragma once

#include "pgduckdb/pg/declarations.hpp"

void DuckdbHandleDDL(Node *ParseTree, const char *queryString);
void DuckdbTruncateTable(Oid relation_oid);
