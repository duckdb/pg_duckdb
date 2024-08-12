extern "C" {
#include "postgres.h"
#include "nodes/nodes.h"

PGDLLEXPORT extern bool pgduckdb_forward_create_table;
}
void duckdb_handle_ddl(Node *ParseTree, const char *queryString);
