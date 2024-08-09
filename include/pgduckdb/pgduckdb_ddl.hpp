extern "C" {
#include "postgres.h"
#include "nodes/nodes.h"
}
void duckdb_handle_ddl(Node *ParseTree, const char *queryString);
