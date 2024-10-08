extern "C" {
#include "postgres.h"
#include "nodes/nodes.h"
}
void DuckdbHandleDDL(Node *ParseTree, const char *queryString);
void DuckdbTruncateTable(Oid relation_oid);
