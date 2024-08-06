#pragma once

extern "C" {
#include "postgres.h"
#include "nodes/pathnodes.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/rel.h"
}

namespace pgduckdb {

Node *FixIndexQualOperand(Node *node, IndexOptInfo *index, int indexcol);
Node *FixIndexQualClause(PlannerInfo *root, IndexOptInfo *index, int indexcol, Node *clause, List *indexcolnos);

} // namespace pgduckdb
