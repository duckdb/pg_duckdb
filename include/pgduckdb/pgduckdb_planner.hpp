#pragma once

extern "C" {
#include "postgres.h"
#include "optimizer/planner.h"
}

PlannedStmt *duckdb_plan_node(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams);