#pragma once

extern "C" {
#include "postgres.h"
#include "optimizer/planner.h"
}

extern bool duckdb_explain_analyze;

PlannedStmt *DuckdbPlanNode(Query *parse, int cursor_options, ParamListInfo bound_params);
