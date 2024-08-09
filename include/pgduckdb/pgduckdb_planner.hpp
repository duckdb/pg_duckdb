#pragma once

extern "C" {
#include "postgres.h"
#include "optimizer/planner.h"
}

PlannedStmt *DuckdbPlanNode(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params);

PlannerInfo *PlanQuery(Query *parse, ParamListInfo bound_params);
