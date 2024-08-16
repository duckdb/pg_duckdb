#pragma once

extern "C" {
#include "postgres.h"
#include "optimizer/planner.h"
}

PlannedStmt *DuckdbPlanNode(Query *parse, int cursor_options, ParamListInfo bound_params);
