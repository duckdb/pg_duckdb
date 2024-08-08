#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "optimizer/planner.h"
}

#include "pgduckdb/pgduckdb_duckdb.hpp"

extern bool duckdb_explain_analyze;

PlannedStmt *DuckdbPlanNode(Query *parse, int cursor_options, ParamListInfo bound_params);
std::tuple<duckdb::unique_ptr<duckdb::PreparedStatement>, duckdb::unique_ptr<duckdb::Connection>>
DuckdbPrepare(Query *query, ParamListInfo bound_params);
