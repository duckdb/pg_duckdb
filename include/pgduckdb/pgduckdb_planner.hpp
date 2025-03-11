#pragma once

#include "duckdb.hpp"

#include "pgduckdb/pg/declarations.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

extern bool duckdb_explain_analyze;

PlannedStmt *DuckdbPlanNode(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params,
                            bool throw_error);
duckdb::unique_ptr<duckdb::PreparedStatement> DuckdbPrepare(const Query *query, bool allow_explain);
