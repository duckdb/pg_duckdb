#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
#include "optimizer/optimizer.h"
}

namespace pgduckdb {

duckdb::unique_ptr<duckdb::DuckDB> DuckdbOpenDatabase();
duckdb::unique_ptr<duckdb::Connection> DuckdbCreateConnection(List *rtables, PlannerInfo *planner_info,
                                                              List *needed_columns, const char *query);

} // namespace pgduckdb
