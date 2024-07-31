#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
#include "optimizer/optimizer.h"
}

namespace pgduckdb {

extern duckdb::unique_ptr<duckdb::DuckDB> DuckdbOpenDatabase();
extern duckdb::unique_ptr<duckdb::Connection> DuckdbCreateConnection(List *rtables, PlannerInfo *plannerInfo,
                                                                     List *neededColumns, const char *query);

} // namespace pgduckdb