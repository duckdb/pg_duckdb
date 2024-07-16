#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
#include "optimizer/optimizer.h"
}

namespace quack {

extern duckdb::unique_ptr<duckdb::DuckDB> quack_open_database();
extern duckdb::unique_ptr<duckdb::Connection> quack_create_duckdb_connection(List *rtables, PlannerInfo *plannerInfo,
                                                                             List *neededColumns, const char *query);

} // namespace quack