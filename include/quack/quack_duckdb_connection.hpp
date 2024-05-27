#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
}

#include "quack/quack_heap_scan.hpp"

namespace quack {

extern duckdb::unique_ptr<duckdb::Connection> quack_create_duckdb_connection(List *tables, List *neededColumns,
                                                                             const char *query);

} // namespace quack