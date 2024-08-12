#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

void DuckdbInitBackgroundWorker(void);

namespace pgduckdb {

void SyncMotherDuckCatalogsWithPg(bool drop_with_cascade);
extern bool doing_motherduck_sync;

} // namespace pgduckdb
