#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

class SyncCatalogsWithPgExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;
};

struct PgCreateTableInfo : public CreateTableInfo {
	PgCreateTableInfo(unique_ptr<CreateTableInfo> info);

public:
	string ToString() const override;
};

} // namespace duckdb
