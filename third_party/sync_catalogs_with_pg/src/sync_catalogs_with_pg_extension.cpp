#include "duckdb/common/helper.hpp"
#define DUCKDB_EXTENSION_MAIN

#include "sync_catalogs_with_pg_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

extern "C" {
#undef ENABLE_NLS
#include "postgres.h"
#include "catalog/pg_authid_d.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "executor/spi.h"
#include "miscadmin.h"

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_ddl.hpp"

// Postgres overrides printf - we don't want that
#ifdef printf
#undef printf
#endif

} // extern "C"

namespace duckdb {

struct DuckDBTablesData : public GlobalTableFunctionState {
	DuckDBTablesData() : offset(0) {
	}

	vector<reference<CatalogEntry>> entries;
	idx_t offset;
};

static unique_ptr<FunctionData>
SyncCatalogsWithPgBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                       vector<string> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("comment");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState>
SyncCatalogsWithPgInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBTablesData>();

	// scan all the schemas for tables and collect themand collect them
	// TODO: support for multiple databases
	// auto schemas = Catalog::GetAllSchemas(context);
	auto schemas = Catalog::GetSchemas(context, duckdb_default_db);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::TABLE_ENTRY,
		                  [&](CatalogEntry &entry) { result->entries.push_back(entry); });
	};
	return std::move(result);
}

PgCreateTableInfo::PgCreateTableInfo(unique_ptr<CreateTableInfo> info) {
	CopyProperties(*info);
	table = info->table;
	columns = info->columns.Copy();
	for (auto &constraint : constraints) {
		constraints.push_back(constraint->Copy());
	}
}

string
PgCreateTableInfo::ToString() const {
	string ret = "";

	ret += "CREATE TABLE IF NOT EXISTS ";

	ret += KeywordHelper::WriteOptionallyQuoted(table);

	ret += TableCatalogEntry::ColumnsToSQL(columns, constraints);
	ret += " USING duckdb;";
	return ret;
}

void
SyncCatalogsWithPgFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBTablesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	pgduckdb_forward_create_table = false;
	SPI_connect();
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++].get();

		if (entry.type != CatalogType::TABLE_ENTRY) {
			continue;
		}
		auto &table = entry.Cast<TableCatalogEntry>();
		auto storage_info = table.GetStorageInfo(context);
		// return values:
		idx_t col = 0;
		// database_name, VARCHAR
		output.SetValue(col++, count, table.catalog.GetName());
		// schema_name, LogicalType::VARCHAR
		output.SetValue(col++, count, Value(table.schema.name));
		// table_name, LogicalType::VARCHAR
		output.SetValue(col++, count, Value(table.name));
		// comment, LogicalType::VARCHAR
		output.SetValue(col++, count, Value(table.comment));

		// sql, LogicalType::VARCHAR
		// unique_ptr<CreateInfo> table_info = table.GetInfo();
		auto table_info = unique_ptr_cast<CreateInfo, CreateTableInfo>(table.GetInfo());
		auto pg_table_info = make_uniq<PgCreateTableInfo>(std::move(table_info));
		pg_table_info->catalog = table.catalog.GetName();
		pg_table_info->schema = table.schema.name;
		output.SetValue(col++, count, Value(pg_table_info->ToString()));

		int ret = SPI_exec(pg_table_info->ToString().c_str(), 0);

		if (ret != SPI_OK_UTILITY)
			elog(ERROR, "SPI_execute failed: error code %d", ret);

		count++;
	}
	SPI_finish();
	pgduckdb_forward_create_table = true;
	output.SetCardinality(count);
}

static void
LoadInternal(DatabaseInstance &instance) {
	auto duckdb_tables_function = TableFunction("sync_catalogs_with_pg", {}, SyncCatalogsWithPgFunction,
	                                            SyncCatalogsWithPgBind, SyncCatalogsWithPgInit);
	ExtensionUtil::RegisterFunction(instance, duckdb_tables_function);
}

void
SyncCatalogsWithPgExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string
SyncCatalogsWithPgExtension::Name() {
	return "sync_catalogs_with_pg";
}

std::string
SyncCatalogsWithPgExtension::Version() const {
#ifdef EXT_VERSION_SYNC_CATALOGS_WITH_PG
	return EXT_VERSION_SYNC_CATALOGS_WITH_PG;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void
sync_catalogs_with_pg_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SyncCatalogsWithPgExtension>();
}

DUCKDB_EXTENSION_API const char *
sync_catalogs_with_pg_version() {
	printf("Initializing 'sync_catalogs_with_pg' extension -- sync_catalogs_with_pg_version\n");
	elog(NOTICE, "Initializing 'sync_catalogs_with_pg' extension -- sync_catalogs_with_pg_version\n");
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
