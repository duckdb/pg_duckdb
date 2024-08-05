#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"

extern "C" {
#include "postgres.h"
#include "utils/snapshot.h"
}

namespace pgduckdb {

using duckdb::BaseStatistics;
using duckdb::Catalog;
using duckdb::CatalogEntry;
using duckdb::CatalogType;
using duckdb::ClientContext;
using duckdb::column_t;
using duckdb::CreateTableInfo;
using duckdb::FunctionData;
using duckdb::optional_ptr;
using duckdb::SchemaCatalogEntry;
using duckdb::string;
using duckdb::TableCatalogEntry;
using duckdb::TableFunction;
using duckdb::TableStorageInfo;
using duckdb::unique_ptr;

class PostgresTable : public TableCatalogEntry {
public:
	PostgresTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Oid oid, Snapshot snapshot);

public:
	static bool PopulateColumns(CreateTableInfo &info, Oid relid, Snapshot snapshot);

public:
	// -- Table API --
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
	Oid oid;
	Snapshot snapshot;
};

} // namespace pgduckdb
