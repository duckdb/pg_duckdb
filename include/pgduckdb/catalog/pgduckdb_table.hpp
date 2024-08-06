#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"

extern "C" {
#include "postgres.h"
#include "utils/snapshot.h"
#include "postgres.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
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
	virtual ~PostgresTable() {}
public:
	static bool PopulateColumns(CreateTableInfo &info, Oid relid, Snapshot snapshot);
protected:
	PostgresTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Snapshot snapshot);
protected:
	Snapshot snapshot;
};

class PostgresHeapTable : public PostgresTable {
public:
	PostgresHeapTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Snapshot snapshot, Oid oid);

public:
	// -- Table API --
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
private:
	Oid oid;
};

class PostgresIndexTable : public PostgresTable {
public:
	PostgresIndexTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Snapshot snapshot, Path *path, PlannerInfo *planner_info);

public:
	// -- Table API --
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
private:
	Path *path;
	PlannerInfo *planner_info;
};

} // namespace pgduckdb
