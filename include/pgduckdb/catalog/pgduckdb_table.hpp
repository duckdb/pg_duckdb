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

namespace duckdb {

class PostgresTable : public TableCatalogEntry {
public:
	virtual ~PostgresTable() {
	}

public:
	static bool PopulateHeapColumns(CreateTableInfo &info, Oid relid, Snapshot snapshot);
	static bool PopulateIndexColumns(CreateTableInfo &info, Oid relid, Path *path, bool indexonly);

protected:
	PostgresTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Cardinality cardinality,
	              Snapshot snapshot);

private:
	static TupleDesc ExecTypeFromTLWithNames(List *target_list, TupleDesc tuple_desc);

protected:
	Cardinality cardinality;
	Snapshot snapshot;
};

class PostgresHeapTable : public PostgresTable {
public:
	PostgresHeapTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Cardinality cardinality,
	                  Snapshot snapshot, Oid oid);

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
	PostgresIndexTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Cardinality cardinality,
	                   Snapshot snapshot, bool is_indexonly_scan, Path *path, PlannerInfo *planner_info, Oid oid);

public:
	// -- Table API --
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
	Path *path;
	PlannerInfo *planner_info;
	bool indexonly_scan;
	Oid oid;
};

} // namespace duckdb
