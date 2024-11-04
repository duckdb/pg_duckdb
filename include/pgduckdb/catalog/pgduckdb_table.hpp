#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "pgduckdb/pg_declarations.h"

namespace duckdb {

class PostgresTable : public TableCatalogEntry {
public:
	virtual ~PostgresTable();

public:
	static ::Relation OpenRelation(Oid relid);
	static void SetTableInfo(CreateTableInfo &info, ::Relation rel);
	static Cardinality GetTableCardinality(::Relation rel);

protected:
	PostgresTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, ::Relation rel,
	              Cardinality cardinality, Snapshot snapshot);

protected:
	::Relation rel;
	Cardinality cardinality;
	Snapshot snapshot;
};

class PostgresHeapTable : public PostgresTable {
public:
	PostgresHeapTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, ::Relation rel,
	                  Cardinality cardinality, Snapshot snapshot);

public:
	// -- Table API --
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
};

} // namespace duckdb
