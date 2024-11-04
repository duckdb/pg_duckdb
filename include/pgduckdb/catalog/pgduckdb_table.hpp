#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "pgduckdb/pg_declarations.hpp"

namespace pgduckdb {

class PostgresTable : public duckdb::TableCatalogEntry {
public:
	virtual ~PostgresTable();

public:
	static Relation OpenRelation(Oid relid);
	static void SetTableInfo(duckdb::CreateTableInfo &info, Relation rel);
	static Cardinality GetTableCardinality(Relation rel);

protected:
	PostgresTable(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateTableInfo &info,
	              Relation rel, Cardinality cardinality, Snapshot snapshot);

protected:
	Relation rel;
	Cardinality cardinality;
	Snapshot snapshot;
};

class PostgresHeapTable : public PostgresTable {
public:
	PostgresHeapTable(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateTableInfo &info,
	                  Relation rel, Cardinality cardinality, Snapshot snapshot);

public:
	// -- Table API --
	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                         duckdb::column_t column_id) override;
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                      duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;
	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;
};

} // namespace pgduckdb
