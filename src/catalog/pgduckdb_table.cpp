#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_process_lock.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "pgduckdb/scan/postgres_seq_scan.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "access/relation.h"   // relation_open and relation_close
#include "utils/rel.h"         // RelationGetDescr
#include "optimizer/plancat.h" // estimate_rel_size
#include "catalog/namespace.h" // makeRangeVarFromNameList
}

namespace pgduckdb {

PostgresTable::PostgresTable(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
                             duckdb::CreateTableInfo &info, Relation rel, Cardinality cardinality, Snapshot snapshot)
    : duckdb::TableCatalogEntry(catalog, schema, info), rel(rel), cardinality(cardinality), snapshot(snapshot) {
}

PostgresTable::~PostgresTable() {
	std::lock_guard<std::mutex> lock(DuckdbProcessLock::GetLock());

	PostgresFunctionGuard(relation_close, rel, NoLock);
}

Relation
PostgresTable::OpenRelation(Oid relid) {
	std::lock_guard<std::mutex> lock(DuckdbProcessLock::GetLock());
	/* We lock the tables as well just to be sure */
	return PostgresFunctionGuard(relation_open, relid, AccessShareLock);
}

void
PostgresTable::SetTableInfo(duckdb::CreateTableInfo &info, Relation rel) {
	auto tupleDesc = RelationGetDescr(rel);

	for (int i = 0; i < tupleDesc->natts; ++i) {
		Form_pg_attribute attr = &tupleDesc->attrs[i];
		auto col_name = duckdb::string(NameStr(attr->attname));
		auto duck_type = ConvertPostgresToDuckColumnType(attr);
		info.columns.AddColumn(duckdb::ColumnDefinition(col_name, duck_type));
		/* Log column name and type */
		elog(DEBUG2, "(DuckDB/SetTableInfo) Column name: %s, Type: %s --", col_name.c_str(),
		     duck_type.ToString().c_str());
	}
}

Cardinality
PostgresTable::GetTableCardinality(Relation rel) {
	Cardinality cardinality;
	BlockNumber n_pages;
	double allvisfrac;
	estimate_rel_size(rel, NULL, &n_pages, &cardinality, &allvisfrac);
	return cardinality;
}

//===--------------------------------------------------------------------===//
// PostgresHeapTable
//===--------------------------------------------------------------------===//

PostgresHeapTable::PostgresHeapTable(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
                                     duckdb::CreateTableInfo &info, Relation rel, Cardinality cardinality,
                                     Snapshot snapshot)
    : PostgresTable(catalog, schema, info, rel, cardinality, snapshot) {
}

duckdb::unique_ptr<duckdb::BaseStatistics>
PostgresHeapTable::GetStatistics(duckdb::ClientContext &context, duckdb::column_t column_id) {
	throw duckdb::NotImplementedException("GetStatistics not supported yet");
}

duckdb::TableFunction
PostgresHeapTable::GetScanFunction(duckdb::ClientContext &context,
                                   duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	bind_data = duckdb::make_uniq<PostgresSeqScanFunctionData>(rel, cardinality, snapshot);
	return PostgresSeqScanFunction();
}

duckdb::TableStorageInfo
PostgresHeapTable::GetStorageInfo(duckdb::ClientContext &context) {
	throw duckdb::NotImplementedException("GetStorageInfo not supported yet");
}

} // namespace pgduckdb
