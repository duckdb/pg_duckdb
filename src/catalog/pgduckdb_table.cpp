#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "pgduckdb/scan/postgres_seq_scan.hpp"
#include "pgduckdb/scan/postgres_index_scan.hpp"

extern "C" {
#include "postgres.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
#include "parser/parsetree.h"
}

namespace pgduckdb {

PostgresTable::PostgresTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Snapshot snapshot)
    : TableCatalogEntry(catalog, schema, info), snapshot(snapshot) {
}

bool
PostgresTable::PopulateColumns(CreateTableInfo &info, Oid relid, Snapshot snapshot) {
	auto rel = RelationIdGetRelation(relid);
	auto tupleDesc = RelationGetDescr(rel);

	if (!tupleDesc) {
		elog(ERROR, "Failed to get tuple descriptor for relation with OID %u", relid);
		RelationClose(rel);
		return false;
	}

	for (int i = 0; i < tupleDesc->natts; i++) {
		Form_pg_attribute attr = &tupleDesc->attrs[i];
		auto col_name = duckdb::string(NameStr(attr->attname));
		auto duck_type = ConvertPostgresToDuckColumnType(attr);
		info.columns.AddColumn(duckdb::ColumnDefinition(col_name, duck_type));
		/* Log column name and type */
		elog(DEBUG3, "-- (DuckDB/PostgresHeapBind) Column name: %s, Type: %s --", col_name.c_str(),
		     duck_type.ToString().c_str());
	}

	RelationClose(rel);
	return true;
}

//===--------------------------------------------------------------------===//
// PostgresHeapTable
//===--------------------------------------------------------------------===//

PostgresHeapTable::PostgresHeapTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                     Snapshot snapshot, Oid oid)
    : PostgresTable(catalog, schema, info, snapshot), oid(oid) {
}

unique_ptr<BaseStatistics>
PostgresHeapTable::GetStatistics(ClientContext &context, column_t column_id) {
	throw duckdb::NotImplementedException("GetStatistics not supported yet");
}

TableFunction
PostgresHeapTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// TODO: add cardinality
	bind_data = duckdb::make_uniq<PostgresSeqScanFunctionData>(0, oid, snapshot);
	return PostgresSeqScanFunction();
}

TableStorageInfo
PostgresHeapTable::GetStorageInfo(ClientContext &context) {
	throw duckdb::NotImplementedException("GetStorageInfo not supported yet");
}

//===--------------------------------------------------------------------===//
// PostgresIndexTable
//===--------------------------------------------------------------------===//

PostgresIndexTable::PostgresIndexTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                       Snapshot snapshot, Path *path, PlannerInfo *planner_info)
    : PostgresTable(catalog, schema, info, snapshot), path(path), planner_info(planner_info) {
}

unique_ptr<BaseStatistics>
PostgresIndexTable::GetStatistics(ClientContext &context, column_t column_id) {
	throw duckdb::NotImplementedException("GetStatistics not supported yet");
}

TableFunction
PostgresIndexTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// TODO: add cardinality
	RangeTblEntry *rte = planner_rt_fetch(path->parent->relid, planner_info);
	bind_data = duckdb::make_uniq<PostgresIndexScanFunctionData>(0, path, planner_info, rte->relid, snapshot);
	return PostgresIndexScanFunction();
}

TableStorageInfo
PostgresIndexTable::GetStorageInfo(ClientContext &context) {
	throw duckdb::NotImplementedException("GetStorageInfo not supported yet");
}

} // namespace pgduckdb
