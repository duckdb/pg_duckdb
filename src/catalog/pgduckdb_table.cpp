#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "pgduckdb/scan/postgres_seq_scan.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
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
#include "postgres.h"
#include "executor/nodeIndexscan.h"
#include "nodes/pathnodes.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/rel.h"
}

namespace duckdb {

PostgresTable::PostgresTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                             Cardinality cardinality, Snapshot snapshot)
    : TableCatalogEntry(catalog, schema, info), cardinality(cardinality), snapshot(snapshot) {
}

bool
PostgresTable::PopulateHeapColumns(CreateTableInfo &info, Oid relid, Snapshot snapshot) {
	auto rel = RelationIdGetRelation(relid);
	auto tuple_desc = RelationGetDescr(rel);

	if (!tuple_desc) {
		elog(WARNING, "Failed to get tuple descriptor for relation with OID %u", relid);
		RelationClose(rel);
		return false;
	}

	for (int i = 0; i < tuple_desc->natts; i++) {
		Form_pg_attribute attr = &tuple_desc->attrs[i];
		auto col_name = duckdb::string(NameStr(attr->attname));
		auto duck_type = pgduckdb::ConvertPostgresToDuckColumnType(attr);
		info.columns.AddColumn(duckdb::ColumnDefinition(col_name, duck_type));
		/* Log column name and type */
		elog(DEBUG2, "(DuckDB/PopulateHeapColumns) Column name: %s, Type: %s --", col_name.c_str(),
		     duck_type.ToString().c_str());
	}

	RelationClose(rel);
	return true;
}

/*
 * Generate tuple descriptor from target_list with column names matching
 * relation original tuple descriptor.
 */
TupleDesc
PostgresTable::ExecTypeFromTLWithNames(List *target_list, TupleDesc tuple_desc) {
	TupleDesc type_info;
	ListCell *l;
	int len;
	int cur_resno = 1;

	len = ExecTargetListLength(target_list);
	type_info = CreateTemplateTupleDesc(len);

	foreach (l, target_list) {
		TargetEntry *tle = (TargetEntry *)lfirst(l);
		const Var *var = (Var *)tle->expr;

		TupleDescInitEntry(type_info, cur_resno, tuple_desc->attrs[var->varattno - 1].attname.data,
		                   exprType((Node *)tle->expr), exprTypmod((Node *)tle->expr), 0);

		TupleDescInitEntryCollation(type_info, cur_resno, exprCollation((Node *)tle->expr));
		cur_resno++;
	}

	return type_info;
}

bool
PostgresTable::PopulateIndexColumns(CreateTableInfo &info, Oid relid, Path *path, bool indexonly) {
	auto rel = RelationIdGetRelation(relid);
	auto tuple_desc = RelationGetDescr(rel);

	if (indexonly) {
		tuple_desc = ExecTypeFromTLWithNames(((IndexPath *)path)->indexinfo->indextlist, RelationGetDescr(rel));
		elog(DEBUG2, "(PostgresTable::PopulateIndexColumns) IndexOnlyScan selected");
	} else {
		tuple_desc = RelationGetDescr(rel);
		elog(DEBUG2, "(PostgresTable::PopulateIndexColumns) IndexScan selected");
	}

	if (!tuple_desc) {
		elog(WARNING, "Failed to get tuple descriptor for relation with OID %u", relid);
		RelationClose(rel);
		return false;
	}

	for (int i = 0; i < tuple_desc->natts; i++) {
		Form_pg_attribute attr = &tuple_desc->attrs[i];
		auto col_name = duckdb::string(NameStr(attr->attname));
		auto duck_type = pgduckdb::ConvertPostgresToDuckColumnType(attr);
		info.columns.AddColumn(duckdb::ColumnDefinition(col_name, duck_type));
		/* Log column name and type */
		elog(DEBUG2, "-- (DuckDB/PopulateIndexColumns) Column name: %s, Type: %s --", col_name.c_str(),
		     duck_type.ToString().c_str());
	}

	RelationClose(rel);
	return true;
}

//===--------------------------------------------------------------------===//
// PostgresHeapTable
//===--------------------------------------------------------------------===//

PostgresHeapTable::PostgresHeapTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                     Cardinality cardinality, Snapshot snapshot, Oid oid)
    : PostgresTable(catalog, schema, info, cardinality, snapshot), oid(oid) {
}

unique_ptr<BaseStatistics>
PostgresHeapTable::GetStatistics(ClientContext &context, column_t column_id) {
	throw duckdb::NotImplementedException("GetStatistics not supported yet");
}

TableFunction
PostgresHeapTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	bind_data = duckdb::make_uniq<pgduckdb::PostgresSeqScanFunctionData>(cardinality, oid, snapshot);
	return pgduckdb::PostgresSeqScanFunction();
}

TableStorageInfo
PostgresHeapTable::GetStorageInfo(ClientContext &context) {
	throw duckdb::NotImplementedException("GetStorageInfo not supported yet");
}

//===--------------------------------------------------------------------===//
// PostgresIndexTable
//===--------------------------------------------------------------------===//

PostgresIndexTable::PostgresIndexTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                       Cardinality cardinality, Snapshot snapshot, bool is_indexonly_scan, Path *path,
                                       PlannerInfo *planner_info, Oid oid)
    : PostgresTable(catalog, schema, info, cardinality, snapshot), path(path), planner_info(planner_info),
      indexonly_scan(is_indexonly_scan), oid(oid) {
}

unique_ptr<BaseStatistics>
PostgresIndexTable::GetStatistics(ClientContext &context, column_t column_id) {
	throw duckdb::NotImplementedException("GetStatistics not supported yet");
}

TableFunction
PostgresIndexTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	bind_data = duckdb::make_uniq<pgduckdb::PostgresIndexScanFunctionData>(cardinality, indexonly_scan, path,
	                                                                       planner_info, oid, snapshot);
	return pgduckdb::PostgresIndexScanFunction();
}

TableStorageInfo
PostgresIndexTable::GetStorageInfo(ClientContext &context) {
	throw duckdb::NotImplementedException("GetStorageInfo not supported yet");
}

} // namespace duckdb
