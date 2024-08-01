#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "pgduckdb/scan/postgres_seq_scan.hpp"

extern "C" {
#include "postgres.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
}

namespace pgduckdb {

PostgresTable::PostgresTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Oid oid, Snapshot snapshot) : TableCatalogEntry(catalog, schema, info), oid(oid), snapshot(snapshot) {}

bool PostgresTable::PopulateColumns(CreateTableInfo &info, Oid relid, Snapshot snapshot) {
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

unique_ptr<BaseStatistics> PostgresTable::GetStatistics(ClientContext &context, column_t column_id) {
	throw duckdb::NotImplementedException("GetStatistics not supported yet");
}

TableFunction PostgresTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// TODO: add cardinality
	bind_data = duckdb::make_uniq<PostgresSeqScanFunctionData>(0, oid, snapshot);
	return PostgresSeqScanFunction();
}

TableStorageInfo PostgresTable::GetStorageInfo(ClientContext &context) {
	throw duckdb::NotImplementedException("GetStorageInfo not supported yet");
}

} // namespace pgduckdb
