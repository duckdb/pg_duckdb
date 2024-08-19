#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

extern "C" {
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
#include "executor/nodeIndexscan.h"
#include "nodes/pathnodes.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/rel.h"
}

namespace duckdb {

PostgresSchema::PostgresSchema(Catalog &catalog, CreateSchemaInfo &info, Snapshot snapshot, PlannerInfo *planner_info)
    : SchemaCatalogEntry(catalog, info), snapshot(snapshot), catalog(catalog), planner_info(planner_info) {
}

void
PostgresSchema::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	return;
}

void
PostgresSchema::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan(no context) not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info, TableCatalogEntry &table) {
	throw NotImplementedException("CreateIndex not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw NotImplementedException("CreateFunction not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw NotImplementedException("CreateTable not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("CreateView not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw NotImplementedException("CreateSequence not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) {
	throw NotImplementedException("CreateTableFunction not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) {
	throw NotImplementedException("CreateCopyFunction not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("CreatePragmaFunction not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw NotImplementedException("CreateCollation not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("CreateType not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::GetEntry(CatalogTransaction transaction, CatalogType type, const string &entry_name) {
	auto &pg_transaction = transaction.transaction->Cast<PostgresTransaction>();
	return pg_transaction.GetCatalogEntry(type, name, entry_name);
}

void
PostgresSchema::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DropEntry not supported yet");
}

void
PostgresSchema::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("Alter not supported yet");
}

} // namespace duckdb
