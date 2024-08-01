#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
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
}

namespace pgduckdb {

PostgresSchema::PostgresSchema(Catalog &catalog, CreateSchemaInfo &info, Snapshot snapshot) : SchemaCatalogEntry(catalog, info), snapshot(snapshot), catalog(catalog) {}

void PostgresSchema::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw duckdb::NotImplementedException("Scan(with context) not supported yet");
}

void PostgresSchema::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw duckdb::NotImplementedException("Scan(no context) not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info, TableCatalogEntry &table) {
	throw duckdb::NotImplementedException("CreateIndex not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreateFunction not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw duckdb::NotImplementedException("CreateTable not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw duckdb::NotImplementedException("CreateView not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw duckdb::NotImplementedException("CreateSequence not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreateTableFunction not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreateCopyFunction not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreatePragmaFunction not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw duckdb::NotImplementedException("CreateCollation not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw duckdb::NotImplementedException("CreateType not supported yet");
}

optional_ptr<CatalogEntry> PostgresSchema::GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) {
	if (type != CatalogType::TABLE_ENTRY) {
		throw duckdb::NotImplementedException("GetEntry (type: %s) not supported yet", duckdb::EnumUtil::ToString(type));
	}

	auto it = tables.find(name);
	if (it != tables.end()) {
		return it->second.get();
	}

	RangeVar *table_range_var = makeRangeVarFromNameList(stringToQualifiedNameList(name.c_str(), NULL));
	Oid rel_oid = RangeVarGetRelid(table_range_var, AccessShareLock, true);
	if (rel_oid == InvalidOid) {
		// Table could not be found
		return nullptr;
	}

	CreateTableInfo info;
	info.table = name;
	if (!PostgresTable::PopulateColumns(info, rel_oid, snapshot)) {
		return nullptr;
	}
	tables[name] = duckdb::make_uniq<PostgresTable>(catalog, *this, info, rel_oid, snapshot);
	return tables[name].get();
}

void PostgresSchema::DropEntry(ClientContext &context, DropInfo &info) {
	throw duckdb::NotImplementedException("DropEntry not supported yet");
}

void PostgresSchema::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw duckdb::NotImplementedException("Alter not supported yet");
}

} // namespace pgduckdb
