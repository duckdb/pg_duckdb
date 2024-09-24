#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"

extern "C" {
#include "postgres.h"
#include "utils/fmgroids.h"
#include "fmgr.h"
#include "catalog/pg_namespace.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "access/genam.h"
#include "access/xact.h"
}

namespace duckdb {

PostgresCatalog::PostgresCatalog(AttachedDatabase &db, const string &connection_string, AccessMode access_mode)
    : Catalog(db), path(connection_string), access_mode(access_mode) {
}

unique_ptr<Catalog>
PostgresCatalog::Attach(StorageExtensionInfo *storage_info_p, ClientContext &context, AttachedDatabase &db,
                        const string &name, AttachInfo &info, AccessMode access_mode) {
	string connection_string = info.path;
	return make_uniq<PostgresCatalog>(db, connection_string, access_mode);
}

// ------------------ Catalog API ---------------------

void
PostgresCatalog::Initialize(bool load_builtin) {
	return;
}

string
PostgresCatalog::GetCatalogType() {
	return "pgduckdb";
}

optional_ptr<CatalogEntry>
PostgresCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("CreateSchema not supported yet");
}

optional_ptr<SchemaCatalogEntry>
PostgresCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name, OnEntryNotFound if_not_found,
                           QueryErrorContext error_context) {
	auto &pg_transaction = transaction.transaction->Cast<PostgresTransaction>();
	auto res = pg_transaction.GetCatalogEntry(CatalogType::SCHEMA_ENTRY, schema_name, "");
	D_ASSERT(res);
	D_ASSERT(res->type == CatalogType::SCHEMA_ENTRY);
	return (SchemaCatalogEntry *)res.get();
}

void
PostgresCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	return;
}

unique_ptr<PhysicalOperator>
PostgresCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op, unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("PlanCreateTableAs not supported yet");
}

unique_ptr<PhysicalOperator>
PostgresCatalog::PlanInsert(ClientContext &context, LogicalInsert &op, unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("PlanInsert not supported yet");
}

unique_ptr<PhysicalOperator>
PostgresCatalog::PlanDelete(ClientContext &context, LogicalDelete &op, unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("PlanDelete not supported yet");
}

unique_ptr<PhysicalOperator>
PostgresCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op, unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("PlanUpdate not supported yet");
}

unique_ptr<LogicalOperator>
PostgresCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                 unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("BindCreateIndex not supported yet");
}

DatabaseSize
PostgresCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("GetDatabaseSize not supported yet");
}

bool
PostgresCatalog::InMemory() {
	return false;
}

string
PostgresCatalog::GetDBPath() {
	return path;
}

void
PostgresCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DropSchema not supported yet");
}

} // namespace duckdb
