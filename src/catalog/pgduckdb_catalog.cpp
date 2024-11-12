#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

PostgresCatalog::PostgresCatalog(duckdb::AttachedDatabase &db, const duckdb::string &connection_string,
                                 duckdb::AccessMode access_mode)
    : Catalog(db), path(connection_string), access_mode(access_mode) {
}

duckdb::unique_ptr<duckdb::Catalog>
PostgresCatalog::Attach(duckdb::StorageExtensionInfo *storage_info_p, duckdb::ClientContext &context,
                        duckdb::AttachedDatabase &db, const duckdb::string &name, duckdb::AttachInfo &info,
                        duckdb::AccessMode access_mode) {
	auto connection_string = info.path;
	return duckdb::make_uniq<PostgresCatalog>(db, connection_string, access_mode);
}

// ------------------ Catalog API ---------------------

void
PostgresCatalog::Initialize(bool load_builtin) {
	return;
}

duckdb::string
PostgresCatalog::GetCatalogType() {
	return "pgduckdb";
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresCatalog::CreateSchema(duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo &info) {
	throw duckdb::NotImplementedException("CreateSchema not supported yet");
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
PostgresCatalog::GetSchema(duckdb::CatalogTransaction transaction, const duckdb::string &schema_name,
                           duckdb::OnEntryNotFound if_not_found, duckdb::QueryErrorContext error_context) {
	auto &pg_transaction = transaction.transaction->Cast<PostgresTransaction>();
	auto res = pg_transaction.GetCatalogEntry(duckdb::CatalogType::SCHEMA_ENTRY, schema_name, "");
	D_ASSERT(res);
	D_ASSERT(res->type == duckdb::CatalogType::SCHEMA_ENTRY);
	return (duckdb::SchemaCatalogEntry *)res.get();
}

void
PostgresCatalog::ScanSchemas(duckdb::ClientContext &context,
                             std::function<void(duckdb::SchemaCatalogEntry &)> callback) {
	return;
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
PostgresCatalog::PlanCreateTableAs(duckdb::ClientContext &context, duckdb::LogicalCreateTable &op,
                                   duckdb::unique_ptr<duckdb::PhysicalOperator> plan) {
	throw duckdb::NotImplementedException("PlanCreateTableAs not supported yet");
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
PostgresCatalog::PlanInsert(duckdb::ClientContext &context, duckdb::LogicalInsert &op,
                            duckdb::unique_ptr<duckdb::PhysicalOperator> plan) {
	throw duckdb::NotImplementedException("PlanInsert not supported yet");
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
PostgresCatalog::PlanDelete(duckdb::ClientContext &context, duckdb::LogicalDelete &op,
                            duckdb::unique_ptr<duckdb::PhysicalOperator> plan) {
	throw duckdb::NotImplementedException("PlanDelete not supported yet");
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
PostgresCatalog::PlanUpdate(duckdb::ClientContext &context, duckdb::LogicalUpdate &op,
                            duckdb::unique_ptr<duckdb::PhysicalOperator> plan) {
	throw duckdb::NotImplementedException("PlanUpdate not supported yet");
}

duckdb::unique_ptr<duckdb::LogicalOperator>
PostgresCatalog::BindCreateIndex(duckdb::Binder &binder, duckdb::CreateStatement &stmt,
                                 duckdb::TableCatalogEntry &table, duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
	throw duckdb::NotImplementedException("BindCreateIndex not supported yet");
}

duckdb::DatabaseSize
PostgresCatalog::GetDatabaseSize(duckdb::ClientContext &context) {
	throw duckdb::NotImplementedException("GetDatabaseSize not supported yet");
}

bool
PostgresCatalog::InMemory() {
	return false;
}

duckdb::string
PostgresCatalog::GetDBPath() {
	return path;
}

void
PostgresCatalog::DropSchema(duckdb::ClientContext &context, duckdb::DropInfo &info) {
	throw duckdb::NotImplementedException("DropSchema not supported yet");
}

} // namespace pgduckdb
