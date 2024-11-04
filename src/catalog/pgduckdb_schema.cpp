#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace pgduckdb {

PostgresSchema::PostgresSchema(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info, Snapshot snapshot)
    : SchemaCatalogEntry(catalog, info), snapshot(snapshot), catalog(catalog) {
}

void
PostgresSchema::Scan(duckdb::ClientContext &context, duckdb::CatalogType type,
                     const std::function<void(CatalogEntry &)> &callback) {
	return;
}

void
PostgresSchema::Scan(duckdb::CatalogType type, const std::function<void(duckdb::CatalogEntry &)> &callback) {
	throw duckdb::NotImplementedException("Scan(no context) not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::CreateIndex(duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo &info,
                            duckdb::TableCatalogEntry &table) {
	throw duckdb::NotImplementedException("CreateIndex not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::CreateFunction(duckdb::CatalogTransaction transaction, duckdb::CreateFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreateFunction not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::CreateTable(duckdb::CatalogTransaction transaction, duckdb::BoundCreateTableInfo &info) {
	throw duckdb::NotImplementedException("CreateTable not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::CreateView(duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo &info) {
	throw duckdb::NotImplementedException("CreateView not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::CreateSequence(duckdb::CatalogTransaction transaction, duckdb::CreateSequenceInfo &info) {
	throw duckdb::NotImplementedException("CreateSequence not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::CreateTableFunction(duckdb::CatalogTransaction transaction, duckdb::CreateTableFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreateTableFunction not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::CreateCopyFunction(duckdb::CatalogTransaction transaction, duckdb::CreateCopyFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreateCopyFunction not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::CreatePragmaFunction(duckdb::CatalogTransaction transaction, duckdb::CreatePragmaFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreatePragmaFunction not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::CreateCollation(duckdb::CatalogTransaction transaction, duckdb::CreateCollationInfo &info) {
	throw duckdb::NotImplementedException("CreateCollation not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::CreateType(duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo &info) {
	throw duckdb::NotImplementedException("CreateType not supported yet");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresSchema::GetEntry(duckdb::CatalogTransaction transaction, duckdb::CatalogType type,
                         const duckdb::string &entry_name) {
	auto &pg_transaction = transaction.transaction->Cast<PostgresTransaction>();
	return pg_transaction.GetCatalogEntry(type, name, entry_name);
}

void
PostgresSchema::DropEntry(duckdb::ClientContext &context, duckdb::DropInfo &info) {
	throw duckdb::NotImplementedException("DropEntry not supported yet");
}

void
PostgresSchema::Alter(duckdb::CatalogTransaction transaction, duckdb::AlterInfo &info) {
	throw duckdb::NotImplementedException("Alter not supported yet");
}

} // namespace pgduckdb
