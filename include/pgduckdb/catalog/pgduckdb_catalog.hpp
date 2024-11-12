#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

class PostgresSchema;

class PostgresCatalog : public duckdb::Catalog {
public:
	PostgresCatalog(duckdb::AttachedDatabase &db, const duckdb::string &connection_string,
	                duckdb::AccessMode access_mode);

public:
	static duckdb::unique_ptr<duckdb::Catalog> Attach(duckdb::StorageExtensionInfo *storage_info,
	                                                  duckdb::ClientContext &context, duckdb::AttachedDatabase &db,
	                                                  const duckdb::string &name, duckdb::AttachInfo &info,
	                                                  duckdb::AccessMode access_mode);

public:
	duckdb::string path;
	duckdb::AccessMode access_mode;

public:
	// -- Catalog API --
	void Initialize(bool load_builtin) override;
	duckdb::string GetCatalogType() override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(duckdb::CatalogTransaction transaction,
	                                                        duckdb::CreateSchemaInfo &info) override;
	duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
	GetSchema(duckdb::CatalogTransaction transaction, const duckdb::string &schema_name,
	          duckdb::OnEntryNotFound if_not_found,
	          duckdb::QueryErrorContext error_context = duckdb::QueryErrorContext()) override;
	void ScanSchemas(duckdb::ClientContext &context,
	                 std::function<void(duckdb::SchemaCatalogEntry &)> callback) override;
	duckdb::unique_ptr<duckdb::PhysicalOperator>
	PlanCreateTableAs(duckdb::ClientContext &context, duckdb::LogicalCreateTable &op,
	                  duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;
	duckdb::unique_ptr<duckdb::PhysicalOperator> PlanInsert(duckdb::ClientContext &context, duckdb::LogicalInsert &op,
	                                                        duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;
	duckdb::unique_ptr<duckdb::PhysicalOperator> PlanDelete(duckdb::ClientContext &context, duckdb::LogicalDelete &op,
	                                                        duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;
	duckdb::unique_ptr<duckdb::PhysicalOperator> PlanUpdate(duckdb::ClientContext &context, duckdb::LogicalUpdate &op,
	                                                        duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;
	duckdb::unique_ptr<duckdb::LogicalOperator>
	BindCreateIndex(duckdb::Binder &binder, duckdb::CreateStatement &stmt, duckdb::TableCatalogEntry &table,
	                duckdb::unique_ptr<duckdb::LogicalOperator> plan) override;
	duckdb::DatabaseSize GetDatabaseSize(duckdb::ClientContext &context) override;
	bool InMemory() override;
	duckdb::string GetDBPath() override;
	void DropSchema(duckdb::ClientContext &context, duckdb::DropInfo &info) override;

private:
	duckdb::case_insensitive_map_t<duckdb::unique_ptr<PostgresSchema>> schemas;
};

} // namespace pgduckdb
