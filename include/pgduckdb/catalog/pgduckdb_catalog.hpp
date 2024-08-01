#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/snapshot.h"
}

namespace pgduckdb {

using duckdb::optional_ptr;
using duckdb::case_insensitive_map_t;
using duckdb::unique_ptr;
using duckdb::string;
using duckdb::CatalogEntry;
using duckdb::TableCatalogEntry;
using duckdb::SchemaCatalogEntry;
using duckdb::Catalog;
using duckdb::AttachedDatabase;
using duckdb::AccessMode;
using duckdb::AttachInfo;
using duckdb::StorageExtensionInfo;
using duckdb::ClientContext;
using duckdb::CreateSchemaInfo;
using duckdb::CatalogTransaction;
using duckdb::PhysicalOperator;
using duckdb::LogicalOperator;
using duckdb::CreateStatement;
using duckdb::LogicalInsert;
using duckdb::LogicalDelete;
using duckdb::LogicalUpdate;
using duckdb::LogicalCreateTable;
using duckdb::OnEntryNotFound;
using duckdb::QueryErrorContext;
using duckdb::Binder;
using duckdb::DropInfo;
using duckdb::DatabaseSize;

class PostgresCatalog : public Catalog {
public:
	PostgresCatalog(AttachedDatabase &db, const string &connection_string, AccessMode access_mode);
public:
	static unique_ptr<Catalog> Attach(StorageExtensionInfo *storage_info, ClientContext &context, AttachedDatabase &db, const string &name, AttachInfo &info, AccessMode access_mode);
public:
	string path;
	AccessMode access_mode;
public:
	// -- Catalog API --
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override;
	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction, const string &schema_name, OnEntryNotFound if_not_found, QueryErrorContext error_context = QueryErrorContext()) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) override;
	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;
	void DropSchema(ClientContext &context, DropInfo &info) override;
private:
	case_insensitive_map_t<unique_ptr<PostgresSchema>> schemas;
	Snapshot snapshot;
};

} // namespace pgduckdb
