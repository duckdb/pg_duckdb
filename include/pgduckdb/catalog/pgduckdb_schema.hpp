#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/snapshot.h"
}

namespace pgduckdb {

using duckdb::optional_ptr;
using duckdb::unique_ptr;
using duckdb::string;
using duckdb::case_insensitive_map_t;
using duckdb::CatalogType;
using duckdb::CatalogEntry;
using duckdb::TableCatalogEntry;
using duckdb::SchemaCatalogEntry;
using duckdb::Catalog;
using duckdb::AttachedDatabase;
using duckdb::AccessMode;
using duckdb::AlterInfo;
using duckdb::ClientContext;
using duckdb::CreateSchemaInfo;
using duckdb::CreateTableFunctionInfo;
using duckdb::CreateSequenceInfo;
using duckdb::CreateViewInfo;
using duckdb::CreateFunctionInfo;
using duckdb::CreateTypeInfo;
using duckdb::CreateIndexInfo;
using duckdb::CreateCollationInfo;
using duckdb::CreateCopyFunctionInfo;
using duckdb::CreatePragmaFunctionInfo;
using duckdb::BoundCreateTableInfo;
using duckdb::CatalogTransaction;
using duckdb::DropInfo;

class PostgresSchema : public SchemaCatalogEntry {
public:
	PostgresSchema(Catalog &catalog, CreateSchemaInfo &info, Snapshot snapshot);
public:
	// -- Schema API --
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info, TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
private:
	case_insensitive_map_t<unique_ptr<PostgresTable>> tables;
	Snapshot snapshot;
	Catalog &catalog;
};

} // namespace pgduckdb
