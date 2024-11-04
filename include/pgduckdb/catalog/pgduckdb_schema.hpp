#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "pgduckdb/pg_declarations.hpp"

namespace pgduckdb {

class PostgresSchema : public duckdb::SchemaCatalogEntry {
public:
	PostgresSchema(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info, Snapshot snapshot);

public:
	// -- Schema API --
	void Scan(duckdb::ClientContext &context, duckdb::CatalogType type,
	          const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(duckdb::CatalogType type, const std::function<void(duckdb::CatalogEntry &)> &callback) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateIndex(duckdb::CatalogTransaction transaction,
	                                                       duckdb::CreateIndexInfo &info,
	                                                       duckdb::TableCatalogEntry &table) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateFunction(duckdb::CatalogTransaction transaction,
	                                                          duckdb::CreateFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateTable(duckdb::CatalogTransaction transaction,
	                                                       duckdb::BoundCreateTableInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateView(duckdb::CatalogTransaction transaction,
	                                                      duckdb::CreateViewInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateSequence(duckdb::CatalogTransaction transaction,
	                                                          duckdb::CreateSequenceInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateTableFunction(duckdb::CatalogTransaction transaction,
	                                                               duckdb::CreateTableFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateCopyFunction(duckdb::CatalogTransaction transaction,
	                                                              duckdb::CreateCopyFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreatePragmaFunction(duckdb::CatalogTransaction transaction,
	                                                                duckdb::CreatePragmaFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateCollation(duckdb::CatalogTransaction transaction,
	                                                           duckdb::CreateCollationInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateType(duckdb::CatalogTransaction transaction,
	                                                      duckdb::CreateTypeInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> GetEntry(duckdb::CatalogTransaction transaction,
	                                                    duckdb::CatalogType type, const duckdb::string &name) override;
	void DropEntry(duckdb::ClientContext &context, duckdb::DropInfo &info) override;
	void Alter(duckdb::CatalogTransaction transaction, duckdb::AlterInfo &info) override;

public:
	Snapshot snapshot;
	duckdb::Catalog &catalog;
};

} // namespace pgduckdb
