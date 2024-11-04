#pragma once

#include "duckdb/transaction/transaction.hpp"

#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"

namespace pgduckdb {

class PostgresCatalog;

class SchemaItems {
public:
	SchemaItems(duckdb::unique_ptr<PostgresSchema> &&schema, const duckdb::string &name)
	    : name(name), schema(std::move(schema)) {
	}

	duckdb::optional_ptr<duckdb::CatalogEntry> GetTable(const duckdb::string &name);

	duckdb::optional_ptr<duckdb::CatalogEntry> GetSchema() const;

private:
	duckdb::string name;
	duckdb::unique_ptr<PostgresSchema> schema;
	duckdb::case_insensitive_map_t<duckdb::unique_ptr<PostgresTable>> tables;
};

class PostgresTransaction : public duckdb::Transaction {
public:
	PostgresTransaction(duckdb::TransactionManager &manager, duckdb::ClientContext &context, PostgresCatalog &catalog,
	                    Snapshot snapshot);
	~PostgresTransaction() override;

	duckdb::optional_ptr<duckdb::CatalogEntry> GetCatalogEntry(duckdb::CatalogType type, const duckdb::string &schema,
	                                                           const duckdb::string &name);

private:
	duckdb::optional_ptr<duckdb::CatalogEntry> GetSchema(const duckdb::string &name);

	duckdb::case_insensitive_map_t<SchemaItems> schemas;
	PostgresCatalog &catalog;
	Snapshot snapshot;
};

} // namespace pgduckdb
