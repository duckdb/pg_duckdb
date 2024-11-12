#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "pgduckdb/pg/declarations.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

class PostgresCatalog;
class PostgresSchema;
class PostgresTable;

void ClosePostgresRelations(duckdb::ClientContext &context);

class SchemaItems {
public:
	SchemaItems(duckdb::unique_ptr<PostgresSchema> &&schema, const duckdb::string &name);

	duckdb::optional_ptr<duckdb::CatalogEntry> GetTable(const duckdb::string &name);

	duckdb::optional_ptr<duckdb::CatalogEntry> GetSchema() const;

private:
	duckdb::string name;
	duckdb::unique_ptr<PostgresSchema> schema;
	duckdb::case_insensitive_map_t<duckdb::unique_ptr<PostgresTable>> tables;
};

class PostgresContextState : public duckdb::ClientContextState {
public:
	duckdb::case_insensitive_map_t<SchemaItems> schemas;
	void QueryEnd() override;
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

	PostgresCatalog &catalog;
	Snapshot snapshot;
};

} // namespace pgduckdb
