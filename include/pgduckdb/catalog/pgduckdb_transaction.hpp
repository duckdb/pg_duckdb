#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "pgduckdb/catalog/pgduckdb_type.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"

namespace duckdb {

class PostgresCatalog;

class SchemaItems {
public:
	SchemaItems(unique_ptr<PostgresSchema> &&schema, const string &name) : name(name), schema(std::move(schema)) {
	}

public:
	optional_ptr<CatalogEntry> GetTable(const string &name, PlannerInfo *planner_info);
	optional_ptr<CatalogEntry> GetType(const string &name);

public:
	string name;
	unique_ptr<PostgresSchema> schema;
	case_insensitive_map_t<unique_ptr<PostgresTable>> tables;
	case_insensitive_map_t<unique_ptr<PostgresType>> types;
};

class PostgresTransaction : public Transaction {
public:
	PostgresTransaction(TransactionManager &manager, ClientContext &context, PostgresCatalog &catalog,
	                    Snapshot snapshot);
	~PostgresTransaction() override;

public:
	optional_ptr<CatalogEntry> GetCatalogEntry(CatalogType type, const string &schema, const string &name);

private:
	optional_ptr<CatalogEntry> GetSchema(const string &name);

private:
	case_insensitive_map_t<SchemaItems> schemas;
	PostgresCatalog &catalog;
	Snapshot snapshot;
};

} // namespace duckdb
