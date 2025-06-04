#include "pgducklake/pgducklake_table.hpp"

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgducklake/pgducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <stdexcept>

extern "C" {
#include "postgres.h"

#include "utils/lsyscache.h"
#include "utils/rel.h"
}

namespace pgduckdb {

void
PgDuckLakeTable::CreateTable(Relation rel) {
	auto conn = DuckDBManager::Get().GetConnection(true);
	auto context = conn->context;

	auto &db_manager = duckdb::DatabaseInstance::GetDatabase(*context).GetDatabaseManager();
	auto ducklake_db = db_manager.GetDatabase(*context, "pgducklake");
	if (!ducklake_db) {
		throw std::runtime_error("PgDuckLakeTable::CreateTable: pgducklake database not found");
	}
	auto &ducklake_txn = duckdb::Transaction::Get(*context, *ducklake_db).Cast<PgDuckLakeTransaction>();
	auto &ducklake_catalog = ducklake_txn.GetCatalog();
	auto catalog_txn = ducklake_catalog.GetCatalogTransaction(*context);

	// Get schema entry
	char *nspname = get_namespace_name(rel->rd_rel->relnamespace);
	duckdb::EntryLookupInfo schema_lookup(duckdb::CatalogType::SCHEMA_ENTRY, nspname);
	auto schema_entry =
	    ducklake_catalog.LookupSchema(catalog_txn, schema_lookup, duckdb::OnEntryNotFound::RETURN_NULL);
	// Try create schema if not found
	if (!schema_entry) {
		duckdb::CreateSchemaInfo create_schema_info;
		create_schema_info.schema = nspname;
		create_schema_info.on_conflict= duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
		auto entry = ducklake_catalog.CreateSchema(catalog_txn, create_schema_info);
		schema_entry = entry ? &entry->Cast<duckdb::SchemaCatalogEntry>() : nullptr;
	}

	if (!schema_entry) {
		throw std::runtime_error("PgDuckLakeTable::CreateTable: schema '" + std::string(nspname) +
		                         "' creation failed, try again later");
	}

	// Create table
	duckdb::CreateTableInfo info(*schema_entry, NameStr(rel->rd_rel->relname));
	TupleDesc desc = RelationGetDescr(rel);
	for (int i = 0; i < desc->natts; i++) {
		auto attr = &desc->attrs[i];
		auto duck_type = pgduckdb::ConvertPostgresToDuckColumnType(attr);
		duckdb::ColumnDefinition column(NameStr(attr->attname), duck_type);
		info.columns.AddColumn(std::move(column));
		if (attr->atthasdef) {
			throw std::runtime_error("PgDuckLakeTable::CreateTable: default value not supported");
		}
	}

	duckdb::BoundCreateTableInfo table_info(*schema_entry, std::make_unique<duckdb::CreateTableInfo>(std::move(info)));
	schema_entry->CreateTable(catalog_txn, table_info);
}

} // namespace pgduckdb
