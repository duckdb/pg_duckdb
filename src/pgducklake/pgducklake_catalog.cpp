#include "pgducklake/pgducklake_catalog.hpp"

#include "pgducklake/pgducklake_schema_entry.hpp"
#include "pgducklake/pgducklake_transaction.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace pgduckdb {

void
PgDuckLakeCatalog::Initialize(duckdb::optional_ptr<duckdb::ClientContext> /*context*/, bool /*load_builtin*/) {
	// do nothing, extension already created metadata tables for us.
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
PgDuckLakeCatalog::LookupSchema(duckdb::CatalogTransaction transaction, const duckdb::EntryLookupInfo &schema_lookup,
                                duckdb::OnEntryNotFound if_not_found) {
	return DuckLakeCatalog::LookupSchema(transaction, schema_lookup, if_not_found);
	// Lazy metadata loading. Commented out as it requires upstream ducklake changes.
#if 0
	auto &schema_name = schema_lookup.GetEntryName();
	auto at_clause = schema_lookup.GetAtClause();
	auto result = DuckLakeCatalog::LookupSchema(transaction, schema_lookup, if_not_found);
	if (!result) {
		return nullptr;
	}

	// TODO handle transaction local schema

	// wrap the schema entry in a PgDuckLakeSchemaEntry
	auto &duck_transaction = transaction.transaction->Cast<PgDuckLakeTransaction>();
	auto snapshot = duck_transaction.GetSnapshot(at_clause);
	std::lock_guard<std::mutex> guard(schemas_lock);
	if (schemas.find(snapshot.schema_version) == schemas.end()) {
		schemas.insert(
		    duckdb::make_pair(snapshot.schema_version, std::move(duckdb::make_uniq<duckdb::DuckLakeCatalogSet>())));
	}
	auto &entry = *schemas.find(snapshot.schema_version)->second;

	duckdb::DuckLakeSchemaEntry &ducklake_schema_entry = result->Cast<duckdb::DuckLakeSchemaEntry>();
	auto info = result->GetInfo();
	duckdb::CreateSchemaInfo schema_info;
	schema_info.schema = info->schema;
	auto schema_entry = duckdb::make_uniq<PgDuckLakeSchemaEntry>(
	    ducklake_schema_entry.ParentCatalog(), schema_info, ducklake_schema_entry.GetSchemaId(),
	    ducklake_schema_entry.GetSchemaUUID(), ducklake_schema_entry.DataPath());
	entry.CreateEntry(std::move(schema_entry));
	return entry.GetEntry<duckdb::SchemaCatalogEntry>(schema_name);
#endif
}

} // namespace pgduckdb
