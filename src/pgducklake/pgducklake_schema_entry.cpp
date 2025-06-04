#include "pgducklake/pgducklake_schema_entry.hpp"

#include "storage/ducklake_field_data.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

#include "pgducklake/pgducklake_metadata_manager.hpp"
#include "pgducklake/pgducklake_catalog.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace pgduckdb {

PgDuckLakeSchemaEntry::PgDuckLakeSchemaEntry(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info,
                                             duckdb::SchemaIndex schema_id, duckdb::string schema_uuid,
                                             duckdb::string data_path)
    : DuckLakeSchemaEntry(catalog, info, schema_id, schema_uuid, data_path) {
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PgDuckLakeSchemaEntry::LookupEntry(duckdb::CatalogTransaction transaction, const duckdb::EntryLookupInfo &lookup_info) {
	if (!duckdb::DuckLakeSchemaEntry::CatalogTypeIsSupported(lookup_info.GetCatalogType())) {
		throw duckdb::NotImplementedException("Unsupported catalog type: %s", lookup_info.GetCatalogType());
	}

	auto entry = tables.GetEntry(lookup_info.GetEntryName());
	if (entry) {
		return entry;
	}

	if (lookup_info.GetCatalogType() == duckdb::CatalogType::TABLE_ENTRY) {
		return LoadTableEntry(transaction, lookup_info);
	}

	return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PgDuckLakeSchemaEntry::LoadTableEntry(duckdb::CatalogTransaction transaction,
                                      const duckdb::EntryLookupInfo &lookup_info) {
	duckdb::DuckLakeTableInfo table_info;
	auto &txn = transaction.transaction->Cast<duckdb::DuckLakeTransaction>();
	auto &metadata_manager = reinterpret_cast<PgDuckLakeMetadataManager &>(txn.GetMetadataManager());
	table_info.name = lookup_info.GetEntryName();

	bool found = metadata_manager.GetDuckLakeTableInfo(txn.GetSnapshot(), *this, table_info);
	if (!found) {
		return nullptr;
	}
	// Lazy metadata loading. Commented out as it requires upstream ducklake changes.
#if 0
	auto table_entry =
	    txn.GetCatalog().Cast<duckdb::DuckLakeCatalog>().TableEntryFromTableInfo(transaction, *this, table_info);
	tables.AddEntry(*this, table_info.id, std::move(table_entry));
	return tables.GetEntryById(table_info.id);
#endif
	return nullptr;
}

} // namespace pgduckdb
