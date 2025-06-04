#pragma once

#include "storage/ducklake_schema_entry.hpp"

namespace pgduckdb {

class PgDuckLakeSchemaEntry : public duckdb::DuckLakeSchemaEntry {
public:
	PgDuckLakeSchemaEntry(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info, duckdb::SchemaIndex schema_id,
	                      duckdb::string schema_uuid, duckdb::string data_path);

	duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntry(duckdb::CatalogTransaction transaction,
	                                                       const duckdb::EntryLookupInfo &lookup_info) override;

private:
	duckdb::optional_ptr<duckdb::CatalogEntry> LoadTableEntry(duckdb::CatalogTransaction transaction,
	                                                          const duckdb::EntryLookupInfo &lookup_info);

	duckdb::DuckLakeCatalogSet tables;
};

} // namespace pgduckdb
