#pragma once

#include "storage/ducklake_catalog.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

class PgDuckLakeCatalog : public duckdb::DuckLakeCatalog {
public:
	PgDuckLakeCatalog(duckdb::AttachedDatabase &db_p, duckdb::DuckLakeOptions options_p)
	    : duckdb::DuckLakeCatalog(db_p, std::move(options_p)) {
	}

	void Initialize(duckdb::optional_ptr<duckdb::ClientContext> context, bool load_builtin) override;

	duckdb::optional_ptr<duckdb::SchemaCatalogEntry> LookupSchema(duckdb::CatalogTransaction transaction,
	                                                              const duckdb::EntryLookupInfo &schema_lookup,
	                                                              duckdb::OnEntryNotFound if_not_found) override;

private:
	std::mutex schemas_lock;
	//! Map of schema index -> schema
	std::unordered_map<duckdb::idx_t, duckdb::unique_ptr<duckdb::DuckLakeCatalogSet>> schemas;
};

} // namespace pgduckdb
