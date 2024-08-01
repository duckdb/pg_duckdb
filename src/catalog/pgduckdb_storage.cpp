#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/catalog/pgduckdb_catalog.hpp"

namespace pgduckdb {

PostgresStorageExtension::PostgresStorageExtension(Snapshot snapshot) {
	attach = PostgresCatalog::Attach;
	storage_info = duckdb::make_uniq<PostgresStorageExtensionInfo>(snapshot);
}

} // namespace pgduckdb
