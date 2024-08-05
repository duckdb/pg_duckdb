#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction_manager.hpp"

namespace pgduckdb {

static duckdb::unique_ptr<duckdb::TransactionManager>
CreateTransactionManager(duckdb::StorageExtensionInfo *storage_info, duckdb::AttachedDatabase &db,
                         duckdb::Catalog &catalog) {
	return duckdb::make_uniq<duckdb::PostgresTransactionManager>(db, catalog.Cast<PostgresCatalog>());
}

PostgresStorageExtension::PostgresStorageExtension(Snapshot snapshot) {
	attach = PostgresCatalog::Attach;
	create_transaction_manager = CreateTransactionManager;
	storage_info = duckdb::make_uniq<PostgresStorageExtensionInfo>(snapshot);
}

} // namespace pgduckdb
