#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace pgduckdb {

static duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(duckdb::StorageExtensionInfo *storage_info, duckdb::AttachedDatabase &db, duckdb::Catalog &catalog) {
	return make_uniq<duckdb::DuckTransactionManager>(db);
}

PostgresStorageExtension::PostgresStorageExtension(Snapshot snapshot) {
	attach = PostgresCatalog::Attach;
	create_transaction_manager = CreateTransactionManager;
	storage_info = duckdb::make_uniq<PostgresStorageExtensionInfo>(snapshot);
}

} // namespace pgduckdb
