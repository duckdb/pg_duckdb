#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction_manager.hpp"

namespace duckdb {

static unique_ptr<TransactionManager>
CreateTransactionManager(StorageExtensionInfo *storage_info, AttachedDatabase &db,
                         Catalog &catalog) {
	return make_uniq<PostgresTransactionManager>(db, catalog.Cast<PostgresCatalog>());
}

PostgresStorageExtension::PostgresStorageExtension(Snapshot snapshot, PlannerInfo *planner_info) {
	attach = PostgresCatalog::Attach;
	create_transaction_manager = CreateTransactionManager;
	storage_info = make_uniq<PostgresStorageExtensionInfo>(snapshot, planner_info);
}

} // namespace duckdb
