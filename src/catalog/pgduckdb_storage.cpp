#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction_manager.hpp"

namespace duckdb {

static unique_ptr<TransactionManager>
CreateTransactionManager(StorageExtensionInfo *storage_info, AttachedDatabase &db, Catalog &catalog) {
	auto &pg_storage_info = *(reinterpret_cast<PostgresStorageExtensionInfo *>(storage_info));
	auto snapshot = pg_storage_info.snapshot;
	auto planner_info = pg_storage_info.planner_info;

	return make_uniq<PostgresTransactionManager>(db, catalog.Cast<PostgresCatalog>(), snapshot, planner_info);
}

PostgresStorageExtension::PostgresStorageExtension(Snapshot snapshot, PlannerInfo *planner_info) {
	attach = PostgresCatalog::Attach;
	create_transaction_manager = CreateTransactionManager;
	storage_info = make_uniq<PostgresStorageExtensionInfo>(snapshot, planner_info);
}

} // namespace duckdb
