#include "pgduckdb/catalog/pgduckdb_transaction_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "pgduckdb/pgduckdb_process_lock.hpp"

#include "duckdb/main/attached_database.hpp"

extern "C" {
#include "postgres.h"
#include "utils/snapmgr.h" // GetActiveSnapshot
}

namespace pgduckdb {

PostgresTransactionManager::PostgresTransactionManager(duckdb::AttachedDatabase &db_p, PostgresCatalog &catalog)
    : TransactionManager(db_p), catalog(catalog) {
}

duckdb::Transaction &
PostgresTransactionManager::StartTransaction(duckdb::ClientContext &context) {
	auto transaction = duckdb::make_uniq<PostgresTransaction>(*this, context, catalog, GetActiveSnapshot());
	auto &result = *transaction;
	duckdb::lock_guard<duckdb::mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

duckdb::ErrorData
PostgresTransactionManager::CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) {
	duckdb::lock_guard<duckdb::mutex> l(transaction_lock);
	ClosePostgresRelations(context);
	transactions.erase(transaction);
	return duckdb::ErrorData();
}

void
PostgresTransactionManager::RollbackTransaction(duckdb::Transaction &transaction) {
	duckdb::lock_guard<duckdb::mutex> l(transaction_lock);
	duckdb::shared_ptr<duckdb::ClientContext> context = transaction.context.lock();
	if (context) {
		ClosePostgresRelations(*context);
	}
	transactions.erase(transaction);
}

void
PostgresTransactionManager::Checkpoint(duckdb::ClientContext &context, bool force) {
	return;
}

} // namespace pgduckdb
