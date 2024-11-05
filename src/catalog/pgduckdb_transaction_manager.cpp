#include "pgduckdb/catalog/pgduckdb_transaction_manager.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
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
	transactions.erase(transaction);
	return duckdb::ErrorData();
}

void
PostgresTransactionManager::RollbackTransaction(duckdb::Transaction &transaction) {
	duckdb::lock_guard<duckdb::mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void
PostgresTransactionManager::Checkpoint(duckdb::ClientContext &context, bool force) {
	return;
}

} // namespace pgduckdb
