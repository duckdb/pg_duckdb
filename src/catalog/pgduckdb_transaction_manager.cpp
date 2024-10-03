#include "pgduckdb/catalog/pgduckdb_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

PostgresTransactionManager::PostgresTransactionManager(AttachedDatabase &db_p, PostgresCatalog &catalog)
    : TransactionManager(db_p), catalog(catalog) {
}

Transaction &
PostgresTransactionManager::StartTransaction(ClientContext &context) {
	lock_guard<mutex> l(transaction_lock);
	auto transaction = make_uniq<PostgresTransaction>(*this, context, catalog, GetActiveSnapshot());
	auto &result = *transaction;
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData
PostgresTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void
PostgresTransactionManager::RollbackTransaction(Transaction &transaction) {
	return;
}

void
PostgresTransactionManager::Checkpoint(ClientContext &context, bool force) {
	return;
}

} // namespace duckdb
