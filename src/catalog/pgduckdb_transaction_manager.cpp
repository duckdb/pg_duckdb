#include "pgduckdb/catalog/pgduckdb_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

PostgresTransactionManager::PostgresTransactionManager(AttachedDatabase &db_p, pgduckdb::PostgresCatalog &catalog)
    : TransactionManager(db_p), catalog(catalog) {
}

Transaction &
PostgresTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<PostgresTransaction>(*this, context);
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData
PostgresTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &postgres_transaction = transaction.Cast<PostgresTransaction>();
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
	auto &transaction = PostgresTransaction::Get(context, db.GetCatalog());
}

} // namespace duckdb
