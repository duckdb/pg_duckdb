#include "pgduckdb/catalog/pgduckdb_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

PostgresTransactionManager::PostgresTransactionManager(AttachedDatabase &db_p, PostgresCatalog &catalog, Snapshot snapshot, PlannerInfo *planner_info)
    : TransactionManager(db_p), catalog(catalog), snapshot(snapshot), planner_info(planner_info) {
}

Transaction &
PostgresTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<PostgresTransaction>(*this, context, catalog, snapshot, planner_info);
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
