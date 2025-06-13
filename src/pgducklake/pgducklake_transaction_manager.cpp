#include "pgducklake/pgducklake_transaction_manager.hpp"

#include "pgducklake/pgducklake_transaction.hpp"

namespace pgduckdb {

PgDuckLakeTransactionManager::PgDuckLakeTransactionManager(duckdb::AttachedDatabase &db_p,
                                                           duckdb::DuckLakeCatalog &ducklake_catalog)
    : duckdb::DuckLakeTransactionManager(db_p, ducklake_catalog), ducklake_catalog(ducklake_catalog) {
}

duckdb::Transaction &
PgDuckLakeTransactionManager::StartTransaction(duckdb::ClientContext &context) {
	auto transaction = duckdb::make_shared_ptr<PgDuckLakeTransaction>(ducklake_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	std::lock_guard<std::mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

duckdb::ErrorData
PgDuckLakeTransactionManager::CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<PgDuckLakeTransaction>();
	try {
		ducklake_transaction.Commit();
	} catch (std::exception &ex) {
		return duckdb::ErrorData(ex);
	}
	std::lock_guard<std::mutex> l(transaction_lock);
	transactions.erase(transaction);
	return duckdb::ErrorData();
}

void
PgDuckLakeTransactionManager::RollbackTransaction(duckdb::Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<PgDuckLakeTransaction>();
	ducklake_transaction.Rollback();
	std::lock_guard<std::mutex> l(transaction_lock);
	transactions.erase(transaction);
}

} // namespace pgduckdb
