#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class PostgresTransactionManager : public TransactionManager {
public:
	PostgresTransactionManager(AttachedDatabase &db_p, PostgresCatalog &catalog, Snapshot snapshot, PlannerInfo *planner_info);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	PostgresCatalog &catalog;
	Snapshot snapshot;
	PlannerInfo *planner_info;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<Transaction>> transactions;
};

} // namespace duckdb
