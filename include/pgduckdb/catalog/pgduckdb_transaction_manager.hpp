#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/reference_map.hpp"

#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"

namespace pgduckdb {

class PostgresTransactionManager : public duckdb::TransactionManager {
public:
	PostgresTransactionManager(duckdb::AttachedDatabase &db_p, PostgresCatalog &catalog);

	duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override;
	duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) override;
	void RollbackTransaction(duckdb::Transaction &transaction) override;

	void Checkpoint(duckdb::ClientContext &context, bool force = false) override;

private:
	PostgresCatalog &catalog;
	duckdb::mutex transaction_lock;
	duckdb::reference_map_t<duckdb::Transaction, duckdb::unique_ptr<duckdb::Transaction>> transactions;
};

} // namespace pgduckdb
