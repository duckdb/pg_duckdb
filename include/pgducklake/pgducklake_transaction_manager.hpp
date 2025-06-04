#pragma once

#include "storage/ducklake_transaction_manager.hpp"

#include "pgducklake/pgducklake_transaction.hpp"

namespace pgduckdb {

class PgDuckLakeTransactionManager : public duckdb::DuckLakeTransactionManager {
public:
	PgDuckLakeTransactionManager(duckdb::AttachedDatabase &db_p, duckdb::DuckLakeCatalog &ducklake_catalog);

	duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override;
	duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) override;
	void RollbackTransaction(duckdb::Transaction &transaction) override;

private:
	duckdb::DuckLakeCatalog &ducklake_catalog;
	std::mutex transaction_lock;
	duckdb::reference_map_t<duckdb::Transaction, duckdb::shared_ptr<PgDuckLakeTransaction>> transactions;
};

} // namespace pgduckdb
