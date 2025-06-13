#pragma once

#include "storage/ducklake_transaction.hpp"

namespace pgduckdb {

class PgDuckLakeTransaction : public duckdb::DuckLakeTransaction,
                              public std::enable_shared_from_this<PgDuckLakeTransaction> {
public:
	PgDuckLakeTransaction(duckdb::DuckLakeCatalog &ducklake_catalog, duckdb::TransactionManager &manager,
	                      duckdb::ClientContext &context);
	~PgDuckLakeTransaction() override {};

	void Start() override;
};

} // namespace pgduckdb
