//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/sqlite_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class PostgresTransaction : public Transaction {
public:
	PostgresTransaction(TransactionManager &manager, ClientContext &context);
	~PostgresTransaction() override;
};

} // namespace duckdb
