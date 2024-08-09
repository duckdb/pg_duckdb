#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "pgduckdb/catalog/pgduckdb_catalog.hpp"

namespace duckdb {

PostgresTransaction::PostgresTransaction(TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
}

PostgresTransaction::~PostgresTransaction() {
}

} // namespace duckdb
