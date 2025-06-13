#include "pgducklake/pgducklake_transaction.hpp"

#include "pgducklake/pgducklake_metadata_manager.hpp"

namespace pgduckdb {

PgDuckLakeTransaction::PgDuckLakeTransaction(duckdb::DuckLakeCatalog &ducklake_catalog,
                                             duckdb::TransactionManager &manager, duckdb::ClientContext &context)
    : duckdb::DuckLakeTransaction(ducklake_catalog, manager, context) {
	SetMetadataManager(duckdb::make_uniq<PgDuckLakeMetadataManager>(*this));
}

void
PgDuckLakeTransaction::Start() {
	// Manually call GetConnection() to ensure that the connection is created
	(void)GetConnection();
}

} // namespace pgduckdb
