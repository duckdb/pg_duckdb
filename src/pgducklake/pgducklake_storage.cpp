#include "pgducklake/pgducklake_storage.hpp"

#include "pgducklake/pgducklake_catalog.hpp"
#include "pgducklake/pgducklake_transaction_manager.hpp"

namespace pgduckdb {

static duckdb::unique_ptr<duckdb::Catalog>
DuckLakeAttach(duckdb::StorageExtensionInfo *storage_info, duckdb::ClientContext &context, duckdb::AttachedDatabase &db,
               const duckdb::string &name, duckdb::AttachInfo &info, duckdb::AccessMode access_mode) {
	duckdb::DuckLakeOptions options;
	options.metadata_path = info.path;
	options.data_path = "/tmp/ducklake/";
	options.metadata_schema = "duckdb";
	options.encryption = duckdb::DuckLakeEncryption::UNENCRYPTED;
	options.access_mode = access_mode;
	return duckdb::make_uniq<PgDuckLakeCatalog>(db, std::move(options));
}

static duckdb::unique_ptr<duckdb::TransactionManager>
DuckLakeCreateTransactionManager(duckdb::StorageExtensionInfo *storage_info, duckdb::AttachedDatabase &db,
                                 duckdb::Catalog &catalog) {
	auto &ducklake_catalog = catalog.Cast<PgDuckLakeCatalog>();
	return duckdb::make_uniq<PgDuckLakeTransactionManager>(db, ducklake_catalog);
}

PgDuckLakeStorageExtension::PgDuckLakeStorageExtension() {
	attach = DuckLakeAttach;
	create_transaction_manager = DuckLakeCreateTransactionManager;
}

} // namespace pgduckdb
