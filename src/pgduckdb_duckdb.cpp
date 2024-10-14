#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/extension_install_info.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/fmgrprotos.h"
}

#include "pgduckdb/pgduckdb_options.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/scan/postgres_seq_scan.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/catalog/pgduckdb_storage.hpp"

namespace pgduckdb {

DuckDBManager::DuckDBManager() {
}

void
DuckDBManager::Initialize() {
	elog(DEBUG2, "(PGDuckDB/DuckDBManager) Creating DuckDB instance");

	duckdb::DBConfig config;
	config.SetOptionByName("extension_directory", CreateOrGetDirectoryPath("duckdb_extensions"));
	// Transforms VIEWs into their view definition
	config.replacement_scans.emplace_back(pgduckdb::PostgresReplacementScan);

	const char *connection_string = nullptr;

	/*
	 * If MotherDuck is enabled, use it to connect to DuckDB. That way DuckDB
	 * its default database will be set to the default MotherDuck database.
	 */
	if (pgduckdb::IsMotherDuckEnabled()) {

		connection_string = psprintf("md:?motherduck_token=%s", duckdb_motherduck_token);
	}
	database = duckdb::make_uniq<duckdb::DuckDB>(connection_string, &config);
	duckdb::DBConfig::GetConfig(*database->instance).storage_extensions["pgduckdb"] =
	    duckdb::make_uniq<duckdb::PostgresStorageExtension>();
	duckdb::ExtensionInstallInfo extension_install_info;
	database->instance->SetExtensionLoaded("pgduckdb", extension_install_info);

	auto connection = duckdb::make_uniq<duckdb::Connection>(*database);

	auto &context = *connection->context;
	auto &db_manager = duckdb::DatabaseManager::Get(context);
	default_dbname = db_manager.GetDefaultDatabase(context);
	context.Query("ATTACH DATABASE 'pgduckdb' (TYPE pgduckdb)", false);
	auto result = context.Query("ATTACH DATABASE ':memory:' AS pg_temp;", false);
	if (result->HasError()) {
		elog(ERROR, "(PGDuckDB/DuckDBManager) could not attach pg_temp: %s", result->GetError().c_str());
	}

	LoadFunctions(context);
	LoadExtensions(context);
}

void
DuckDBManager::LoadFunctions(duckdb::ClientContext &context) {
	pgduckdb::PostgresSeqScanFunction seq_scan_fun;
	duckdb::CreateTableFunctionInfo seq_scan_info(seq_scan_fun);

	auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
	context.transaction.BeginTransaction();
	auto &instance = *database->instance;
	duckdb::ExtensionUtil::RegisterType(instance, "UnsupportedPostgresType", duckdb::LogicalTypeId::VARCHAR);
	catalog.CreateTableFunction(context, &seq_scan_info);
	context.transaction.Commit();
}

bool
DuckDBManager::CheckSecretsSeq() {
	Oid duckdb_namespace = get_namespace_oid("duckdb", false);
	Oid secret_table_seq_oid = get_relname_relid("secrets_table_seq", duckdb_namespace);
	int64 seq =
	    PostgresFunctionGuard<int64>(DirectFunctionCall1Coll, pg_sequence_last_value, InvalidOid, secret_table_seq_oid);
	if (secret_table_current_seq < seq) {
		secret_table_current_seq = seq;
		return true;
	}
	return false;
}

void
DuckDBManager::LoadSecrets(duckdb::ClientContext &context) {
	auto duckdb_secrets = ReadDuckdbSecrets();

	int secret_id = 0;
	for (auto &secret : duckdb_secrets) {
		StringInfo secret_key = makeStringInfo();
		bool is_r2_cloud_secret = (secret.type.rfind("R2", 0) == 0);
		appendStringInfo(secret_key, "CREATE SECRET pgduckb_secret_%d ", secret_id);
		appendStringInfo(secret_key, "(TYPE %s, KEY_ID '%s', SECRET '%s'", secret.type.c_str(), secret.id.c_str(),
		                 secret.secret.c_str());
		if (secret.region.length() && !is_r2_cloud_secret) {
			appendStringInfo(secret_key, ", REGION '%s'", secret.region.c_str());
		}
		if (secret.session_token.length() && !is_r2_cloud_secret) {
			appendStringInfo(secret_key, ", SESSION_TOKEN '%s'", secret.session_token.c_str());
		}
		if (secret.endpoint.length() && !is_r2_cloud_secret) {
			appendStringInfo(secret_key, ", ENDPOINT '%s'", secret.endpoint.c_str());
		}
		if (is_r2_cloud_secret) {
			appendStringInfo(secret_key, ", ACCOUNT_ID '%s'", secret.endpoint.c_str());
		}
		if (!secret.use_ssl) {
			appendStringInfo(secret_key, ", USE_SSL 'FALSE'");
		}
		appendStringInfo(secret_key, ");");
		DuckDBQueryOrThrow(context, secret_key->data);

		pfree(secret_key->data);
		secret_id++;
	}

	secret_table_num_rows = secret_id;
}

void
DuckDBManager::DropSecrets(duckdb::ClientContext &context) {
	for (auto secret_id = 0; secret_id < secret_table_num_rows; secret_id++) {
		auto drop_secret_cmd = duckdb::StringUtil::Format("DROP SECRET pgduckb_secret_%d;", secret_id);
		pgduckdb::DuckDBQueryOrThrow(drop_secret_cmd);
	}
	secret_table_num_rows = 0;
}

void
DuckDBManager::LoadExtensions(duckdb::ClientContext &context) {
	auto duckdb_extensions = ReadDuckdbExtensions();

	for (auto &extension : duckdb_extensions) {
		if (extension.enabled) {
			DuckDBQueryOrThrow(context, "LOAD " + extension.name);
		}
	}
}

duckdb::unique_ptr<duckdb::Connection>
DuckDBManager::CreateConnection() {
	auto &instance = Get();
	auto connection = duckdb::make_uniq<duckdb::Connection>(*instance.database);
	auto &context = *connection->context;
	if (instance.CheckSecretsSeq()) {
		instance.DropSecrets(context);
		instance.LoadSecrets(context);
	}

	auto http_file_cache_set_dir_query =
	    duckdb::StringUtil::Format("SET http_file_cache_dir TO '%s';", CreateOrGetDirectoryPath("duckdb_cache"));
	context.Query(http_file_cache_set_dir_query, false);

	return connection;
}

} // namespace pgduckdb
