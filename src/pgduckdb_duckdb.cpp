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

	database = duckdb::make_uniq<duckdb::DuckDB>(nullptr, &config);
	duckdb::DBConfig::GetConfig(*database->instance).storage_extensions["pgduckdb"] =
	    duckdb::make_uniq<duckdb::PostgresStorageExtension>();
	duckdb::ExtensionInstallInfo extension_install_info;
	database->instance->SetExtensionLoaded("pgduckdb", extension_install_info);

	auto connection = duckdb::make_uniq<duckdb::Connection>(*database);

	auto &context = *connection->context;
	context.Query("ATTACH DATABASE 'pgduckdb' (TYPE pgduckdb)", false);

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
	    PostgresFunctionGuard(DirectFunctionCall1Coll, pg_sequence_last_value, InvalidOid, secret_table_seq_oid);
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
		std::ostringstream query;
		bool is_r2_cloud_secret = (secret.type.rfind("R2", 0) == 0);
		query << "CREATE SECRET pgduckb_secret_" << std::to_string(secret_id) << " ";
		query << "(TYPE " << secret.type << ", KEY_ID '" << secret.id << "', SECRET '" << secret.secret << "'";
		if (secret.region.length() && !is_r2_cloud_secret) {
			query << ", REGION '" << secret.region << "'";
		}
		if (secret.session_token.length() && !is_r2_cloud_secret) {
			query << ", SESSION_TOKEN '" << secret.session_token << "'";
		}
		if (secret.endpoint.length() && !is_r2_cloud_secret) {
			query << ", ENDPOINT '" << secret.endpoint << "'";
		}
		if (is_r2_cloud_secret) {
			query << ", ACCOUNT_ID '" << secret.endpoint << "'";
		}
		if (!secret.use_ssl) {
			query << ", USE_SSL 'FALSE'";
		}
		query << ");";

		DuckDBQueryOrThrow(context, query.str());

		++secret_id;
	}

	secret_table_num_rows = secret_id;
}

void
DuckDBManager::DropSecrets(duckdb::ClientContext &context) {
	for (auto secret_id = 0; secret_id < secret_table_num_rows; ++secret_id) {
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
	DuckDBQueryOrThrow(context, http_file_cache_set_dir_query);

	return connection;
}

} // namespace pgduckdb
