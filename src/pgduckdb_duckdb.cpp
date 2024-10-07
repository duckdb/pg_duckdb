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

extern "C" {
#include "postgres.h"

#include "utils/elog.h"
}
#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <filesystem>

namespace pgduckdb {

static bool
CheckDataDirectory(const std::string &data_directory) {
	struct stat info;

	std::ostringstream oss;
	if (lstat(data_directory.c_str(), &info) != 0) {
		if (errno == ENOENT) {
			elog(DEBUG2, "(PGDuckDB/CheckDataDirectory) Directory `%s` doesn't exists", data_directory.c_str());
			return false;
		}

		oss << "(PGDuckDB/CheckDataDirectory) ";
		if (errno == EACCES) {
			oss << "Can't access `" << data_directory << "` directory";
		} else {
			oss << "Other error when reading `" << data_directory << "`: (" << errno << ") " << strerror(errno);
		}

		throw std::runtime_error(oss.str());
	} else if (!S_ISDIR(info.st_mode)) {
		oss << "(PGDuckDB/CheckDataDirectory) `" << data_directory << "` is not directory";
		throw std::runtime_error(oss.str());
	} else if (access(data_directory.c_str(), R_OK | W_OK)) {
		oss << "(PGDuckDB/CheckDataDirectory) Directory `" << data_directory << "` permission problem";
		throw std::runtime_error(oss.str());
	}

	return true;
}

static std::string
GetExtensionDirectory() {
	std::ostringstream oss;
	oss << DataDir << "/duckdb_extensions";
	std::string data_directory = oss.str();

	if (!CheckDataDirectory(data_directory)) {
		std::filesystem::create_directories(data_directory);
		elog(DEBUG2, "(PGDuckDB/GetExtensionDirectory) Created %s as `duckdb.data_dir`", data_directory.c_str());
	}

	return data_directory;
}

DuckDBManager::DuckDBManager() : secret_table_num_rows(0), secret_table_current_seq(0) {
}

void
DuckDBManager::Initialize() {
	elog(DEBUG2, "(PGDuckDB/DuckDBManager) Creating DuckDB instance");

	duckdb::DBConfig config;
	config.SetOptionByName("extension_directory", GetExtensionDirectory());
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

	for (auto secret_id = 0; secret_id < secret_table_num_rows; secret_id++) {
		auto drop_secret_cmd = duckdb::StringUtil::Format("DROP SECRET pgduckb_secret_%d;", secret_id);
		auto res = context.Query(drop_secret_cmd, false);
		if (res->HasError()) {
			elog(ERROR, "(PGDuckDB/DropSecrets) secret `%d` could not be dropped.", secret_id);
		}
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
	auto& instance = Get();
	auto connection = duckdb::make_uniq<duckdb::Connection>(GetDatabase());
	if (instance.CheckSecretsSeq()) {
		auto &context = *connection->context;
		instance.DropSecrets(context);
		instance.LoadSecrets(context);
	}
	return connection;
}

} // namespace pgduckdb
