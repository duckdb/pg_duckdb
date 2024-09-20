#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"

#include "pgduckdb/pgduckdb_options.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/scan/postgres_index_scan.hpp"
#include "pgduckdb/scan/postgres_seq_scan.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

namespace pgduckdb {

static bool
CheckDataDirectory(const char *data_directory) {
	struct stat info;

	if (lstat(data_directory, &info) != 0) {
		if (errno == ENOENT) {
			elog(DEBUG2, "(PGDuckDB/CheckDataDirectory) Directory `%s` doesn't exists", data_directory);
			return false;
		} else if (errno == EACCES) {
			elog(ERROR, "(PGDuckDB/CheckDataDirectory) Can't access `%s` directory", data_directory);
		} else {
			elog(ERROR, "(PGDuckDB/CheckDataDirectory) Other error when reading `%s`", data_directory);
		}
	}

	if (!S_ISDIR(info.st_mode)) {
		elog(WARNING, "(PGDuckDB/CheckDataDirectory) `%s` is not directory", data_directory);
	}

	if (access(data_directory, R_OK | W_OK)) {
		elog(ERROR, "(PGDuckDB/CheckDataDirectory) Directory `%s` permission problem", data_directory);
	}

	return true;
}

static std::string
GetExtensionDirectory() {
	StringInfo duckdb_extension_data_directory = makeStringInfo();
	appendStringInfo(duckdb_extension_data_directory, "%s/duckdb_extensions", DataDir);

	if (!CheckDataDirectory(duckdb_extension_data_directory->data)) {
		if (mkdir(duckdb_extension_data_directory->data, S_IRWXU | S_IRWXG | S_IRWXO) == -1) {
			int error = errno;
			pfree(duckdb_extension_data_directory->data);
			elog(ERROR,
			     "(PGDuckDB/GetExtensionDirectory) Creating duckdb extensions directory failed with reason `%s`\n",
			     strerror(error));
		}
		elog(DEBUG2, "(PGDuckDB/GetExtensionDirectory) Created %s as `duckdb.data_dir`",
		     duckdb_extension_data_directory->data);
	};

	std::string duckdb_extension_directory(duckdb_extension_data_directory->data);
	pfree(duckdb_extension_data_directory->data);
	return duckdb_extension_directory;
}

DuckDBManager::DuckDBManager() {
	elog(DEBUG2, "(PGDuckDB/DuckDBManager) Creating DuckDB instance");

	duckdb::DBConfig config;
	config.SetOptionByName("extension_directory", GetExtensionDirectory());
	config.replacement_scans.emplace_back(pgduckdb::PostgresReplacementScan);
	database = duckdb::make_uniq<duckdb::DuckDB>(nullptr, &config);

	auto connection = duckdb::make_uniq<duckdb::Connection>(*database);

	auto &context = *connection->context;
	LoadFunctions(context);
	LoadSecrets(context);
	LoadExtensions(context);
}

void
DuckDBManager::LoadFunctions(duckdb::ClientContext &context) {
	pgduckdb::PostgresSeqScanFunction seq_scan_fun;
	duckdb::CreateTableFunctionInfo seq_scan_info(seq_scan_fun);

	pgduckdb::PostgresIndexScanFunction index_scan_fun;
	duckdb::CreateTableFunctionInfo index_scan_info(index_scan_fun);

	auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
	context.transaction.BeginTransaction();
	auto &instance = *database->instance;
	duckdb::ExtensionUtil::RegisterType(instance, "UnsupportedPostgresType", duckdb::LogicalTypeId::VARCHAR);
	catalog.CreateTableFunction(context, &seq_scan_info);
	catalog.CreateTableFunction(context, &index_scan_info);
	context.transaction.Commit();
}

void
DuckDBManager::LoadSecrets(duckdb::ClientContext &context) {
	auto duckdb_secrets = ReadDuckdbSecrets();

	int secret_id = 0;
	for (auto &secret : duckdb_secrets) {
		StringInfo secret_key = makeStringInfo();
		bool is_r2_cloud_secret = (secret.type.rfind("R2", 0) == 0);
		appendStringInfo(secret_key, "CREATE SECRET duckdbSecret_%d ", secret_id);
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
		context.Query(secret_key->data, false);
		pfree(secret_key->data);
		secret_id++;
	}
}

void
DuckDBManager::LoadExtensions(duckdb::ClientContext &context) {
	auto duckdb_extensions = ReadDuckdbExtensions();

	for (auto &extension : duckdb_extensions) {
		StringInfo duckdb_extension = makeStringInfo();
		if (extension.enabled) {
			appendStringInfo(duckdb_extension, "LOAD %s;", extension.name.c_str());
			auto res = context.Query(duckdb_extension->data, false);
			if (res->HasError()) {
				elog(ERROR, "(PGDuckDB/LoadExtensions) `%s` could not be loaded with DuckDB", extension.name.c_str());
			}
		}
		pfree(duckdb_extension->data);
	}
}

duckdb::unique_ptr<duckdb::Connection>
DuckdbCreateConnection(List *rtables, PlannerInfo *planner_info, List *needed_columns, const char *query) {
	auto &db = DuckDBManager::Get().GetDatabase();
	/* Add DuckDB replacement scan to read PG data */
	auto con = duckdb::make_uniq<duckdb::Connection>(db);
	con->context->registered_state->Insert(
	    "postgres_scan", duckdb::make_shared_ptr<PostgresReplacementScanDataClientContextState>(rtables, planner_info,
	                                                                                            needed_columns, query));
	return con;
}

} // namespace pgduckdb
