#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"

#include "pgduckdb/pgduckdb_options.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/scan/postgres_index_scan.hpp"
#include "pgduckdb/scan/postgres_seq_scan.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"

#include "utils/elog.h"
}
#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <pwd.h>
#include <fstream>

namespace pgduckdb {

static bool
CheckDataDirectory(const char *data_directory) {
	struct stat info;

	if (lstat(data_directory, &info) != 0) {
		if (errno == ENOENT) {
			elog(DEBUG2, "Directory `%s` doesn't exists.", data_directory);
			return false;
		} else if (errno == EACCES) {
			elog(ERROR, "Can't access `%s` directory.", data_directory);
		} else {
			elog(ERROR, "Other error when reading `%s`.", data_directory);
		}
	}

	if (!S_ISDIR(info.st_mode)) {
		elog(WARNING, "`%s` is not directory.", data_directory);
	}

	if (access(data_directory, R_OK | W_OK)) {
		elog(ERROR, "Directory `%s` permission problem.", data_directory);
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
			elog(ERROR, "Creating duckdb extensions directory failed with reason `%s`\n", strerror(error));
		}
		elog(DEBUG2, "Created %s as `duckdb.data_dir`", duckdb_extension_data_directory->data);
	};

	std::string duckdb_extension_directory(duckdb_extension_data_directory->data);
	pfree(duckdb_extension_data_directory->data);
	return duckdb_extension_directory;
}

Connection::~Connection() {
	auto &scans = context->db->config.replacement_scans;
	for (auto it = scans.begin(); it != scans.end(); ++it) {
		auto casted = dynamic_cast<PostgresReplacementScanData *>(it->data.get());
		if (casted && casted->id == tracked_replacement_scan_id) {
			scans.erase(it);
			break;
		}
	}
}

DuckDBManager::DuckDBManager() {
	elog(DEBUG2, "Creating DuckDB instance");

	duckdb::DBConfig config;
	config.SetOptionByName("extension_directory", GetExtensionDirectory());
	config.SetOptionByName("allow_unsigned_extensions", true);
	config.SetOptionByName("threads", 1);
	database = duckdb::make_uniq<duckdb::DuckDB>(nullptr, &config);

	auto connection = duckdb::make_uniq<duckdb::Connection>(*database);

	auto &context = *connection->context;
	LoadFunctions(context);
	LoadSecrets(context);
	LoadExtensions(context);
	ProcessRCFile(context);
}

std::string
FindHomeDirectory() {
	struct passwd *pwent;
	uid_t uid = getuid();
	if ((pwent = getpwuid(uid)) != NULL) {
		if (pwent->pw_dir) {
			return pwent->pw_dir;
		}
	}
	return getenv("HOME");
}

void
DuckDBManager::ProcessRCFile(duckdb::ClientContext &context) {
	std::string home = FindHomeDirectory();
	std::ifstream rc_file(home + "/.pg_duckrc");
	if (rc_file.fail()) {
		elog(DEBUG2, "No '.pg_duckrc' file found in home directory '%s'", home.c_str());
		return;
	}

	elog(DEBUG2, "Executing commands from '%s/.pg_duckrc' file", home.c_str());
	std::string line;
	while (std::getline(rc_file, line)) {
		auto res = context.Query(line, false);
		if (res->HasError()) {
			elog(WARNING, "Error executing '%s' from '.pg_duckrc' file: %s", line.c_str(), res->GetError().c_str());
		}
	}
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
		if (secret.endpoint.length() && !is_r2_cloud_secret) {
			appendStringInfo(secret_key, ", ENDPOINT '%s'", secret.endpoint.c_str());
		}
		if (is_r2_cloud_secret) {
			appendStringInfo(secret_key, ", ACCOUNT_ID '%s'", secret.endpoint.c_str());
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
				elog(ERROR, "Extension `%s` could not be loaded with DuckDB", extension.name.c_str());
			}
		}
		pfree(duckdb_extension->data);
	}
}

static void
UseDefaultDB(duckdb::Connection const &connection) {
	if (duckdb_default_db[0]) {
		// TODO: Escape the database name correctly
		pgduckdb::RunQuery(connection, "USE " + std::string(duckdb_default_db) + ";");
	}
}

duckdb::unique_ptr<duckdb::Connection>
DuckdbCreateSimpleConnection() {
	auto db = pgduckdb::DuckDBManager::Get().GetDatabase();
	auto connection = duckdb::make_uniq<duckdb::Connection>(db);
	UseDefaultDB(*connection);
	return connection;
}

duckdb::unique_ptr<Connection>
DuckdbCreateConnection(List *rtables, PlannerInfo *planner_info, List *needed_columns, const char *query) {
	auto &db = DuckDBManager::Get().GetDatabase();

	/* Add DuckDB replacement scan */
	auto &ref = db.instance->config.replacement_scans.emplace_back(
	    pgduckdb::PostgresReplacementScan,
	    duckdb::make_uniq_base<duckdb::ReplacementScanData, PostgresReplacementScanData>(rtables, planner_info,
	                                                                                     needed_columns, query));

	auto con = duckdb::make_uniq<Connection>(db);
	UseDefaultDB(*con);

	/* Store replacement scan identifier to be removed when connection is destroyed */
	con->SetTrackedReplacementScanId(static_cast<PostgresReplacementScanData *>(ref.data.get())->id);
	return con;
}

duckdb::unique_ptr<duckdb::QueryResult>
RunQuery(duckdb::Connection const &connection, const std::string &query) {
	auto result = connection.context->Query(query, false);
	if (result->HasError()) {
		auto err = result->GetError().c_str();
		ereport(ERROR,
		        (errmsg("received duckdb error: %s", err), errcontext_msg("while running query: %s", query.c_str())));
	}
	return result;
}

} // namespace pgduckdb
