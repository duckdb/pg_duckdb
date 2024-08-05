#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/extension_install_info.hpp"

#include "pgduckdb/pgduckdb_options.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/scan/postgres_index_scan.hpp"
#include "pgduckdb/scan/postgres_seq_scan.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/catalog/pgduckdb_storage.hpp"

#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

namespace pgduckdb {

static bool
duckdbCheckDataDirectory(const char *dataDirectory) {
	struct stat info;

	if (lstat(dataDirectory, &info) != 0) {
		if (errno == ENOENT) {
			elog(DEBUG2, "Directory `%s` doesn't exists.", dataDirectory);
			return false;
		} else if (errno == EACCES) {
			elog(ERROR, "Can't access `%s` directory.", dataDirectory);
		} else {
			elog(ERROR, "Other error when reading `%s`.", dataDirectory);
		}
	}

	if (!S_ISDIR(info.st_mode)) {
		elog(WARNING, "`%s` is not directory.", dataDirectory);
	}

	if (access(dataDirectory, R_OK | W_OK)) {
		elog(ERROR, "Directory `%s` permission problem.", dataDirectory);
	}

	return true;
}

static std::string
duckdbGetExtensionDirectory() {
	StringInfo duckdbExtensionDataDirectory = makeStringInfo();
	appendStringInfo(duckdbExtensionDataDirectory, "%s/duckdb_extensions", DataDir);

	if (!duckdbCheckDataDirectory(duckdbExtensionDataDirectory->data)) {
		if (mkdir(duckdbExtensionDataDirectory->data, S_IRWXU | S_IRWXG | S_IRWXO) == -1) {
			int error = errno;
			pfree(duckdbExtensionDataDirectory->data);
			elog(ERROR, "Creating duckdb extensions directory failed with reason `%s`\n", strerror(error));
		}
		elog(DEBUG2, "Created %s as `duckdb.data_dir`", duckdbExtensionDataDirectory->data);
	};

	std::string duckdbExtensionDirectory(duckdbExtensionDataDirectory->data);
	pfree(duckdbExtensionDataDirectory->data);
	return duckdbExtensionDirectory;
}

duckdb::unique_ptr<duckdb::DuckDB>
DuckdbOpenDatabase() {
	duckdb::DBConfig config;
	config.SetOptionByName("extension_directory", duckdbGetExtensionDirectory());
	return duckdb::make_uniq<duckdb::DuckDB>(nullptr, &config);
}

duckdb::unique_ptr<duckdb::Connection>
DuckdbCreateConnection(List *rtables, PlannerInfo *plannerInfo, List *neededColumns, const char *query) {
	auto db = DuckdbOpenDatabase();

	/* Add tables */
	//db->instance->config.replacement_scans.emplace_back(
	//    pgduckdb::PostgresReplacementScan,
	//    duckdb::make_uniq_base<duckdb::ReplacementScanData, PostgresReplacementScanData>(rtables, plannerInfo,
	//                                                                                     neededColumns, query));

	auto &config = duckdb::DBConfig::GetConfig(*db->instance);
	config.storage_extensions["pgduckdb"] = duckdb::make_uniq<PostgresStorageExtension>(GetActiveSnapshot());

	auto connection = duckdb::make_uniq<duckdb::Connection>(*db);

	auto &context = *connection->context;
	auto &client_data = duckdb::ClientData::Get(context);

	pgduckdb::PostgresSeqScanFunction seq_scan_fun;
	duckdb::CreateTableFunctionInfo seq_scan_info(seq_scan_fun);

	pgduckdb::PostgresIndexScanFunction index_scan_fun;
	duckdb::CreateTableFunctionInfo index_scan_info(index_scan_fun);

	auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
	duckdb::ExtensionInstallInfo extension_install_info;
	db->instance->SetExtensionLoaded("pgduckdb", extension_install_info);
	context.Query("ATTACH DATABASE 'pgduckdb' (TYPE pgduckdb)", false);
	context.Query("USE pgduckdb", false);
	context.transaction.BeginTransaction();
	auto &instance = *db->instance;
	duckdb::ExtensionUtil::RegisterType(instance, "UnsupportedPostgresType", duckdb::LogicalTypeId::VARCHAR);
	catalog.CreateTableFunction(context, &seq_scan_info);
	catalog.CreateTableFunction(context, &index_scan_info);
	context.transaction.Commit();

	auto duckdbSecrets = ReadDuckdbSecrets();

	int secretId = 0;
	for (auto &secret : duckdbSecrets) {
		StringInfo secretKey = makeStringInfo();
		bool isR2CloudSecret = (secret.type.rfind("R2", 0) == 0);
		appendStringInfo(secretKey, "CREATE SECRET duckdbSecret_%d ", secretId);
		appendStringInfo(secretKey, "(TYPE %s, KEY_ID '%s', SECRET '%s'", secret.type.c_str(), secret.id.c_str(),
		                 secret.secret.c_str());
		if (secret.region.length() && !isR2CloudSecret) {
			appendStringInfo(secretKey, ", REGION '%s'", secret.region.c_str());
		}
		if (secret.endpoint.length() && !isR2CloudSecret) {
			appendStringInfo(secretKey, ", ENDPOINT '%s'", secret.endpoint.c_str());
		}
		if (isR2CloudSecret) {
			appendStringInfo(secretKey, ", ACCOUNT_ID '%s'", secret.endpoint.c_str());
		}
		appendStringInfo(secretKey, ");");
		context.Query(secretKey->data, false);
		pfree(secretKey->data);
		secretId++;
	}

	auto duckdbExtensions = ReadDuckdbExtensions();

	for (auto &extension : duckdbExtensions) {
		StringInfo duckdbExtension = makeStringInfo();
		if (extension.enabled) {
			appendStringInfo(duckdbExtension, "LOAD %s;", extension.name.c_str());
			auto res = context.Query(duckdbExtension->data, false);
			if (res->HasError()) {
				elog(ERROR, "Extension `%s` could not be loaded with DuckDB", extension.name.c_str());
			}
		}
		pfree(duckdbExtension->data);
	}

	return connection;
}

} // namespace pgduckdb
