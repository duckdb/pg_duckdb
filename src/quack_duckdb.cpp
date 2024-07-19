#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"

#include "quack/quack_options.hpp"
#include "quack/quack_duckdb.hpp"
#include "quack/scan/postgres_scan.hpp"
#include "quack/scan/postgres_index_scan.hpp"
#include "quack/scan/postgres_seq_scan.hpp"
#include "quack/quack_utils.hpp"

#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

namespace quack {

static bool
quackCheckDataDirectory(const char *dataDirectory) {
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
quackGetExtensionDirectory() {
	StringInfo quackExtensionDataDirectory = makeStringInfo();
	appendStringInfo(quackExtensionDataDirectory, "%s/quack_extensions", DataDir);

	if (!quackCheckDataDirectory(quackExtensionDataDirectory->data)) {
		if (mkdir(quackExtensionDataDirectory->data, S_IRWXU | S_IRWXG | S_IRWXO) == -1) {
			int error = errno;
			pfree(quackExtensionDataDirectory->data);
			elog(ERROR, "Creating quack extensions directory failed with reason `%s`\n", strerror(error));
		}
		elog(DEBUG2, "Created %s as `quack.data_dir`", quackExtensionDataDirectory->data);
	};

	std::string quackExtensionDirectory(quackExtensionDataDirectory->data);
	pfree(quackExtensionDataDirectory->data);
	return quackExtensionDirectory;
}

duckdb::unique_ptr<duckdb::DuckDB>
quack_open_database() {
	duckdb::DBConfig config;
	// config.SetOption("memory_limit", "2GB");
	// config.SetOption("threads", "8");
	// config.allocator = duckdb::make_uniq<duckdb::Allocator>(QuackAllocate, QuackFree, QuackReallocate, nullptr);
	config.SetOptionByName("extension_directory", quackGetExtensionDirectory());
	return duckdb::make_uniq<duckdb::DuckDB>("test.duckdb", &config);
}

duckdb::unique_ptr<duckdb::Connection>
quack_create_duckdb_connection(List *rtables, PlannerInfo *plannerInfo, List *neededColumns, const char *query) {
	auto db = quack::quack_open_database();

	/* Add tables */
	db->instance->config.replacement_scans.emplace_back(
	    quack::PostgresReplacementScan,
	    duckdb::make_uniq_base<duckdb::ReplacementScanData, quack::PostgresReplacementScanData>(rtables, plannerInfo,
	                                                                                            neededColumns, query));

	auto connection = duckdb::make_uniq<duckdb::Connection>(*db);

	// Add the postgres_scan inserted by the replacement scan
	auto &context = *connection->context;

	quack::PostgresSeqScanFunction seq_scan_fun;
	duckdb::CreateTableFunctionInfo seq_scan_info(seq_scan_fun);

	quack::PostgresIndexScanFunction index_scan_fun;
	duckdb::CreateTableFunctionInfo index_scan_info(index_scan_fun);

	auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
	context.transaction.BeginTransaction();
	auto &instance = *db->instance;
	duckdb::ExtensionUtil::RegisterType(instance, "UnsupportedPostgresType", duckdb::LogicalTypeId::VARCHAR);
	catalog.CreateTableFunction(context, &seq_scan_info);
	catalog.CreateTableFunction(context, &index_scan_info);
	context.transaction.Commit();

	auto quackSecrets = read_quack_secrets();

	int secretId = 0;
	for (auto &secret : quackSecrets) {
		StringInfo secretKey = makeStringInfo();
		bool isR2CloudSecret = (secret.type.rfind("R2", 0) == 0);
		appendStringInfo(secretKey, "CREATE SECRET quackSecret_%d ", secretId);
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

	auto quackExtensions = read_quack_extensions();

	for (auto &extension : quackExtensions) {
		StringInfo quackExtension = makeStringInfo();
		if (extension.enabled) {
			appendStringInfo(quackExtension, "LOAD %s;", extension.name.c_str());
			auto res = context.Query(quackExtension->data, false);
			if (res->HasError()) {
				elog(ERROR, "Extension `%s` could not be loaded with DuckDB", extension.name.c_str());
			}
		}
		pfree(quackExtension->data);
	}

	return connection;
}

} // namespace quack