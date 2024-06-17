#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"

#include "quack/quack_duckdb.hpp"
#include "quack/scan/postgres_scan.hpp"
#include "quack/scan/postgres_seq_scan.hpp"
#include "quack/scan/postgres_index_scan.hpp"
#include "quack/quack_utils.hpp"

namespace quack {

static duckdb::unique_ptr<duckdb::DuckDB>
quack_open_database() {
	duckdb::DBConfig config;
	// config.SetOption("memory_limit", "2GB");
	// config.SetOption("threads", "8");
	// config.allocator = duckdb::make_uniq<duckdb::Allocator>(QuackAllocate, QuackFree, QuackReallocate, nullptr);
	return duckdb::make_uniq<duckdb::DuckDB>(nullptr, &config);
}

duckdb::unique_ptr<duckdb::Connection>
quack_create_duckdb_connection(List *rtables, PlannerInfo *plannerInfo, List *neededColumns, const char *query) {
	auto db = quack::quack_open_database();

	/* Add tables */
	db->instance->config.replacement_scans.emplace_back(
	    quack::PostgresReplacementScan,
	    duckdb::make_uniq_base<duckdb::ReplacementScanData, quack::PostgresReplacementScanData>(
	        rtables, plannerInfo, neededColumns, query));

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

	if (strlen(quack_secret) != 0) {
		std::vector<std::string> quackSecret = quack::tokenizeString(quack_secret, '#');
		StringInfo s3SecretKey = makeStringInfo();
		appendStringInfoString(s3SecretKey, "CREATE SECRET s3Secret ");
		appendStringInfo(s3SecretKey, "(TYPE S3, KEY_ID '%s', SECRET '%s', REGION '%s');", quackSecret[1].c_str(),
		                 quackSecret[2].c_str(), quackSecret[3].c_str());
		context.Query(s3SecretKey->data, false);
		pfree(s3SecretKey->data);
	}

	return connection;
}

} // namespace quack