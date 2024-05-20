#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "quack/quack_duckdb_connection.hpp"
#include "quack/quack_heap_scan.hpp"
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
quack_create_duckdb_connection(List *tables, List *neededColumns, const char *query) {
	auto db = quack::quack_open_database();

	/* Add tables */
	db->instance->config.replacement_scans.emplace_back(
	    quack::PostgresHeapReplacementScan,
	    duckdb::make_uniq_base<duckdb::ReplacementScanData, quack::PostgresHeapReplacementScanData>(
	        tables, neededColumns, query));

	auto connection = duckdb::make_uniq<duckdb::Connection>(*db);

	// Add the postgres_scan inserted by the replacement scan
	auto &context = *connection->context;
	quack::PostgresHeapScanFunction heap_scan_fun;
	duckdb::CreateTableFunctionInfo heap_scan_info(heap_scan_fun);

	auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
	context.transaction.BeginTransaction();
	catalog.CreateTableFunction(context, &heap_scan_info);
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