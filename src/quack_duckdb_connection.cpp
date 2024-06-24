#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"

extern "C" {
#include "postgres.h"
#include "access/genam.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
}

#include "quack/quack_duckdb_connection.hpp"
#include "quack/quack_heap_scan.hpp"
#include "quack/quack_utils.hpp"

#include <string>

namespace quack {

/* constants for quack.secrets */
#define Natts_quack_secret              6
#define Anum_quack_secret_type          1
#define Anum_quack_secret_id            2
#define Anum_quack_secret_secret        3
#define Anum_quack_secret_region        4
#define Anum_quack_secret_endpoint      5
#define Anum_quack_secret_r2_account_id 6

typedef struct QuackSecret {
	std::string type;
	std::string id;
	std::string secret;
	std::string region;
	std::string endpoint;
	std::string r2_account_id;
} QuackSecret;

static Oid
quack_get_namespace(void) {
	return get_namespace_oid("quack", false);
}

static Oid
quack_secret_relation_id(void) {
	return get_relname_relid("secrets", quack_get_namespace());
}

static std::string
read_quack_secret_column(Datum columnDatum) {
	std::string columnValue;
	text *cloudType = DatumGetTextPP(columnDatum);
	columnValue = VARDATA_ANY(cloudType);
	columnValue.resize(VARSIZE_ANY_EXHDR(cloudType));
	return columnValue;
}

std::vector<QuackSecret>
read_quack_secrets() {
	HeapTuple tuple = NULL;
	Oid quackSecretRelationId = quack_secret_relation_id();
	Relation quackSecretRelation = table_open(quackSecretRelationId, AccessShareLock);
	SysScanDescData *scan = systable_beginscan(quackSecretRelation, InvalidOid, false, GetActiveSnapshot(), 0, NULL);
	std::vector<QuackSecret> quack_secrets;

	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		Datum datumArray[Natts_quack_secret];
		bool isNullArray[Natts_quack_secret];

		heap_deform_tuple(tuple, RelationGetDescr(quackSecretRelation), datumArray, isNullArray);
		QuackSecret secret;

		secret.type = read_quack_secret_column(datumArray[Anum_quack_secret_type - 1]);
		secret.id = read_quack_secret_column(datumArray[Anum_quack_secret_id - 1]);
		secret.secret = read_quack_secret_column(datumArray[Anum_quack_secret_secret - 1]);

		if (!isNullArray[Anum_quack_secret_region - 1])
			secret.region = read_quack_secret_column(datumArray[Anum_quack_secret_region - 1]);

		if (!isNullArray[Anum_quack_secret_endpoint - 1])
			secret.endpoint = read_quack_secret_column(datumArray[Anum_quack_secret_endpoint - 1]);

		if (!isNullArray[Anum_quack_secret_r2_account_id - 1])
			secret.endpoint = read_quack_secret_column(datumArray[Anum_quack_secret_r2_account_id - 1]);

		quack_secrets.push_back(secret);
	}

	systable_endscan(scan);
	table_close(quackSecretRelation, NoLock);
	return quack_secrets;
}

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
	auto &instance = *db->instance;
	duckdb::ExtensionUtil::RegisterType(instance, "UnsupportedPostgresType", duckdb::LogicalTypeId::VARCHAR);
	catalog.CreateTableFunction(context, &heap_scan_info);
	context.transaction.Commit();

	auto quackSecrets = read_quack_secrets();

	int secretId = 0;
	for (auto &secret : quackSecrets) {
		StringInfo s3SecretKey = makeStringInfo();
		bool isR2CloudSecret = (secret.type.rfind("R2", 0) == 0);
		appendStringInfo(s3SecretKey, "CREATE SECRET quackSecret_%d ", secretId);
		appendStringInfo(s3SecretKey, "(TYPE %s, KEY_ID '%s', SECRET '%s'", secret.type.c_str(),
		                 secret.id.c_str(), secret.secret.c_str());
		if (secret.region.length() && !isR2CloudSecret) {
			appendStringInfo(s3SecretKey, ", REGION '%s'", secret.region.c_str());
		}
		if (secret.endpoint.length() && !isR2CloudSecret) {
			appendStringInfo(s3SecretKey, ", ENDPOINT '%s'", secret.endpoint.c_str());
		}
		if (isR2CloudSecret) {
			appendStringInfo(s3SecretKey, ", ACCOUNT_ID '%s'", secret.endpoint.c_str());
		}
		appendStringInfo(s3SecretKey, ");");
		context.Query(s3SecretKey->data, false);
		pfree(s3SecretKey->data);
		secretId++;
	}

	return connection;
}

} // namespace quack