
#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
}

#include "pgduckdb/pgduckdb_options.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

namespace pgduckdb {

/* constants for duckdb.secrets */
#define Natts_duckdb_secret              6
#define Anum_duckdb_secret_type          1
#define Anum_duckdb_secret_id            2
#define Anum_duckdb_secret_secret        3
#define Anum_duckdb_secret_region        4
#define Anum_duckdb_secret_endpoint      5
#define Anum_duckdb_secret_r2_account_id 6

typedef struct DuckdbSecret {
	std::string type;
	std::string id;
	std::string secret;
	std::string region;
	std::string endpoint;
	std::string r2_account_id;
} DuckdbSecret;

/* constants for duckdb.extensions */
#define Natts_duckdb_extension       2
#define Anum_duckdb_extension_name   1
#define Anum_duckdb_extension_enable 2

static Oid
GetDuckdbNamespace(void) {
	return get_namespace_oid("duckdb", false);
}

static Oid
SecretsTableRelationId(void) {
	return get_relname_relid("secrets", GetDuckdbNamespace());
}

static Oid
ExtensionsTableRelationId(void) {
	return get_relname_relid("extensions", GetDuckdbNamespace());
}

static std::string
DatumToString(Datum datum) {
	std::string column_value;
	text *datum_text = DatumGetTextPP(datum);
	column_value = VARDATA_ANY(datum_text);
	column_value.resize(VARSIZE_ANY_EXHDR(datum_text));
	return column_value;
}

static std::vector<DuckdbSecret>
DuckdbReadSecrets() {
	HeapTuple tuple = NULL;
	Oid duckdb_secret_table_relation_id = SecretsTableRelationId();
	Relation duckdb_secret_relation = table_open(duckdb_secret_table_relation_id, AccessShareLock);
	SysScanDescData *scan = systable_beginscan(duckdb_secret_relation, InvalidOid, false, GetActiveSnapshot(), 0, NULL);
	std::vector<DuckdbSecret> duckdb_secrets;

	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		Datum datum_array[Natts_duckdb_secret];
		bool is_null_array[Natts_duckdb_secret];

		heap_deform_tuple(tuple, RelationGetDescr(duckdb_secret_relation), datum_array, is_null_array);
		DuckdbSecret secret;

		secret.type = DatumToString(datum_array[Anum_duckdb_secret_type - 1]);
		secret.id = DatumToString(datum_array[Anum_duckdb_secret_id - 1]);
		secret.secret = DatumToString(datum_array[Anum_duckdb_secret_secret - 1]);

		if (!is_null_array[Anum_duckdb_secret_region - 1])
			secret.region = DatumToString(datum_array[Anum_duckdb_secret_region - 1]);

		if (!is_null_array[Anum_duckdb_secret_endpoint - 1])
			secret.endpoint = DatumToString(datum_array[Anum_duckdb_secret_endpoint - 1]);

		if (!is_null_array[Anum_duckdb_secret_r2_account_id - 1])
			secret.endpoint = DatumToString(datum_array[Anum_duckdb_secret_r2_account_id - 1]);

		duckdb_secrets.push_back(secret);
	}

	systable_endscan(scan);
	table_close(duckdb_secret_relation, NoLock);
	return duckdb_secrets;
}

void
AddSecretsToDuckdbContext(duckdb::ClientContext &context) {
	auto duckdb_secrets = DuckdbReadSecrets();
	int secret_id = 0;
	for (auto &secret : duckdb_secrets) {
		StringInfo secret_key = makeStringInfo();
		bool is_r2_cloud_secret = (secret.type.rfind("R2", 0) == 0);
		appendStringInfo(secret_key, "CREATE SECRET pg_duckdb_secret_%d ", secret_id);
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

std::vector<DuckdbExtension>
ReadDuckdbExtensions() {
	HeapTuple tuple = NULL;
	Oid duckdb_extension_table_relation_id = ExtensionsTableRelationId();
	Relation duckdb_extension_relation = table_open(duckdb_extension_table_relation_id, AccessShareLock);
	SysScanDescData *scan =
	    systable_beginscan(duckdb_extension_relation, InvalidOid, false, GetActiveSnapshot(), 0, NULL);
	std::vector<DuckdbExtension> duckdb_extensions;

	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		Datum datum_array[Natts_duckdb_secret];
		bool is_null_array[Natts_duckdb_secret];

		heap_deform_tuple(tuple, RelationGetDescr(duckdb_extension_relation), datum_array, is_null_array);
		DuckdbExtension secret;

		secret.name = DatumToString(datum_array[Anum_duckdb_extension_name - 1]);
		secret.enabled = DatumGetBool(datum_array[Anum_duckdb_extension_enable - 1]);
		duckdb_extensions.push_back(secret);
	}

	systable_endscan(scan);
	table_close(duckdb_extension_relation, NoLock);
	return duckdb_extensions;
}

static bool
DuckdbInstallExtension(Datum name) {
	auto db = DuckdbOpenDatabase();
	auto connection = duckdb::make_uniq<duckdb::Connection>(*db);
	auto &context = *connection->context;

	auto extension_name = DatumToString(name);
	auto install_extension_command = duckdb::StringUtil::Format("INSTALL %s;", extension_name.c_str());
	auto res = context.Query(install_extension_command, false);

	if (res->HasError()) {
		elog(WARNING, "(DuckdbInstallExtension) %s", res->GetError().c_str());
		return false;
	}

	bool nulls[Natts_duckdb_extension] = {0};
	Datum values[Natts_duckdb_extension] = {0};

	values[Anum_duckdb_extension_name - 1] = name;
	values[Anum_duckdb_extension_enable - 1] = 1;

	/* create heap tuple and insert into catalog table */
	Relation duckdb_extensions_relation = relation_open(ExtensionsTableRelationId(), RowExclusiveLock);
	TupleDesc tuple_descr = RelationGetDescr(duckdb_extensions_relation);

	/* inserting extension record */
	HeapTuple new_tuple = heap_form_tuple(tuple_descr, values, nulls);
	CatalogTupleInsert(duckdb_extensions_relation, new_tuple);

	CommandCounterIncrement();
	relation_close(duckdb_extensions_relation, RowExclusiveLock);

	return true;
}

static bool
CanCacheRemoteObject(std::string remote_object) {
	return remote_object.rfind("https://", 0) * remote_object.rfind("http://", 0) * remote_object.rfind("s3://", 0) *
	           remote_object.rfind("s3a://", 0) * remote_object.rfind("s3n://", 0) * remote_object.rfind("gcs://", 0) *
	           remote_object.rfind("gs://", 0) * remote_object.rfind("r2://", 0) ==
	       0;
}

static bool
DuckdbCacheObject(Datum object, Datum type) {
	auto db = DuckdbOpenDatabase();
	auto connection = duckdb::make_uniq<duckdb::Connection>(*db);
	auto &context = *connection->context;

	auto object_type = DatumToString(type);

	if (object_type != "parquet" && object_type != "csv") {
		elog(WARNING, "(DuckdbCacheObject) Cache object type should be 'parquet' or 'csv'.");
		return false;
	}

	auto object_type_fun = object_type == "parquet" ? "read_parquet" : "read_csv";

	context.Query("SET enable_http_file_cache TO true;", false);
	auto http_file_cache_set_dir_query =
	    duckdb::StringUtil::Format("SET http_file_cache_dir TO '%s';", CreateOrGetDirectoryPath("duckdb_cache"));
	context.Query(http_file_cache_set_dir_query, false);

	AddSecretsToDuckdbContext(context);

	auto object_path = DatumToString(object);

	if (!CanCacheRemoteObject(object_path)) {
		elog(WARNING, "(DuckdbCacheObject) Object path '%s' can't be cached.", object_path.c_str());
		return false;
	}

	auto cache_object_query =
	    duckdb::StringUtil::Format("SELECT 1 FROM %s('%s');", object_type_fun, object_path.c_str());
	auto res = context.Query(cache_object_query, false);

	if (res->HasError()) {
		elog(WARNING, "(DuckdbCacheObject) %s", res->GetError().c_str());
		return false;
	}

	return true;
}

} // namespace pgduckdb

extern "C" {

PG_FUNCTION_INFO_V1(install_extension);
Datum
install_extension(PG_FUNCTION_ARGS) {
	Datum extension_name = PG_GETARG_DATUM(0);
	bool result = pgduckdb::DuckdbInstallExtension(extension_name);
	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(cache);
Datum
cache(PG_FUNCTION_ARGS) {
	Datum object = PG_GETARG_DATUM(0);
	Datum type = PG_GETARG_DATUM(1);
	bool result = pgduckdb::DuckdbCacheObject(object, type);
	PG_RETURN_BOOL(result);
}

} // extern "C"
