
#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
}

#include "pgduckdb/pgduckdb_options.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

namespace pgduckdb {

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

std::vector<DuckdbSecret>
ReadDuckdbSecrets() {
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
		secret.key_id = DatumToString(datum_array[Anum_duckdb_secret_key_id - 1]);
		secret.secret = DatumToString(datum_array[Anum_duckdb_secret_secret - 1]);

		if (!is_null_array[Anum_duckdb_secret_region - 1])
			secret.region = DatumToString(datum_array[Anum_duckdb_secret_region - 1]);

		if (!is_null_array[Anum_duckdb_secret_session_token - 1])
			secret.session_token = DatumToString(datum_array[Anum_duckdb_secret_session_token - 1]);

		if (!is_null_array[Anum_duckdb_secret_endpoint - 1])
			secret.endpoint = DatumToString(datum_array[Anum_duckdb_secret_endpoint - 1]);

		if (!is_null_array[Anum_duckdb_secret_r2_account_id - 1])
			secret.endpoint = DatumToString(datum_array[Anum_duckdb_secret_r2_account_id - 1]);

		if (!is_null_array[Anum_duckdb_secret_use_ssl - 1])
			secret.use_ssl = DatumGetBool(DatumGetBool(datum_array[Anum_duckdb_secret_use_ssl - 1]));
		else
			secret.use_ssl = true;
		duckdb_secrets.push_back(secret);
	}

	systable_endscan(scan);
	table_close(duckdb_secret_relation, NoLock);
	return duckdb_secrets;
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
DuckdbInstallExtension(Datum name_datum) {
	auto extension_name = DatumToString(name_datum);
	auto install_extension_command = duckdb::StringUtil::Format("INSTALL %s;", extension_name.c_str());
	pgduckdb::DuckDBQueryOrThrow(install_extension_command);

	Oid arg_types[] = {TEXTOID};
	Datum values[] = {name_datum};

	SPI_connect();
	auto ret = SPI_execute_with_args(R"(
		INSERT INTO duckdb.extensions (name, enabled)
		VALUES ($1, true)
		ON CONFLICT (name) DO UPDATE SET enabled = true
		)",
	                                 lengthof(arg_types), arg_types, values, NULL, false, 0);
	if (ret != SPI_OK_INSERT) {
		throw std::runtime_error("SPI_exec failed: error code " + std::string(SPI_result_code_string(ret)));
	}
	SPI_finish();

	return true;
}

static bool
CanCacheRemoteObject(std::string remote_object) {
	return (remote_object.rfind("https://", 0) == 0) || (remote_object.rfind("http://", 0) == 0) ||
	       (remote_object.rfind("s3://", 0) == 0) || (remote_object.rfind("s3a://", 0) == 0) ||
	       (remote_object.rfind("s3n://", 0) == 0) || (remote_object.rfind("gcs://", 0) == 0) ||
	       (remote_object.rfind("gs://", 0) == 0) || (remote_object.rfind("r2://", 0) == 0);
}

static bool
DuckdbCacheObject(Datum object, Datum type) {
	auto object_path = DatumToString(object);
	if (!CanCacheRemoteObject(object_path)) {
		elog(WARNING, "(PGDuckDB/DuckdbCacheObject) Object path '%s' can't be cached.", object_path.c_str());
		return false;
	}

	auto object_type = DatumToString(type);
	if (object_type != "parquet" && object_type != "csv") {
		elog(WARNING, "(PGDuckDB/DuckdbCacheObject) Cache object type should be 'parquet' or 'csv'.");
		return false;
	}

	auto con = DuckDBManager::CreateConnection();
	auto &context = *con->context;

	DuckDBQueryOrThrow(context, "SET enable_http_file_cache TO true;");

	auto object_type_fun = object_type == "parquet" ? "read_parquet" : "read_csv";
	auto cache_object_query =
	    duckdb::StringUtil::Format("SELECT 1 FROM %s('%s');", object_type_fun, object_path.c_str());
	DuckDBQueryOrThrow(context, cache_object_query);
	DuckDBQueryOrThrow(context, "SET enable_http_file_cache TO false;");

	return true;
}

} // namespace pgduckdb

extern "C" {

DECLARE_PG_FUNCTION(install_extension) {
	Datum extension_name = PG_GETARG_DATUM(0);
	bool result = pgduckdb::DuckdbInstallExtension(extension_name);
	PG_RETURN_BOOL(result);
}

DECLARE_PG_FUNCTION(pgduckdb_raw_query) {
	const char *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
	auto result = pgduckdb::DuckDBQueryOrThrow(query);
	elog(NOTICE, "result: %s", result->ToString().c_str());
	PG_RETURN_BOOL(true);
}

DECLARE_PG_FUNCTION(cache) {
	Datum object = PG_GETARG_DATUM(0);
	Datum type = PG_GETARG_DATUM(1);
	bool result = pgduckdb::DuckdbCacheObject(object, type);
	PG_RETURN_BOOL(result);
}

DECLARE_PG_FUNCTION(pgduckdb_recycle_ddb) {
	pgduckdb::DuckDBManager::Get().Reset();
	PG_RETURN_BOOL(true);
}

} // extern "C"
