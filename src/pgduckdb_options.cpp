
#include "duckdb.hpp"

#include <filesystem>
#include <fstream>

#include "pgduckdb/pgduckdb_types.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"
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
#include "pgduckdb/pgduckdb_xact.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

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

		if (!is_null_array[Anum_duckdb_secret_scope - 1])
			secret.scope = DatumToString(datum_array[Anum_duckdb_secret_scope - 1]);

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
	if (ret != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
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

	/* Use a separate connection to cache the objects, so we are sure not to
	 * leak the value change of enable_http_file_cache in case of an error */
	auto con = DuckDBManager::CreateConnection();
	auto &context = *con->context;

	DuckDBQueryOrThrow(context, "SET enable_http_file_cache TO true;");

	auto object_type_fun = object_type == "parquet" ? "read_parquet" : "read_csv";
	auto cache_object_query =
	    duckdb::StringUtil::Format("SELECT 1 FROM %s('%s');", object_type_fun, object_path.c_str());
	DuckDBQueryOrThrow(context, cache_object_query);

	return true;
}

typedef struct CacheFileInfo {
	std::string cache_key;
	std::string remote_path;
	int64_t file_size;
	Timestamp file_timestamp;
} CacheFileInfo;

static std::vector<CacheFileInfo>
DuckdbGetCachedFilesInfos() {
	std::string ext(".meta");
	std::vector<CacheFileInfo> cache_info;
	for (auto &p : std::filesystem::recursive_directory_iterator(CreateOrGetDirectoryPath("duckdb_cache"))) {
		if (p.path().extension() == ext) {
			std::ifstream cache_file_metadata(p.path());
			std::string metadata;
			std::vector<std::string> metadata_tokens;
			while (std::getline(cache_file_metadata, metadata, ',')) {
				metadata_tokens.push_back(metadata);
			}
			if (metadata_tokens.size() != 4) {
				elog(WARNING, "(PGDuckDB/DuckdbGetCachedFilesInfos) Invalid '%s' cache metadata file",
				     p.path().c_str());
				break;
			}
			cache_info.push_back(CacheFileInfo {metadata_tokens[0], metadata_tokens[1], std::stoi(metadata_tokens[2]),
			                                    std::stoi(metadata_tokens[3])});
		}
	}
	return cache_info;
}

static bool
DuckdbCacheDelete(Datum cache_key_datum) {
	auto cache_key = DatumToString(cache_key_datum);
	if (!cache_key.size()) {
		elog(WARNING, "(PGDuckDB/DuckdbGetCachedFilesInfos) Empty cache key");
		return false;
	}
	auto cache_filename = CreateOrGetDirectoryPath("duckdb_cache") + "/" + cache_key;
	bool removed = !std::remove(cache_filename.c_str());
	std::remove(std::string(cache_filename + ".meta").c_str());
	return removed;
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

DECLARE_PG_FUNCTION(cache_info) {
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	Tuplestorestate *tuple_store;
	TupleDesc cache_info_tuple_desc;

	auto result = pgduckdb::DuckdbGetCachedFilesInfos();

	cache_info_tuple_desc = CreateTemplateTupleDesc(4);
	TupleDescInitEntry(cache_info_tuple_desc, (AttrNumber)1, "remote_path", TEXTOID, -1, 0);
	TupleDescInitEntry(cache_info_tuple_desc, (AttrNumber)2, "cache_file_name", TEXTOID, -1, 0);
	TupleDescInitEntry(cache_info_tuple_desc, (AttrNumber)3, "cache_file_size", INT8OID, -1, 0);
	TupleDescInitEntry(cache_info_tuple_desc, (AttrNumber)4, "cache_file_timestamp", TIMESTAMPTZOID, -1, 0);

	// We need to switch to long running memory context
	MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tuple_store = tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random, false, work_mem);
	MemoryContextSwitchTo(oldcontext);

	for (auto &cache_info : result) {
		Datum values[4] = {0};
		bool nulls[4] = {0};

		values[0] = CStringGetTextDatum(cache_info.remote_path.c_str());
		values[1] = CStringGetTextDatum(cache_info.cache_key.c_str());
		values[2] = cache_info.file_size;
		/* We have stored timestamp in *seconds* from epoch. We need to convert this to PG timestamptz. */
		values[3] = (cache_info.file_timestamp * 1000000) - pgduckdb::PGDUCKDB_DUCK_TIMESTAMP_OFFSET;

		HeapTuple tuple = heap_form_tuple(cache_info_tuple_desc, values, nulls);
		tuplestore_puttuple(tuple_store, tuple);
		heap_freetuple(tuple);
	}

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tuple_store;
	rsinfo->setDesc = cache_info_tuple_desc;

	return (Datum)0;
}

DECLARE_PG_FUNCTION(cache_delete) {
	Datum cache_key = PG_GETARG_DATUM(0);
	bool result = pgduckdb::DuckdbCacheDelete(cache_key);
	PG_RETURN_BOOL(result);
}

DECLARE_PG_FUNCTION(pgduckdb_recycle_ddb) {
	/*
	 * We cannot safely run this in a transaction block, because a DuckDB
	 * transaction might have already started. Recycling the database will
	 * violate our assumptions about DuckDB its transaction lifecycle
	 */
	pgduckdb::pg::PreventInTransactionBlock("duckdb.recycle_ddb()");
	pgduckdb::DuckDBManager::Get().Reset();
	PG_RETURN_BOOL(true);
}

} // extern "C"
