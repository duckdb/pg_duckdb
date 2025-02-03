
#include "duckdb.hpp"

#include <filesystem>
#include <fstream>

#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

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
#include "parser/parse_coerce.h"
#include "parser/parse_node.h"
#include "parser/parse_expr.h"
#include "nodes/subscripting.h"
#include "nodes/nodeFuncs.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "nodes/print.h"

#include "pgduckdb/vendor/pg_list.hpp"
}

#include "pgduckdb/pgduckdb_options.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
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

bool
DoesSecretRequiresKeyIdOrSecret(const SecretType type) {
	return type == SecretType::S3 || type == SecretType::GCS || type == SecretType::R2;
}

SecretType
StringToSecretType(const std::string &type) {
	auto uc_type = duckdb::StringUtil::Upper(type);
	if (uc_type == "S3") {
		return SecretType::S3;
	} else if (uc_type == "R2") {
		return SecretType::R2;
	} else if (uc_type == "GCS") {
		return SecretType::GCS;
	} else if (uc_type == "AZURE") {
		return SecretType::AZURE;
	} else {
		throw std::runtime_error("Invalid secret type: '" + type + "'");
	}
}

std::string
SecretTypeToString(SecretType type) {
	switch (type) {
	case SecretType::S3:
		return "S3";
	case SecretType::R2:
		return "R2";
	case SecretType::GCS:
		return "GCS";
	case SecretType::AZURE:
		return "AZURE";
	default:
		throw std::runtime_error("Invalid secret type: '" + std::to_string(type) + "'");
	}
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

		auto type_str = DatumToString(datum_array[Anum_duckdb_secret_type - 1]);
		secret.type = StringToSecretType(type_str);
		if (!is_null_array[Anum_duckdb_secret_key_id - 1])
			secret.key_id = DatumToString(datum_array[Anum_duckdb_secret_key_id - 1]);
		else if (DoesSecretRequiresKeyIdOrSecret(secret.type)) {
			elog(WARNING, "Invalid '%s' secret: key id is required.", type_str.c_str());
			continue;
		}

		if (!is_null_array[Anum_duckdb_secret_secret - 1])
			secret.secret = DatumToString(datum_array[Anum_duckdb_secret_secret - 1]);
		else if (DoesSecretRequiresKeyIdOrSecret(secret.type)) {
			elog(WARNING, "Invalid '%s' secret: secret is required.", type_str.c_str());
			continue;
		}

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

		if (!is_null_array[Anum_duckdb_secret_connection_string - 1])
			secret.connection_string = DatumToString(datum_array[Anum_duckdb_secret_connection_string - 1]);

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

	auto install_extension_command = "INSTALL " + duckdb::KeywordHelper::WriteQuoted(extension_name);

	/*
	 * Temporily allow all filesystems for this query, because INSTALL needs
	 * local filesystem access.
	 */
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("duckdb.disabled_filesystems", "", PGC_SUSET, PGC_S_SESSION);
	pgduckdb::DuckDBQueryOrThrow(install_extension_command);
	AtEOXact_GUC(false, save_nestlevel);

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
			cache_info.push_back(CacheFileInfo {metadata_tokens[0], metadata_tokens[1], std::stoll(metadata_tokens[2]),
			                                    std::stoll(metadata_tokens[3])});
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
	pgduckdb::RequireDuckdbExecution();
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
	pgduckdb::RequireDuckdbExecution();
	Datum cache_key = PG_GETARG_DATUM(0);
	bool result = pgduckdb::DuckdbCacheDelete(cache_key);
	PG_RETURN_BOOL(result);
}

DECLARE_PG_FUNCTION(pgduckdb_recycle_ddb) {
	pgduckdb::RequireDuckdbExecution();
	/*
	 * We cannot safely run this in a transaction block, because a DuckDB
	 * transaction might have already started. Recycling the database will
	 * violate our assumptions about DuckDB its transaction lifecycle
	 */
	pgduckdb::pg::PreventInTransactionBlock("duckdb.recycle_ddb()");
	pgduckdb::DuckDBManager::Get().Reset();
	PG_RETURN_BOOL(true);
}

Node *
CoerceRowSubscriptToText(struct ParseState *pstate, A_Indices *subscript) {
	if (!subscript->uidx) {
		elog(ERROR, "Creating a slice out of duckdb.row is not supported");
	}

	Node *subscript_expr = transformExpr(pstate, subscript->uidx, pstate->p_expr_kind);
	int expr_location = exprLocation(subscript->uidx);
	Oid subscript_expr_type = exprType(subscript_expr);

	if (subscript->lidx) {
		elog(ERROR, "Creating a slice out of duckdb.row is not supported");
	}

	Node *coerced_expr = coerce_to_target_type(pstate, subscript_expr, subscript_expr_type, TEXTOID, -1,
	                                           COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, expr_location);
	if (!coerced_expr) {
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("duckdb.row subscript must have text type"),
		                parser_errposition(pstate, expr_location)));
	}

	if (!IsA(subscript_expr, Const)) {
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("duckdb.row subscript must be a constant"),
		                parser_errposition(pstate, expr_location)));
	}

	Const *subscript_const = castNode(Const, subscript_expr);
	if (subscript_const->constisnull) {
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("duckdb.row subscript cannot be NULL"),
		                parser_errposition(pstate, expr_location)));
	}

	return coerced_expr;
}

/*
 * In Postgres all index operations in a row ar all slices or all plain
 * index operations. If you mix them, all are converted to slices.
 * There's no difference in representation possible between
 * "col[1:2][1]" and "col[1:2][1:]". If you want this seperation you
 * need to use parenthesis to seperate: "(col[1:2])[1]"
 * This might seem like fairly strange behaviour, but Postgres uses
 * this to be able to slice in multi-dimensional arrays and thtis
 * behaviour is documented here:
 * https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
 *
 * This is different from DuckDB, but there's not much we can do about
 * that. So we'll have this same behaviour by, which means we need to always
 * add the lower subscript to the slice. The lower subscript will be NULL in
 * that case.
 *
 * See also comments on SubscriptingRef in nodes/subscripting.h
 */
void
AddSubscriptExpressions(SubscriptingRef *sbsref, struct ParseState *pstate, A_Indices *subscript, bool isSlice) {
	Assert(isSlice || subscript->uidx);

	Node *upper_subscript_expr = NULL;
	if (subscript->uidx) {
		upper_subscript_expr = transformExpr(pstate, subscript->uidx, pstate->p_expr_kind);
	}

	sbsref->refupperindexpr = lappend(sbsref->refupperindexpr, upper_subscript_expr);

	if (isSlice) {
		Node *lower_subscript_expr = NULL;
		if (subscript->uidx) {
			lower_subscript_expr = transformExpr(pstate, subscript->lidx, pstate->p_expr_kind);
		}
		sbsref->reflowerindexpr = lappend(sbsref->reflowerindexpr, lower_subscript_expr);
	}
}

/*
 * DuckdbRowSubscriptTransform is called by the parser when a subscripting
 * operation is performed on a duckdb.row. It has two main puprposes:
 * 1. Ensure that the row is being indexed using a string literal
 * 2. Ensure that the return type of this index operation is duckdb.unresolved_type
 */
void
DuckdbRowSubscriptTransform(SubscriptingRef *sbsref, List *indirection, struct ParseState *pstate, bool isSlice,
                            bool isAssignment) {
	/*
	 * We need to populate our cache for some of the code below. Normally this
	 * cache is populated at the start of our planner hook, but this function
	 * is being called from the parser.
	 */
	if (!pgduckdb::IsExtensionRegistered()) {
		elog(ERROR, "BUG: Using duckdb.row but the pg_duckdb extension is not installed");
	}

	if (isAssignment) {
		elog(ERROR, "Assignment to duckdb.row is not supported");
	}

	if (indirection == NIL) {
		elog(ERROR, "Subscripting duckdb.row with an empty subscript is not supported");
	}

	bool first = true;

	// Transform each subscript expression
	foreach_node(A_Indices, subscript, indirection) {
		/* The first subscript needs to be a TEXT constant, since it should be
		 * a column reference. But the subscripts after that can be anything,
		 * DuckDB should interpret those. */
		if (first) {
			sbsref->refupperindexpr = lappend(sbsref->refupperindexpr, CoerceRowSubscriptToText(pstate, subscript));
			if (isSlice) {
				sbsref->reflowerindexpr = lappend(sbsref->reflowerindexpr, NULL);
			}
			first = false;
			continue;
		}

		AddSubscriptExpressions(sbsref, pstate, subscript, isSlice);
	}

	// Set the result type of the subscripting operation
	sbsref->refrestype = pgduckdb::DuckdbUnresolvedTypeOid();
	sbsref->reftypmod = -1;
}

/*
 * DuckdbRowSubscriptExecSetup is called by the executor when a subscripting
 * operation is performed on a duckdb.row. This should never happen, because
 * any query that contains a duckdb.row should automatically be use DuckDB
 * execution.
 */
void
DuckdbRowSubscriptExecSetup(const SubscriptingRef * /*sbsref*/, SubscriptingRefState * /*sbsrefstate*/,
                            SubscriptExecSteps * /*exprstate*/) {
	elog(ERROR, "Subscripting duckdb.row is not supported in the Postgres Executor");
}

static SubscriptRoutines duckdb_row_subscript_routines = {
    .transform = DuckdbRowSubscriptTransform,
    .exec_setup = DuckdbRowSubscriptExecSetup,
    .fetch_strict = false,
    .fetch_leakproof = true,
    .store_leakproof = true,
};

DECLARE_PG_FUNCTION(duckdb_row_subscript) {
	PG_RETURN_POINTER(&duckdb_row_subscript_routines);
}

/*
 * DuckdbUnresolvedTypeSubscriptTransform is called by the parser when a
 * subscripting operation is performed on a duckdb.unresolved_type. All this
 * does is parse ensre that any subscript on duckdb.unresolved_type returns an
 * unrsolved type again.
 */
void
DuckdbUnresolvedTypeSubscriptTransform(SubscriptingRef *sbsref, List *indirection, struct ParseState *pstate,
                                       bool isSlice, bool isAssignment) {
	/*
	 * We need to populate our cache for some of the code below. Normally this
	 * cache is populated at the start of our planner hook, but this function
	 * is being called from the parser.
	 */
	if (!pgduckdb::IsExtensionRegistered()) {
		elog(ERROR, "BUG: Using duckdb.unresolved_type but the pg_duckdb extension is not installed");
	}

	if (isAssignment) {
		elog(ERROR, "Assignment to duckdb.unresolved_type is not supported");
	}

	if (indirection == NIL) {
		elog(ERROR, "Subscripting duckdb.row with an empty subscript is not supported");
	}

	// Transform each subscript expression
	foreach_node(A_Indices, subscript, indirection) {
		AddSubscriptExpressions(sbsref, pstate, subscript, isSlice);
	}

	// Set the result type of the subscripting operation
	sbsref->refrestype = pgduckdb::DuckdbUnresolvedTypeOid();
	sbsref->reftypmod = -1;
}

/*
 * DuckdbUnresolvedTypeSubscriptExecSetup is called by the executor when a
 * subscripting operation is performed on a duckdb.unresolved_type. This should
 * never happen, because any query that contains a duckdb.unresolved_type should
 * automatically be use DuckDB execution.
 */
void
DuckdbUnresolvedTypeSubscriptExecSetup(const SubscriptingRef * /*sbsref*/, SubscriptingRefState * /*sbsrefstate*/,
                                       SubscriptExecSteps * /*exprstate*/) {
	elog(ERROR, "Subscripting duckdb.unresolved_type is not supported in the Postgres Executor");
}

static SubscriptRoutines duckdb_unresolved_type_subscript_routines = {
    .transform = DuckdbUnresolvedTypeSubscriptTransform,
    .exec_setup = DuckdbUnresolvedTypeSubscriptExecSetup,
    .fetch_strict = false,
    .fetch_leakproof = true,
    .store_leakproof = true,
};

DECLARE_PG_FUNCTION(duckdb_unresolved_type_subscript) {
	PG_RETURN_POINTER(&duckdb_unresolved_type_subscript_routines);
}

DECLARE_PG_FUNCTION(duckdb_row_in) {
	elog(ERROR, "Creating the duckdb.row type is not supported");
}

DECLARE_PG_FUNCTION(duckdb_row_out) {
	elog(ERROR, "Converting a duckdb.row to a string is not supported");
}

DECLARE_PG_FUNCTION(duckdb_unresolved_type_in) {
	return textin(fcinfo);
}

DECLARE_PG_FUNCTION(duckdb_unresolved_type_out) {
	return textout(fcinfo);
}

DECLARE_PG_FUNCTION(duckdb_unresolved_type_operator) {
	elog(ERROR, "Unresolved duckdb types cannot be used by the Postgres executor");
}

DECLARE_PG_FUNCTION(duckdb_only_function) {
	char *function_name = DatumGetCString(DirectFunctionCall1(regprocout, fcinfo->flinfo->fn_oid));
	elog(ERROR, "Function '%s' only works with DuckDB execution", function_name);
}

} // extern "C"
