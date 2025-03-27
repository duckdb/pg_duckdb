
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
#include "pgduckdb/pgduckdb_userdata_cache.hpp"
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
	return std::string(text_to_cstring(DatumGetTextPP(datum)));
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

UrlStyle
StringToUrlStyle(const std::string &style) {
	auto uc_style = duckdb::StringUtil::Upper(style);
	if (uc_style == "PATH") {
		return UrlStyle::PATH;
	} else if (uc_style == "VHOST") {
		return UrlStyle::VIRTUAL_HOST;
	} else {
		throw std::runtime_error("Invalid URL style: '" + style + "'");
	}
}

std::string
UrlStyleToString(UrlStyle style) {
	switch (style) {
	case UrlStyle::PATH:
		return "path";
	case UrlStyle::VIRTUAL_HOST:
		return "vhost";
	default:
		throw std::runtime_error("Invalid URL style: '" + std::to_string(style) + "'");
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

		if (!is_null_array[Anum_duckdb_secret_url_style - 1])
			secret.url_style = StringToUrlStyle(DatumToString(datum_array[Anum_duckdb_secret_url_style - 1]));
		else
			secret.url_style = UrlStyle::UNDEFINED;

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

static void
DuckdbInstallExtension(const std::string &extension_name, const std::string &repository) {
	auto install_extension_command = "INSTALL " + duckdb::KeywordHelper::WriteQuoted(extension_name) + " FROM ";
	if (repository == "core" || repository == "core_nightly" || repository == "community" ||
	    repository == "local_build_debug" || repository == "local_build_release") {
		install_extension_command += repository;
	} else {
		install_extension_command += duckdb::KeywordHelper::WriteQuoted(repository);
	}

	/*
	 * Temporily allow all filesystems for this query, because INSTALL needs
	 * local filesystem access. Since this setting cannot be changed inside
	 * DuckDB after it's set to LocalFileSystem this temporary configuration
	 * change only really has effect duckdb.install_extension is called as the
	 * first DuckDB query for this session. Since we cannot change it back.
	 *
	 * While that's suboptimal it's also not a huge problem. Users only need to
	 * install an extension once on a server. So doing that on a new connection
	 * or after calling duckdb.recycle_ddb() should not be a big deal.
	 *
	 * NOTE: Because each backend has its own DuckDB instance, this setting
	 * does not impact other backends and thus cannot cause a security issue
	 * due to a race condition.
	 */
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("duckdb.disabled_filesystems", "", PGC_SUSET, PGC_S_SESSION);
	pgduckdb::DuckDBQueryOrThrow(install_extension_command);
	AtEOXact_GUC(false, save_nestlevel);
	Datum extension_name_datum = CStringGetTextDatum(extension_name.c_str());

	Oid arg_types[] = {TEXTOID};
	Datum values[] = {extension_name_datum};

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
}

namespace pg {

std::string
GetArgString(PG_FUNCTION_ARGS, int argno) {
	if (PG_NARGS() <= argno) {
		throw duckdb::InvalidInputException("argument %d is required", argno);
	}

	if (PG_ARGISNULL(argno)) {
		throw duckdb::InvalidInputException("argument %d cannot be NULL", argno);
	}

	return DatumToString(PG_GETARG_DATUM(argno));
}

} // namespace pg
} // namespace pgduckdb

extern "C" {

DECLARE_PG_FUNCTION(install_extension) {
	std::string extension_name = pgduckdb::pg::GetArgString(fcinfo, 0);
	std::string repository = pgduckdb::pg::GetArgString(fcinfo, 1);
	pgduckdb::DuckdbInstallExtension(extension_name, repository);
	PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(pgduckdb_raw_query) {
	const char *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
	auto result = pgduckdb::DuckDBQueryOrThrow(query);
	elog(NOTICE, "result: %s", result->ToString().c_str());
	PG_RETURN_BOOL(true);
}

DECLARE_PG_FUNCTION(pgduckdb_is_motherduck_enabled) {
	PG_RETURN_BOOL(pgduckdb::IsMotherDuckEnabled());
}

DECLARE_PG_FUNCTION(pgduckdb_enable_motherduck) {
	if (pgduckdb::IsMotherDuckEnabled()) {
		elog(NOTICE, "MotherDuck is already enabled");
		PG_RETURN_BOOL(false);
	}

	auto token = pgduckdb::pg::GetArgString(fcinfo, 0);
	auto default_database = pgduckdb::pg::GetArgString(fcinfo, 1);

	// If no token provided, check that token exists in the environment
	if (token == "::FROM_ENV::" && getenv("MOTHERDUCK_TOKEN") == nullptr && getenv("motherduck_token") == nullptr) {
		elog(ERROR, "No token was provided and `motherduck_token` environment variable was not set");
	}

	SPI_connect();

	if (pgduckdb::GetMotherduckForeignServerOid() == InvalidOid) {
		std::string query = "CREATE SERVER motherduck TYPE 'motherduck' FOREIGN DATA WRAPPER duckdb";
		if (default_database.empty()) {
			query += ";";
		} else {
			query += " OPTIONS (default_database " + duckdb::KeywordHelper::WriteQuoted(default_database) + ");";
		}
		auto ret = SPI_exec(query.c_str(), 0);
		if (ret != SPI_OK_UTILITY) {
			elog(ERROR, "Could not create 'motherduck' SERVER: %s", SPI_result_code_string(ret));
		}
	} else if (!default_database.empty()) {
		// TODO: check if it was set to the same value and update it or only error in that case
		elog(ERROR, "Cannot provide a default_database: because the server already exists");
	}

	{
		// Create mapping for current user
		auto query = "CREATE USER MAPPING FOR CURRENT_USER SERVER motherduck OPTIONS (token " +
		             duckdb::KeywordHelper::WriteQuoted(token) + ");";
		auto ret = SPI_exec(query.c_str(), 0);
		if (ret != SPI_OK_UTILITY) {
			elog(ERROR, "Could not create USER MAPPING for current user: %s", SPI_result_code_string(ret));
		}
	}

	SPI_finish();

	PG_RETURN_BOOL(pgduckdb::IsMotherDuckEnabled());
}

/*
 * We need these dummy cache functions so that people are able to load the
 * new pg_duckdb.so file with an old SQL version (where these functions still
 * exist). People should then upgrade the SQL part of the extension using the
 * command described in the error message. Once we believe no-one is on these old
 * version anymore we can remove these dummy functions.
 */
DECLARE_PG_FUNCTION(cache) {
	elog(ERROR, "duckdb.cache is not supported anymore, please run 'ALTER EXTENSION pg_duckdb UPDATE'");
}

DECLARE_PG_FUNCTION(cache_info) {
	elog(ERROR, "duckdb.cache_info is not supported anymore, please run 'ALTER EXTENSION pg_duckdb UPDATE'");
}

DECLARE_PG_FUNCTION(cache_delete) {
	elog(ERROR, "duckdb.cache_delete is not supported anymore, please run 'ALTER EXTENSION pg_duckdb UPDATE'");
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

DECLARE_PG_FUNCTION(duckdb_union_in) {
	elog(ERROR, "Creating the duckdb.union type is not supported");
}

DECLARE_PG_FUNCTION(duckdb_union_out) {
	return textout(fcinfo);
}

DECLARE_PG_FUNCTION(duckdb_map_in) {
	elog(ERROR, "Creating the duckdb.map type is not supported");
}

DECLARE_PG_FUNCTION(duckdb_map_out) {
	return textout(fcinfo);
}

} // extern "C"
