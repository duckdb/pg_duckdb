#include "pgduckdb/pgduckdb_external_tables.hpp"

#include <cctype>
#include <sstream>
#include <string>
#include <unordered_set>

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pg/memory.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/pgduckdb_table_am.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

namespace internal {

static Oid
PgduckdbExternalTablesRelid() {
	static Oid cached_schema_oid = InvalidOid;
	static Oid cached_relid = InvalidOid;

	if (OidIsValid(cached_relid)) {
		char *relname = get_rel_name(cached_relid);
		if (relname != NULL) {
			pfree(relname);
			return cached_relid;
		}
		cached_relid = InvalidOid;
	}

	if (!OidIsValid(cached_schema_oid)) {
		cached_schema_oid = get_namespace_oid("duckdb", true);
		if (!OidIsValid(cached_schema_oid)) {
			return InvalidOid;
		}
	}

	cached_relid = get_relname_relid("external_tables", cached_schema_oid);
	return cached_relid;
}

struct ExternalTableMetadata {
	ExternalTableMetadata() : reader(), location(), options(nullptr) {
	}

	ExternalTableMetadata(const ExternalTableMetadata &) = delete;
	ExternalTableMetadata &operator=(const ExternalTableMetadata &) = delete;

	std::string reader;
	std::string location;
	Jsonb *options;
};

static std::unordered_set<Oid> loaded_external_tables;

/*
 * Helper function to begin a system scan on the external_tables catalog.
 * Returns the scan descriptor and the opened relation.
 */
static inline SysScanDesc
BeginExternalTablesScan(Relation external_rel, Oid relid, Oid *index_oid_out) {
#if PG_VERSION_NUM >= 180000
	Oid index_oid = RelationGetPrimaryKeyIndex(external_rel, false);
#else
	Oid index_oid = RelationGetPrimaryKeyIndex(external_rel);
#endif
	bool has_index = OidIsValid(index_oid);

	ScanKeyData skey;
	ScanKeyInit(&skey, 1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

	if (index_oid_out) {
		*index_oid_out = index_oid;
	}

	return systable_beginscan(external_rel, index_oid, has_index, SnapshotSelf, 1, &skey);
}

void
AppendDuckdbOptions(std::ostringstream &oss, Jsonb *options) {
	if (options == nullptr) {
		return;
	}

	if (!JB_ROOT_IS_OBJECT(options)) {
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("duckdb_external_options must be a JSON object")));
	}

	JsonbIterator *it = JsonbIteratorInit(&options->root);
	JsonbValue v;
	JsonbValue key = {};
	bool expecting_value = false;
	bool skip_nested = false;
	while (true) {
		int r = JsonbIteratorNext(&it, &v, skip_nested);
		if (r == WJB_DONE) {
			break;
		}

		if (r == WJB_BEGIN_OBJECT || r == WJB_END_OBJECT) {
			expecting_value = false;
			skip_nested = false;
			continue;
		}

		if (!expecting_value && r == WJB_KEY) {
			key = v;
			expecting_value = true;
			skip_nested = true;
			continue;
		}

		if (!expecting_value || r != WJB_VALUE) {
			ereport(ERROR,
			        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid duckdb_external_options structure")));
		}

		expecting_value = false;
		skip_nested = false;
		std::string key_str(key.val.string.val, key.val.string.len);
		oss << ", " << key_str << " = ";

		switch (v.type) {
		case jbvNull:
			oss << "NULL";
			break;
		case jbvBool:
			oss << (v.val.boolean ? "true" : "false");
			break;
		case jbvString: {
			char *value = pnstrdup(v.val.string.val, v.val.string.len);
			char *quoted = quote_literal_cstr(value);
			oss << quoted;
			pfree(quoted);
			pfree(value);
			break;
		}
		case jbvNumeric: {
			Datum numeric_text = DirectFunctionCall1(numeric_out, NumericGetDatum(v.val.numeric));
			oss << DatumGetCString(numeric_text);
			pfree(DatumGetPointer(numeric_text));
			break;
		}
		case jbvBinary:
		case jbvArray:
		case jbvObject: {
			JsonbValue container_value = v;
			Jsonb *container_json = JsonbValueToJsonb(&container_value);
			StringInfoData json_text;
			initStringInfo(&json_text);
			JsonbToCString(&json_text, &container_json->root, VARSIZE(container_json));
			char *quoted = quote_literal_cstr(json_text.data);
			oss << quoted;
			pfree(quoted);
			pfree(json_text.data);
			pfree(container_json);
			break;
		}
		default:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			                errmsg("unsupported value type in duckdb_external_options for key \"%s\"", key_str.c_str()),
			                errhint("Only string, numeric, boolean, null, array and object values are supported.")));
		}
	}
}

bool
FetchExternalTableMetadata(Oid relid, ExternalTableMetadata &out) {
	MemoryContext caller_context = CurrentMemoryContext;
	Oid external_relid = PgduckdbExternalTablesRelid();
	if (!OidIsValid(external_relid)) {
		return false;
	}

	Relation external_rel = table_open(external_relid, AccessShareLock);
	SysScanDesc scan = BeginExternalTablesScan(external_rel, relid, nullptr);
	HeapTuple tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple)) {
		systable_endscan(scan);
		table_close(external_rel, AccessShareLock);
		return false;
	}

	TupleDesc tupdesc = RelationGetDescr(external_rel);
	bool isnull = false;
	Datum reader_datum = heap_getattr(tuple, 2, tupdesc, &isnull);
	if (isnull) {
		systable_endscan(scan);
		table_close(external_rel, AccessShareLock);
		ereport(ERROR,
		        (errmsg("external table metadata is incomplete for relation %u", static_cast<unsigned int>(relid))));
	}
	Datum location_datum = heap_getattr(tuple, 3, tupdesc, &isnull);
	if (isnull) {
		systable_endscan(scan);
		table_close(external_rel, AccessShareLock);
		ereport(ERROR,
		        (errmsg("external table metadata is incomplete for relation %u", static_cast<unsigned int>(relid))));
	}

	char *reader_cstr = text_to_cstring(DatumGetTextPP(reader_datum));
	char *location_cstr = text_to_cstring(DatumGetTextPP(location_datum));
	out.reader.assign(reader_cstr);
	out.location.assign(location_cstr);
	pfree(reader_cstr);
	pfree(location_cstr);

	Datum options_datum = heap_getattr(tuple, 4, tupdesc, &isnull);
	out.options = nullptr;
	if (!isnull) {
		MemoryContext oldcontext = pgduckdb::pg::MemoryContextSwitchTo(caller_context);
		out.options = DatumGetJsonbPCopy(options_datum);
		pgduckdb::pg::MemoryContextSwitchTo(oldcontext);
	}

	systable_endscan(scan);
	table_close(external_rel, AccessShareLock);
	return true;
}

void
UpsertExternalTableCatalog(Relation external_rel, Oid relid, const char *reader, const char *location, Jsonb *options) {
	TupleDesc tupdesc = RelationGetDescr(external_rel);
	Datum values[4];
	bool nulls[4];

	values[0] = ObjectIdGetDatum(relid);
	nulls[0] = false;

	values[1] = CStringGetTextDatum(reader);
	nulls[1] = false;

	values[2] = CStringGetTextDatum(location);
	nulls[2] = false;

	if (options) {
		values[3] = JsonbPGetDatum(options);
		nulls[3] = false;
	} else {
		values[3] = (Datum)0;
		nulls[3] = true;
	}

	/* Check if tuple already exists */
	SysScanDesc scan = BeginExternalTablesScan(external_rel, relid, nullptr);
	HeapTuple old_tuple = systable_getnext(scan);

	if (HeapTupleIsValid(old_tuple)) {
		/* Update existing tuple */
		HeapTuple new_tuple = heap_form_tuple(tupdesc, values, nulls);
		PostgresFunctionGuard(CatalogTupleUpdate, external_rel, &old_tuple->t_self, new_tuple);
		heap_freetuple(new_tuple);
	} else {
		/* Insert new tuple */
		HeapTuple new_tuple = heap_form_tuple(tupdesc, values, nulls);
		PostgresFunctionGuard(CatalogTupleInsert, external_rel, new_tuple);
		heap_freetuple(new_tuple);
	}

	systable_endscan(scan);
	CommandCounterIncrement();
}

void
DeleteExternalTableCatalog(Relation external_rel, Oid relid) {
	SysScanDesc scan = BeginExternalTablesScan(external_rel, relid, nullptr);
	HeapTuple tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple)) {
		PostgresFunctionGuard(CatalogTupleDelete, external_rel, &tuple->t_self);
	}

	systable_endscan(scan);
	CommandCounterIncrement();
}

} // namespace internal

namespace pgduckdb {

std::string
BuildExternalTableFunctionCall(const char *reader, const char *location, void *raw_options) {
	Jsonb *options = (Jsonb *)raw_options;
	char *quoted_location = quote_literal_cstr(location);
	std::ostringstream oss;
	oss << reader << '(' << quoted_location;
	pfree(quoted_location);
	internal::AppendDuckdbOptions(oss, options);
	oss << ')';
	return oss.str();
}

void
EnsureExternalTablesCatalogExists() {
	if (!OidIsValid(internal::PgduckdbExternalTablesRelid())) {
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("required catalog duckdb.external_tables is missing"),
		                errhint("Upgrade the pg_duckdb extension to version 1.1.0 or newer.")));
	}
}

bool
EnsureExternalTableLoaded(Oid relid) {
	if (!OidIsValid(relid)) {
		return false;
	}

	if (internal::loaded_external_tables.find(relid) != internal::loaded_external_tables.end()) {
		return true;
	}

	if (!pgduckdb_is_external_relation(relid)) {
		return false;
	}

	internal::ExternalTableMetadata metadata;
	if (!internal::FetchExternalTableMetadata(relid, metadata)) {
		return false;
	}

	/* We need the schema qualified name and schema path for DuckDB */
	char *qualified_relation_name = pgduckdb_relation_name(relid);

	Relation rel = RelationIdGetRelation(relid);
	if (rel == nullptr) {
		if (metadata.options) {
			pfree(metadata.options);
		}
		pfree(qualified_relation_name);
		return false;
	}

	const char *postgres_schema_name = nullptr;
#if PG_VERSION_NUM >= 140000
	postgres_schema_name = get_namespace_name_or_temp(rel->rd_rel->relnamespace);
#else
	postgres_schema_name = get_namespace_name(rel->rd_rel->relnamespace);
#endif

	const char *duckdb_table_am_name = DuckdbTableAmGetName(rel->rd_tableam);
	RelationClose(rel);
	if (duckdb_table_am_name == nullptr) {
		if (metadata.options) {
			pfree(metadata.options);
		}
		pfree(qualified_relation_name);
		ereport(ERROR, (errmsg("relation %u is not managed by the DuckDB table access method",
		                       static_cast<unsigned int>(relid))));
	}

	const char *duckdb_schema_name = pgduckdb_db_and_schema_string(postgres_schema_name, duckdb_table_am_name, true);

	std::string schema_query = std::string("CREATE SCHEMA IF NOT EXISTS ") + duckdb_schema_name + ";";
	std::string function_call =
	    BuildExternalTableFunctionCall(metadata.reader.c_str(), metadata.location.c_str(), metadata.options);
	std::string view_query =
	    std::string("CREATE OR REPLACE VIEW ") + qualified_relation_name + " AS SELECT * FROM " + function_call;

	if (metadata.options) {
		pfree(metadata.options);
	}
	pfree(qualified_relation_name);
	pfree((void *)duckdb_schema_name);

	auto connection = DuckDBManager::GetConnection(true);
	DuckDBQueryOrThrow(*connection, schema_query);
	DuckDBQueryOrThrow(*connection, view_query);

	internal::loaded_external_tables.insert(relid);
	return true;
}

bool
pgduckdb_is_external_relation(Oid relation_oid) {
	if (!OidIsValid(relation_oid)) {
		return false;
	}

	Oid external_relid = internal::PgduckdbExternalTablesRelid();
	if (!OidIsValid(external_relid)) {
		return false;
	}

	Relation external_rel = table_open(external_relid, AccessShareLock);
	SysScanDesc scan = internal::BeginExternalTablesScan(external_rel, relation_oid, nullptr);
	HeapTuple tuple = systable_getnext(scan);
	bool is_external = HeapTupleIsValid(tuple);
	systable_endscan(scan);
	table_close(external_rel, AccessShareLock);
	return is_external;
}

void
RegisterExternalTableDependency(Oid relid) {
	Oid external_relid = internal::PgduckdbExternalTablesRelid();
	if (!OidIsValid(external_relid)) {
		return;
	}

	ObjectAddress depender = {
	    .classId = RelationRelationId,
	    .objectId = relid,
	    .objectSubId = 0,
	};
	ObjectAddress referenced = {
	    .classId = RelationRelationId,
	    .objectId = external_relid,
	    .objectSubId = 0,
	};
	recordDependencyOn(&depender, &referenced, DEPENDENCY_AUTO);
}

void
ForgetLoadedExternalTable(Oid relid) {
	internal::loaded_external_tables.erase(relid);
}

void
ResetLoadedExternalTableCache() {
	internal::loaded_external_tables.clear();
}

void
UpsertExternalTableMetadata(Oid relid, const char *reader, const char *location, Datum options_datum) {
	Jsonb *options = options_datum ? DatumGetJsonbP(options_datum) : nullptr;
	Oid external_relid = internal::PgduckdbExternalTablesRelid();
	if (!OidIsValid(external_relid)) {
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("required catalog duckdb.external_tables is missing"),
		                errhint("Upgrade the pg_duckdb extension to version 1.1.0 or newer.")));
	}

	Relation external_rel = table_open(external_relid, RowExclusiveLock);

	bool reset_force_writes = !pgduckdb::pg::AllowWrites();
	if (reset_force_writes) {
		pgduckdb::pg::SetForceAllowWrites(true);
	}

	internal::UpsertExternalTableCatalog(external_rel, relid, reader, location, options);

	if (reset_force_writes) {
		pgduckdb::pg::SetForceAllowWrites(false);
	}

	table_close(external_rel, RowExclusiveLock);
}

void
DeleteExternalTableMetadata(Oid relid) {
	Oid external_relid = internal::PgduckdbExternalTablesRelid();
	if (!OidIsValid(external_relid)) {
		return;
	}

	Relation external_rel = table_open(external_relid, RowExclusiveLock);

	bool reset_force_writes = !pgduckdb::pg::AllowWrites();
	if (reset_force_writes) {
		pgduckdb::pg::SetForceAllowWrites(true);
	}

	internal::DeleteExternalTableCatalog(external_rel, relid);

	if (reset_force_writes) {
		pgduckdb::pg::SetForceAllowWrites(false);
	}

	table_close(external_rel, RowExclusiveLock);
}

} // namespace pgduckdb
