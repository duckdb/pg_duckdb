#include "pgduckdb/pgduckdb_external_tables.hpp"

#include <cctype>
#include <initializer_list>
#include <cstring>
#include <sstream>
#include <string>
#include <unordered_set>

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"
#include "pgduckdb/vendor/pg_list.hpp"

extern "C" {
#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

namespace {

const char *const EXTERNAL_SERVER_NAME = "ddb_s3_server";

struct ExternalTableMetadata {
	ExternalTableMetadata() : reader(), location(), format(), options(nullptr) {
	}

	ExternalTableMetadata(const ExternalTableMetadata &) = delete;
	ExternalTableMetadata &operator=(const ExternalTableMetadata &) = delete;

	std::string reader;
	std::string location;
	std::string format;
	Jsonb *options;
};

static std::unordered_set<Oid> loaded_external_tables;

bool
OptionNameMatches(const char *name, std::initializer_list<const char *> candidates) {
	for (const char *candidate : candidates) {
		if (pg_strcasecmp(name, candidate) == 0) {
			return true;
		}
	}
	return false;
}

std::string
LowercaseCopy(const char *value) {
	if (value == nullptr) {
		return {};
	}
	std::string result(value);
	for (char &c : result) {
		c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
	}
	return result;
}

void
AppendDuckdbOptions(std::ostringstream &oss, Jsonb *options) {
	if (options == nullptr) {
		return;
	}

	if (!JB_ROOT_IS_OBJECT(options)) {
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("duckdb external options must be a JSON object")));
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
			        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid duckdb external options structure")));
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
			                errmsg("unsupported value type in duckdb external options for key \"%s\"", key_str.c_str()),
			                errhint("Only string, numeric, boolean, null, array and object values are supported.")));
		}
	}
}

void
PopulateMetadataFromOptions(List *options_list, ExternalTableMetadata &out) {
	foreach_node(DefElem, def, options_list) {
		const char *value = nullptr;
		if (def->arg) {
			value = defGetString(def);
		}
		if (value == nullptr) {
			continue;
		}

		if (OptionNameMatches(def->defname, {"location"})) {
			out.location = value;
		} else if (OptionNameMatches(def->defname, {"reader"})) {
			out.reader = value;
		} else if (OptionNameMatches(def->defname, {"format"})) {
			out.format = LowercaseCopy(value);
		} else if (OptionNameMatches(def->defname, {"options"})) {
			if (out.options != nullptr) {
				pfree(out.options);
				out.options = nullptr;
			}
			const char *json_value = (*value == '\0') ? "{}" : value;
			Datum json_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(json_value));
			out.options = DatumGetJsonbP(json_datum);
		} else if (pg_strncasecmp(def->defname, "duckdb_external_", strlen("duckdb_external_")) == 0) {
			ereport(ERROR,
			        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			         errmsg("option \"%s\" is no longer supported; use location/reader/format/options", def->defname)));
		}
	}

	if (out.reader.empty()) {
		if (out.format.empty() || out.format == "parquet") {
			out.reader = "read_parquet";
			if (out.format.empty()) {
				out.format = "parquet";
			}
		} else if (out.format == "csv") {
			out.reader = "read_csv";
		} else if (out.format == "json") {
			out.reader = "read_json";
		}
	}
}

bool
LoadExternalTableMetadata(Oid relid, ExternalTableMetadata &out) {
	ForeignTable *ft = GetForeignTable(relid);
	if (ft == nullptr) {
		return false;
	}

	PopulateMetadataFromOptions(ft->options, out);

	if (out.reader.empty() || out.location.empty()) {
		return false;
	}

	return true;
}

} // namespace

namespace pgduckdb {

const char *
ExternalTableServerName() {
	return EXTERNAL_SERVER_NAME;
}

std::string
BuildExternalTableFunctionCall(const char *reader, const char *location, void *raw_options) {
	Jsonb *options = (Jsonb *)raw_options;
	char *quoted_location = quote_literal_cstr(location);
	std::ostringstream oss;
	oss << reader << '(' << quoted_location;
	pfree(quoted_location);
	AppendDuckdbOptions(oss, options);
	oss << ')';
	return oss.str();
}

bool
EnsureExternalTableLoaded(Oid relid) {
	if (!OidIsValid(relid)) {
		return false;
	}

	if (loaded_external_tables.find(relid) != loaded_external_tables.end()) {
		return true;
	}

	if (!pgduckdb_is_external_relation(relid)) {
		return false;
	}

	ExternalTableMetadata metadata;
	if (!LoadExternalTableMetadata(relid, metadata)) {
		return false;
	}

	Relation rel = RelationIdGetRelation(relid);
	if (rel == nullptr) {
		if (metadata.options) {
			pfree(metadata.options);
		}
		return false;
	}

	const char *postgres_schema_name = nullptr;
#if PG_VERSION_NUM >= 140000
	postgres_schema_name = get_namespace_name_or_temp(rel->rd_rel->relnamespace);
#else
	postgres_schema_name = get_namespace_name(rel->rd_rel->relnamespace);
#endif
	const char *relname = RelationGetRelationName(rel);
	RelationClose(rel);
	const char *duckdb_schema_name = pgduckdb_db_and_schema_string(postgres_schema_name, "duckdb", true);

	// Construct the qualified view name using duckdb schema (not postgres schema)
	char *qualified_view_name = psprintf("%s.%s", duckdb_schema_name, quote_identifier(relname));
	std::string schema_query = std::string("CREATE SCHEMA IF NOT EXISTS ") + duckdb_schema_name + ";";
	std::string function_call =
	    BuildExternalTableFunctionCall(metadata.reader.c_str(), metadata.location.c_str(), metadata.options);
	std::string view_query =
	    std::string("CREATE OR REPLACE VIEW ") + qualified_view_name + " AS SELECT * FROM " + function_call;

	if (metadata.options) {
		pfree(metadata.options);
	}
	pfree(qualified_view_name);
	pfree((void *)duckdb_schema_name);

	auto connection = DuckDBManager::GetConnection();
	elog(INFO, "Creating DuckDB schema: %s", schema_query.c_str());
	DuckDBQueryOrThrow(*connection, schema_query);
	elog(INFO, "Creating DuckDB view: %s", view_query.c_str());
	DuckDBQueryOrThrow(*connection, view_query);

	loaded_external_tables.insert(relid);
	return true;
}

void
ForgetLoadedExternalTable(Oid relid) {
	loaded_external_tables.erase(relid);
}

void
ResetLoadedExternalTableCache() {
	loaded_external_tables.clear();
}

bool
pgduckdb_is_external_relation(Oid relation_oid) {
	if (!OidIsValid(relation_oid)) {
		return false;
	}

	Relation rel = RelationIdGetRelation(relation_oid);
	if (rel == nullptr) {
		return false;
	}

	bool is_external = false;
	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
		ForeignTable *ft = GetForeignTable(relation_oid);
		if (ft != nullptr) {
			ForeignServer *server = GetForeignServer(ft->serverid);
			if (server != nullptr && server->servername != nullptr &&
			    strcmp(server->servername, EXTERNAL_SERVER_NAME) == 0) {
				is_external = true;
			}
		}
	}

	RelationClose(rel);
	return is_external;
}

} // namespace pgduckdb
