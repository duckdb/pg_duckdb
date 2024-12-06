#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "optimizer/planmain.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/syscache.h"
}

namespace pgduckdb {

static Oid
FindMatchingRelation(const duckdb::string &schema, const duckdb::string &table) {
	List *name_list = NIL;
	if (!schema.empty()) {
		name_list = lappend(name_list, makeString(pstrdup(schema.c_str())));
	}

	name_list = lappend(name_list, makeString(pstrdup(table.c_str())));

	RangeVar *table_range_var = makeRangeVarFromNameList(name_list);
	return RangeVarGetRelid(table_range_var, AccessShareLock, true);
}

const char *
pgduckdb_pg_get_viewdef(Oid view) {
	auto oid = ObjectIdGetDatum(view);
	Datum viewdef = DirectFunctionCall1(pg_get_viewdef, oid);
	return text_to_cstring(DatumGetTextP(viewdef));
}

duckdb::unique_ptr<duckdb::TableRef>
ReplaceView(Oid view) {
	const auto view_definition = PostgresFunctionGuard(pgduckdb_pg_get_viewdef, view);

	if (!view_definition) {
		throw duckdb::InvalidInputException("Could not retrieve view definition for Relation with relid: %u", view);
	}

	duckdb::Parser parser;
	parser.ParseQuery(view_definition);
	auto &statements = parser.statements;
	if (statements.size() != 1) {
		throw duckdb::InvalidInputException("View definition contained more than 1 statement!");
	}

	if (statements[0]->type != duckdb::StatementType::SELECT_STATEMENT) {
		throw duckdb::InvalidInputException("View definition (%s) did not contain a SELECT statement!",
		                                    view_definition);
	}

	auto select = duckdb::unique_ptr_cast<duckdb::SQLStatement, duckdb::SelectStatement>(std::move(statements[0]));
	return duckdb::make_uniq<duckdb::SubqueryRef>(std::move(select));
}

duckdb::unique_ptr<duckdb::TableRef>
PostgresViewScan(duckdb::ClientContext &context, duckdb::ReplacementScanInput &input,
                 duckdb::optional_ptr<duckdb::ReplacementScanData> data) {

	auto &schema_name = input.schema_name;
	auto &table_name = input.table_name;

	auto relid = PostgresFunctionGuard(FindMatchingRelation, schema_name, table_name);
	if (relid == InvalidOid) {
		return nullptr;
	}

	auto tuple = PostgresFunctionGuard(SearchSysCache1, RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple)) {
		elog(WARNING, "(PGDuckDB/PostgresReplacementScan) Cache lookup failed for relation %u", relid);
		return nullptr;
	}

	auto relForm = (Form_pg_class)GETSTRUCT(tuple);
	if (relForm->relkind != RELKIND_VIEW) {
		PostgresFunctionGuard(ReleaseSysCache, tuple);
		return nullptr;
	}

	PostgresFunctionGuard(ReleaseSysCache, tuple);
	return ReplaceView(relid);
}

} // namespace pgduckdb
