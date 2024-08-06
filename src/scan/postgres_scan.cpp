#include "duckdb/main/client_context.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/enums/expression_type.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
}

#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

namespace pgduckdb {

void
PostgresScanGlobalState::InitGlobalState(duckdb::TableFunctionInitInput &input) {
	/* SELECT COUNT(*) FROM */
	if (input.column_ids.size() == 1 && input.column_ids[0] == UINT64_MAX) {
		m_count_tuples_only = true;
		return;
	}

	/* We need ordered columns ids for tuple fetch */
	for (duckdb::idx_t i = 0; i < input.column_ids.size(); i++) {
		m_columns[input.column_ids[i]] = i;
	}

	if (input.CanRemoveFilterColumns()) {
		for (duckdb::idx_t i = 0; i < input.projection_ids.size(); i++) {
			m_projections[i] = input.column_ids[input.projection_ids[i]];
		}
	} else {
		for (duckdb::idx_t i = 0; i < input.projection_ids.size(); i++) {
			m_projections[i] = input.column_ids[i];
		}
	}

	m_filters = input.filters.get();
}

static Oid
FindMatchingRelation(const duckdb::string &table) {
	RangeVar *tableRangeVar = makeRangeVarFromNameList(stringToQualifiedNameList(table.c_str(), NULL));
	Oid relOid = RangeVarGetRelid(tableRangeVar, AccessShareLock, true);
	if (relOid != InvalidOid) {
		return relOid;
	}
	return InvalidOid;
}

duckdb::unique_ptr<duckdb::TableRef>
PostgresReplacementScan(duckdb::ClientContext &context, duckdb::ReplacementScanInput &input,
                        duckdb::optional_ptr<duckdb::ReplacementScanData> data) {

	auto &table_name = input.table_name;
	auto &scan_data = reinterpret_cast<PostgresReplacementScanData &>(*data);

	auto relid = FindMatchingRelation(table_name);

	if (relid == InvalidOid) {
		return nullptr;
	}

	auto tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple)) {
		elog(ERROR, "Cache lookup failed for relation %u", relid);
	}

	auto relForm = (Form_pg_class)GETSTRUCT(tuple);

	if (relForm->relkind != RELKIND_VIEW) {
		ReleaseSysCache(tuple);
		return nullptr;
	}
	ReleaseSysCache(tuple);

	auto oid = ObjectIdGetDatum(relid);
	Datum viewdef = DirectFunctionCall1(pg_get_viewdef, oid);
	auto view_definition = text_to_cstring(DatumGetTextP(viewdef));

	if (!view_definition) {
		elog(ERROR, "Could not retrieve view definition for Relation with relid: %u", relid);
	}

	duckdb::Parser parser;
	parser.ParseQuery(view_definition);
	auto statements = std::move(parser.statements);
	if (statements.size() != 1) {
		elog(ERROR, "View definition contained more than 1 statement!");
	}

	if (statements[0]->type != duckdb::StatementType::SELECT_STATEMENT) {
		elog(ERROR, "View definition (%s) did not contain a SELECT statement!", view_definition);
	}

	auto select = duckdb::unique_ptr_cast<duckdb::SQLStatement, duckdb::SelectStatement>(std::move(statements[0]));
	auto subquery = duckdb::make_uniq<duckdb::SubqueryRef>(std::move(select));
	return std::move(subquery);
}

} // namespace pgduckdb
