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

#include "pgduckdb/pgduckdb_process_lock.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

namespace pgduckdb {

void
PostgresScanGlobalState::InitGlobalState(duckdb::TableFunctionInitInput &input) {
	/* SELECT COUNT(*) FROM */
	if (input.column_ids.size() == 1 && input.column_ids[0] == UINT64_MAX) {
		m_count_tuples_only = true;
		return;
	}

	/*
	 * We need to read columns from the Postgres tuple in column order, but for
	 * outputting them we care about the DuckDB order. A map automatically
	 * orders them based on key, which in this case is the Postgres column
	 * order
	 */
	duckdb::map<AttrNumber, duckdb::idx_t> pg_column_order;
	duckdb::idx_t scan_index = 0;
	for (const auto &pg_column : input.column_ids) {
		/* Postgres AttrNumbers are 1-based */
		pg_column_order[pg_column + 1] = scan_index++;
	}

	auto table_filters = input.filters.get();
	m_column_filters.resize(input.column_ids.size(), 0);

	for (auto const &[att_num, duckdb_scanned_index] : pg_column_order) {
		m_columns_to_scan.emplace_back(att_num, duckdb_scanned_index);

		if (!table_filters) {
			continue;
		}

		auto column_filter_it = table_filters->filters.find(duckdb_scanned_index);
		if (column_filter_it != table_filters->filters.end()) {
			m_column_filters[duckdb_scanned_index] = column_filter_it->second.get();
		}
	}

	/* We need to check do we consider projection_ids or column_ids list to be used
	 * for writing to output vector. Projection ids list will be used when
	 * columns that are used for query filtering are not used afterwards; otherwise
	 * column ids list will be used and all read tuple columns need to passed
	 * to upper layers of query execution.
	 */
	if (input.CanRemoveFilterColumns()) {
		for (const auto &projection_id : input.projection_ids) {
			m_output_columns.emplace_back(projection_id, input.column_ids[projection_id] + 1);
		}
	} else {
		duckdb::idx_t output_index = 0;
		for (const auto &column_id : input.column_ids) {
			m_output_columns.emplace_back(output_index++, column_id + 1);
		}
	}
}

void
PostgresScanGlobalState::InitRelationMissingAttrs(TupleDesc tuple_desc) {
	std::lock_guard<std::mutex> lock(DuckdbProcessLock::GetLock());
	for (int attnum = 0; attnum < tuple_desc->natts; attnum++) {
		bool is_null = false;
		Datum attr = PostgresFunctionGuard(getmissingattr, tuple_desc, attnum + 1, &is_null);
		/* Add missing attr datum if not null*/
		if (!is_null) {
			m_relation_missing_attrs[attnum] = attr;
		}
	}
}

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
PostgresReplacementScan(duckdb::ClientContext &context, duckdb::ReplacementScanInput &input,
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
