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
FindMatchingRelation(const duckdb::string &to_find) {
	RangeVar *table_range_var = makeRangeVarFromNameList(stringToQualifiedNameList(to_find.c_str(), NULL));
	Oid rel_oid = RangeVarGetRelid(table_range_var, AccessShareLock, true);
	if (rel_oid != InvalidOid) {
		return rel_oid;
	}
	return InvalidOid;
}

static duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>
CreateFunctionSeqScanArguments(uint64 cardinality, Oid relid, Snapshot snapshot) {
	duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("cardinality"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UBIGINT(cardinality))));

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("relid"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UINTEGER(relid))));

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("snapshot"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::POINTER(duckdb::CastPointerToValue(snapshot)))));

	return children;
}

static duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>
CreateFunctionIndexScanArguments(uint64_t cardinality, Path *path, PlannerInfo *plannerInfo, Snapshot snapshot) {
	duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("cardinality"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UBIGINT(cardinality))));

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("path"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::POINTER(duckdb::CastPointerToValue(path)))));

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("planner_info"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(
	        duckdb::Value::POINTER(duckdb::CastPointerToValue(plannerInfo)))));

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("snapshot"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::POINTER(duckdb::CastPointerToValue(snapshot)))));

	return children;
}

duckdb::unique_ptr<duckdb::TableRef>
ReplaceView(Oid view) {
	auto oid = ObjectIdGetDatum(view);
	Datum view_def = DirectFunctionCall1(pg_get_viewdef, oid);
	auto view_definiton = text_to_cstring(DatumGetTextP(view_def));

	if (!view_definiton) {
		elog(WARNING, "(PGDuckDB/ReplaceView) Could not retrieve view definition for Relation with relid: %u", view);
		return nullptr;
	}

	duckdb::Parser parser;
	parser.ParseQuery(view_definiton);
	auto statements = std::move(parser.statements);
	if (statements.size() != 1) {
		elog(WARNING, "(PGDuckDB/ReplaceView) View definition contained more than 1 statement!");
		return nullptr;
	}

	if (statements[0]->type != duckdb::StatementType::SELECT_STATEMENT) {
		elog(WARNING, "(PGDuckDB/ReplaceView) View definition (%s) did not contain a SELECT statement!",
		     view_definiton);
		return nullptr;
	}

	auto select = duckdb::unique_ptr_cast<duckdb::SQLStatement, duckdb::SelectStatement>(std::move(statements[0]));
	auto subquery = duckdb::make_uniq<duckdb::SubqueryRef>(std::move(select));
	return std::move(subquery);
}

static RelOptInfo *
FindMatchingRelEntry(Oid relid, PlannerInfo *planner_info) {
	int i = 1;
	RelOptInfo *node = nullptr;
	for (; i < planner_info->simple_rel_array_size; i++) {
		if (planner_info->simple_rte_array[i]->rtekind == RTE_SUBQUERY && planner_info->simple_rel_array[i]) {
			node = FindMatchingRelEntry(relid, planner_info->simple_rel_array[i]->subroot);
			if (node) {
				return node;
			}
		} else if (planner_info->simple_rte_array[i]->rtekind == RTE_RELATION) {
			if (relid == planner_info->simple_rte_array[i]->relid) {
				return planner_info->simple_rel_array[i];
			}
		};
	}
	return nullptr;
}

duckdb::unique_ptr<duckdb::TableRef>
PostgresReplacementScan(duckdb::ClientContext &context, duckdb::ReplacementScanInput &input,
                        duckdb::optional_ptr<duckdb::ReplacementScanData> data) {

	auto table_name = duckdb::ReplacementScan::GetFullPath(input);
	auto scan_data = context.registered_state->Get<PostgresReplacementScanDataClientContextState>("postgres_scan");
	if (!scan_data) {
		/* There is no scan data provided by postgres so we cannot access any
		 * of postgres tables. This is the case for queries that are not
		 * directly based on a Postgres query, such as queries that pg_duckdb
		 * executes internally like CREATE SECRET.
		 */
		return nullptr;
	}

	/* Check name against query table list and verify that it is heap table */
	auto relid = FindMatchingRelation(table_name);

	if (relid == InvalidOid) {
		return nullptr;
	}

	// Check if the Relation is a VIEW
	auto tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple)) {
		elog(WARNING, "(PGDuckDB/PostgresReplacementScan) Cache lookup failed for relation %u", relid);
		return nullptr;
	}

	auto relForm = (Form_pg_class)GETSTRUCT(tuple);

	// Check if the relation is a view
	if (relForm->relkind == RELKIND_VIEW) {
		ReleaseSysCache(tuple);
		return ReplaceView(relid);
	}
	ReleaseSysCache(tuple);

	RelOptInfo *node = nullptr;
	Path *node_path = nullptr;

	if (scan_data->m_query_planner_info) {
		node = FindMatchingRelEntry(relid, scan_data->m_query_planner_info);
		if (node) {
			node_path = get_cheapest_fractional_path(node, 0.0);
		}
	}

	/* SELECT query will have nodePath so we can return cardinality estimate of scan */
	Cardinality nodeCardinality = node_path ? node_path->rows : 1;

	if ((node_path != nullptr && (node_path->pathtype == T_IndexScan || node_path->pathtype == T_IndexOnlyScan))) {
		auto children = CreateFunctionIndexScanArguments(nodeCardinality, node_path, scan_data->m_query_planner_info,
		                                                 GetActiveSnapshot());
		auto table_function = duckdb::make_uniq<duckdb::TableFunctionRef>();
		table_function->function =
		    duckdb::make_uniq<duckdb::FunctionExpression>("postgres_index_scan", std::move(children));
		table_function->alias = table_name;
		return std::move(table_function);
	} else {
		auto children = CreateFunctionSeqScanArguments(nodeCardinality, relid, GetActiveSnapshot());
		auto table_function = duckdb::make_uniq<duckdb::TableFunctionRef>();
		table_function->function =
		    duckdb::make_uniq<duckdb::FunctionExpression>("postgres_seq_scan", std::move(children));
		table_function->alias = table_name;
		return std::move(table_function);
	}
	return nullptr;
}

} // namespace pgduckdb
