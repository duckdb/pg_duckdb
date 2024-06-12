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
#include "utils/regproc.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
}

#include "quack/quack_heap_scan.hpp"
#include "quack/quack_types.hpp"

namespace quack {

//
// PostgresHeapScanFunctionData
//

PostgresHeapScanFunctionData::PostgresHeapScanFunctionData(PostgresHeapSeqScan &&relation, Snapshot snapshot)
	: m_relation(std::move(relation)) {
	m_relation.SetSnapshot(snapshot);
}

PostgresHeapScanFunctionData::~PostgresHeapScanFunctionData() {
}

//
// PostgresHeapScanGlobalState
//

PostgresHeapScanGlobalState::PostgresHeapScanGlobalState(PostgresHeapSeqScan &relation,
														 duckdb::TableFunctionInitInput &input) {
	elog(DEBUG3, "-- (DuckDB/PostgresHeapScanGlobalState) Running %llu threads -- ", MaxThreads());
	relation.InitParallelScanState(input);
}

PostgresHeapScanGlobalState::~PostgresHeapScanGlobalState() {
}

//
// PostgresHeapScanLocalState
//

PostgresHeapScanLocalState::PostgresHeapScanLocalState(PostgresHeapSeqScan &relation) : m_rel(relation) {
	m_thread_seq_scan_info.m_tuple.t_tableOid = RelationGetRelid(relation.GetRelation());
	m_thread_seq_scan_info.m_tuple_desc = RelationGetDescr(relation.GetRelation());
}

PostgresHeapScanLocalState::~PostgresHeapScanLocalState() {
	m_thread_seq_scan_info.EndScan();
}

//
// PostgresHeapScanFunction
//

PostgresHeapScanFunction::PostgresHeapScanFunction()
	: TableFunction("postgres_heap_scan", {}, PostgresHeapScanFunc, PostgresHeapBind, PostgresHeapInitGlobal,
					PostgresHeapInitLocal) {
	named_parameters["relid"] = duckdb::LogicalType::UINTEGER;
	named_parameters["snapshot"] = duckdb::LogicalType::POINTER;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
}

duckdb::unique_ptr<duckdb::FunctionData>
PostgresHeapScanFunction::PostgresHeapBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
										   duckdb::vector<duckdb::LogicalType> &return_types,
										   duckdb::vector<duckdb::string> &names) {
	auto relid = input.named_parameters["relid"].GetValue<uint32_t>();
	auto snapshot = (reinterpret_cast<Snapshot>(input.named_parameters["snapshot"].GetPointer()));

	auto rel = PostgresHeapSeqScan(relid);
	auto tupleDesc = RelationGetDescr(rel.GetRelation());

	if (!tupleDesc) {
		elog(ERROR, "Failed to get tuple descriptor for relation with OID %u", relid);
		return nullptr;
	}

	for (int i = 0; i < tupleDesc->natts; i++) {
		Form_pg_attribute attr = &tupleDesc->attrs[i];
		auto col_name = duckdb::string(NameStr(attr->attname));
		auto duck_type = ConvertPostgresToDuckColumnType(attr);
		return_types.push_back(duck_type);
		names.push_back(col_name);
		/* Log column name and type */
		elog(DEBUG3, "-- (DuckDB/PostgresHeapBind) Column name: %s, Type: %s --", col_name.c_str(),
			 duck_type.ToString().c_str());
	}
	return duckdb::make_uniq<PostgresHeapScanFunctionData>(std::move(rel), snapshot);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PostgresHeapScanFunction::PostgresHeapInitGlobal(duckdb::ClientContext &context,
												 duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresHeapScanFunctionData>();
	return duckdb::make_uniq<PostgresHeapScanGlobalState>(bind_data.m_relation, input);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PostgresHeapScanFunction::PostgresHeapInitLocal(duckdb::ExecutionContext &context,
												duckdb::TableFunctionInitInput &input,
												duckdb::GlobalTableFunctionState *gstate) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresHeapScanFunctionData>();
	return duckdb::make_uniq<PostgresHeapScanLocalState>(bind_data.m_relation);
}

void
PostgresHeapScanFunction::PostgresHeapScanFunc(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
											   duckdb::DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<PostgresHeapScanFunctionData>();
	auto &l_data = data_p.local_state->Cast<PostgresHeapScanLocalState>();

	auto &relation = bind_data.m_relation;
	auto &exhausted_scan = l_data.m_exhausted_scan;

	l_data.m_thread_seq_scan_info.m_output_vector_size = 0;

	/* We have exhausted seq scan of heap table so we can return */
	if (exhausted_scan) {
		output.SetCardinality(0);
		return;
	}

	auto has_tuple = relation.ReadPageTuples(output, l_data.m_thread_seq_scan_info);

	if (!has_tuple || l_data.m_thread_seq_scan_info.m_block_number == InvalidBlockNumber) {
		exhausted_scan = true;
	}
}

//
// PostgresHeapReplacementScan
//

static Oid
FindMatchingRelation(const duckdb::string &to_find) {
	RangeVar *tableRangeVar = makeRangeVarFromNameList(stringToQualifiedNameList(to_find.c_str(), NULL));
	Oid relOid = RangeVarGetRelid(tableRangeVar, AccessShareLock, true);
	if (relOid != InvalidOid) {
		return relOid;
	}
	return InvalidOid;
}

static duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>
CreateFunctionArguments(Oid relid, Snapshot snapshot) {
	duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
		duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("relid"),
		duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UINTEGER(relid))));

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
		duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("snapshot"),
		duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::POINTER(duckdb::CastPointerToValue(snapshot)))));
	return children;
}

duckdb::unique_ptr<duckdb::TableRef> ReplaceView(Oid view) {
	auto oid = ObjectIdGetDatum(view);
	Datum viewdef = DirectFunctionCall1(pg_get_viewdef, oid);
	auto view_definition = text_to_cstring(DatumGetTextP(viewdef));

	if (!view_definition) {
		elog(ERROR, "Could not retrieve view definition for Relation with relid: %u", view);
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

duckdb::unique_ptr<duckdb::TableRef>
PostgresHeapReplacementScan(duckdb::ClientContext &context, duckdb::ReplacementScanInput &input,
							duckdb::optional_ptr<duckdb::ReplacementScanData> data) {

	auto &table_name = input.table_name;
	auto &scan_data = reinterpret_cast<PostgresHeapReplacementScanData &>(*data);

	/* Check name against query table list and verify that it is heap table */
	auto relid = FindMatchingRelation(table_name);

	if (relid == InvalidOid) {
		return nullptr;
	}

	// Check if the Relation is a VIEW
	auto tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple)) {
		elog(ERROR, "Cache lookup failed for relation %u", relid);
	}

	auto relForm = (Form_pg_class) GETSTRUCT(tuple);

	// Check if the relation is a view
	if (relForm->relkind == RELKIND_VIEW) {
		ReleaseSysCache(tuple);
		return ReplaceView(relid);
	}
	ReleaseSysCache(tuple);

	auto rel = RelationIdGetRelation(relid);
	/* Allow only heap tables */
	if (!rel->rd_amhandler || (GetTableAmRoutine(rel->rd_amhandler) != GetHeapamTableAmRoutine())) {
		/* This doesn't have an access method handler, we cant read from this */
		RelationClose(rel);
		return nullptr;
	}
	RelationClose(rel);

	// Create POINTER values from the 'table' and 'snapshot' variables
	auto children = CreateFunctionArguments(relid, GetActiveSnapshot());
	auto table_function = duckdb::make_uniq<duckdb::TableFunctionRef>();
	table_function->function = duckdb::make_uniq<duckdb::FunctionExpression>("postgres_heap_scan", std::move(children));
	table_function->alias = table_name;

	return std::move(table_function);
}

} // namespace quack
