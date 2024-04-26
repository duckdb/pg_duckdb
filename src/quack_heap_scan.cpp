#include "duckdb/main/client_context.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"

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
	elog(DEBUG3, "-- (DuckDB/PostgresHeapScanGlobalState) Running %lu threads -- ", MaxThreads());
	relation.InitParallelScanState(input.column_ids, input.projection_ids, input.filters.get());
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
	named_parameters["table"] = duckdb::LogicalType::POINTER;
	named_parameters["snapshot"] = duckdb::LogicalType::POINTER;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
}

duckdb::unique_ptr<duckdb::FunctionData>
PostgresHeapScanFunction::PostgresHeapBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                           duckdb::vector<duckdb::string> &names) {
	auto table = (reinterpret_cast<RangeTblEntry *>(input.named_parameters["table"].GetPointer()));
	auto snapshot = (reinterpret_cast<Snapshot>(input.named_parameters["snapshot"].GetPointer()));

	auto rel = PostgresHeapSeqScan(table);
	auto tupleDesc = RelationGetDescr(rel.GetRelation());

	if (!tupleDesc) {
		elog(ERROR, "Failed to get tuple descriptor for relation with OID %u", table->relid);
		return nullptr;
	}

	for (int i = 0; i < tupleDesc->natts; i++) {
		Form_pg_attribute attr = &tupleDesc->attrs[i];
		Oid type_oid = attr->atttypid;
		auto col_name = duckdb::string(NameStr(attr->attname));
		auto duck_type = ConvertPostgresToDuckColumnType(type_oid);
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

static RangeTblEntry *
FindMatchingHeapRelation(List *tables, const duckdb::string &to_find) {
	ListCell *lc;
	foreach (lc, tables) {
		RangeTblEntry *table = (RangeTblEntry *)lfirst(lc);
		if (table->relid) {
			auto rel = RelationIdGetRelation(table->relid);

			if (!RelationIsValid(rel)) {
				elog(ERROR, "Relation with OID %u is not valid", table->relid);
				return nullptr;
			}

			char *rel_name = RelationGetRelationName(rel);
			auto table_name = std::string(rel_name);
			if (duckdb::StringUtil::CIEquals(table_name, to_find)) {
				/* Allow only heap tables */
				if (!rel->rd_amhandler || (GetTableAmRoutine(rel->rd_amhandler) != GetHeapamTableAmRoutine())) {
					/* This doesn't have an access method handler, we cant read from this */
					RelationClose(rel);
					return nullptr;
				} else {
					RelationClose(rel);
					return table;
				}
			}
			RelationClose(rel);
		}
	}
	return nullptr;
}

static duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>
CreateFunctionArguments(RangeTblEntry *table, Snapshot snapshot) {
	duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("table"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::POINTER(duckdb::CastPointerToValue(table)))));

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("snapshot"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::POINTER(duckdb::CastPointerToValue(snapshot)))));
	return children;
}

duckdb::unique_ptr<duckdb::TableRef>
PostgresHeapReplacementScan(duckdb::ClientContext &context, const duckdb::string &table_name,
                            duckdb::ReplacementScanData *data) {

	auto &scan_data = reinterpret_cast<PostgresHeapReplacementScanData &>(*data);

	/* Check name against query rtable list and verify that it is heap table */
	auto table = FindMatchingHeapRelation(scan_data.desc->plannedstmt->rtable, table_name);

	if (!table) {
		return nullptr;
	}

	// Create POINTER values from the 'table' and 'snapshot' variables
	auto children = CreateFunctionArguments(table, scan_data.desc->estate->es_snapshot);
	auto table_function = duckdb::make_uniq<duckdb::TableFunctionRef>();
	table_function->function = duckdb::make_uniq<duckdb::FunctionExpression>("postgres_heap_scan", std::move(children));

	return std::move(table_function);
}

} // namespace quack
