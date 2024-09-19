#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "executor/nodeIndexscan.h"
#include "nodes/pathnodes.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/rel.h"
}

#include "pgduckdb/scan/index_scan_utils.hpp"
#include "pgduckdb/scan/postgres_index_scan.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/vendor/pg_list.hpp"

namespace pgduckdb {

//
// PostgresIndexScanGlobalState
//

PostgresIndexScanGlobalState::PostgresIndexScanGlobalState(IndexScanState *indexScanState, Relation relation,
                                                           duckdb::TableFunctionInitInput &input)
    : m_global_state(duckdb::make_shared_ptr<PostgresScanGlobalState>()), m_index_scan(indexScanState),
      m_relation(relation) {
	m_global_state->InitGlobalState(input);
	m_global_state->m_tuple_desc = RelationGetDescr(m_relation);
}

PostgresIndexScanGlobalState::~PostgresIndexScanGlobalState() {
	index_close(m_index_scan->iss_RelationDesc, NoLock);
	RelationClose(m_relation);
}

//
// PostgresIndexScanLocalState
//

PostgresIndexScanLocalState::PostgresIndexScanLocalState(IndexScanDesc index_scan_desc, Relation relation)
    : m_local_state(duckdb::make_shared_ptr<PostgresScanLocalState>()), m_index_scan_desc(index_scan_desc),
      m_relation(relation) {
	m_slot = MakeTupleTableSlot(CreateTupleDescCopy(RelationGetDescr(m_relation)), &TTSOpsBufferHeapTuple);
}

PostgresIndexScanLocalState::~PostgresIndexScanLocalState() {
	index_endscan(m_index_scan_desc);
}

//
// PostgresIndexScanFunctionData
//

PostgresIndexScanFunctionData::PostgresIndexScanFunctionData(uint64_t cardinality, Path *path,
                                                             PlannerInfo *planner_info, Oid relation_oid,
                                                             Snapshot snapshot)
    : m_cardinality(cardinality), m_path(path), m_planner_info(planner_info), m_snapshot(snapshot),
      m_relation_oid(relation_oid) {
}

PostgresIndexScanFunctionData::~PostgresIndexScanFunctionData() {
}

//
// PostgresSeqScanFunction
//

PostgresIndexScanFunction::PostgresIndexScanFunction()
    : TableFunction("postgres_index_scan", {}, PostgresIndexScanFunc, PostgresIndexScanBind,
                    PostgresIndexScanInitGlobal, PostgresIndexScanInitLocal) {
	named_parameters["cardinality"] = duckdb::LogicalType::UBIGINT;
	named_parameters["path"] = duckdb::LogicalType::POINTER;
	named_parameters["planner_info"] = duckdb::LogicalType::POINTER;
	named_parameters["snapshot"] = duckdb::LogicalType::POINTER;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
	cardinality = PostgresIndexScanCardinality;
}

duckdb::unique_ptr<duckdb::FunctionData>
PostgresIndexScanFunction::PostgresIndexScanBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                 duckdb::vector<duckdb::LogicalType> &return_types,
                                                 duckdb::vector<duckdb::string> &names) {
	auto cardinality = input.named_parameters["cardinality"].GetValue<uint64_t>();
	auto path = (reinterpret_cast<Path *>(input.named_parameters["path"].GetPointer()));
	auto planner_info = (reinterpret_cast<PlannerInfo *>(input.named_parameters["planner_info"].GetPointer()));
	auto snapshot = (reinterpret_cast<Snapshot>(input.named_parameters["snapshot"].GetPointer()));

	RangeTblEntry *rte = planner_rt_fetch(path->parent->relid, planner_info);

	auto rel = RelationIdGetRelation(rte->relid);
	auto relation_descr = RelationGetDescr(rel);

	if (!relation_descr) {
		elog(WARNING, "(PGDuckDB/PostgresIndexScanBind) Failed to get tuple descriptor for relation with OID %u",
		     rel->rd_id);
		return nullptr;
	}

	for (int i = 0; i < relation_descr->natts; i++) {
		Form_pg_attribute attr = &relation_descr->attrs[i];
		auto col_name = duckdb::string(NameStr(attr->attname));
		auto duck_type = ConvertPostgresToDuckColumnType(attr);
		return_types.push_back(duck_type);
		names.push_back(col_name);
		/* Log column name and type */
		elog(DEBUG2, "(PGDuckDB/PostgresIndexScanBind) Column name: %s, Type: %s --", col_name.c_str(),
		     duck_type.ToString().c_str());
	}

	RelationClose(rel);

	return duckdb::make_uniq<PostgresIndexScanFunctionData>(cardinality, path, planner_info, rte->relid, snapshot);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PostgresIndexScanFunction::PostgresIndexScanInitGlobal(duckdb::ClientContext &context,
                                                       duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresIndexScanFunctionData>();

	IndexScanState *index_state = makeNode(IndexScanState);
	IndexPath *index_path = (IndexPath *)bind_data.m_path;

	index_state->iss_RelationDesc = index_open(index_path->indexinfo->indexoid, AccessShareLock);
	index_state->iss_RuntimeKeysReady = false;
	index_state->iss_RuntimeKeys = NULL;
	index_state->iss_NumRuntimeKeys = 0;

	List *stripped_clause_list = NIL;
	IndexOptInfo *index = index_path->indexinfo;

	foreach_node(IndexClause, iclause, index_path->indexclauses) {
		int indexcol = iclause->indexcol;
		foreach_node(RestrictInfo, rinfo, iclause->indexquals) {
			Node *clause = (Node *)rinfo->clause;
			clause = FixIndexQualClause(bind_data.m_planner_info, index, indexcol, clause, iclause->indexcols);
			stripped_clause_list = lappend(stripped_clause_list, clause);
		}
	}

	ExecIndexBuildScanKeys((PlanState *)index_state, index_state->iss_RelationDesc, stripped_clause_list, false,
	                       &index_state->iss_ScanKeys, &index_state->iss_NumScanKeys, &index_state->iss_RuntimeKeys,
	                       &index_state->iss_NumRuntimeKeys, NULL, /* no ArrayKeys */
	                       NULL);

	return duckdb::make_uniq<PostgresIndexScanGlobalState>(index_state, RelationIdGetRelation(bind_data.m_relation_oid),
	                                                       input);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PostgresIndexScanFunction::PostgresIndexScanInitLocal(duckdb::ExecutionContext &context,
                                                      duckdb::TableFunctionInitInput &input,
                                                      duckdb::GlobalTableFunctionState *gstate) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresIndexScanFunctionData>();
	auto global_state = reinterpret_cast<PostgresIndexScanGlobalState *>(gstate);

	IndexScanDesc scandesc =
	    index_beginscan(global_state->m_relation, global_state->m_index_scan->iss_RelationDesc, bind_data.m_snapshot,
	                    global_state->m_index_scan->iss_NumScanKeys, global_state->m_index_scan->iss_NumOrderByKeys);

	if (global_state->m_index_scan->iss_NumRuntimeKeys == 0 || global_state->m_index_scan->iss_RuntimeKeysReady)
		index_rescan(scandesc, global_state->m_index_scan->iss_ScanKeys, global_state->m_index_scan->iss_NumScanKeys,
		             global_state->m_index_scan->iss_OrderByKeys, global_state->m_index_scan->iss_NumOrderByKeys);

	return duckdb::make_uniq<PostgresIndexScanLocalState>(scandesc, global_state->m_relation);
}

void
PostgresIndexScanFunction::PostgresIndexScanFunc(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
                                                 duckdb::DataChunk &output) {
	auto &local_state = data.local_state->Cast<PostgresIndexScanLocalState>();
	auto &global_state = data.global_state->Cast<PostgresIndexScanGlobalState>();
	bool next_index_tuple = false;

	local_state.m_local_state->m_output_vector_size = 0;

	if (local_state.m_local_state->m_exhausted_scan) {
		output.SetCardinality(0);
		return;
	}

	while (local_state.m_local_state->m_output_vector_size < STANDARD_VECTOR_SIZE &&
	       (next_index_tuple =
	            index_getnext_slot(local_state.m_index_scan_desc, ForwardScanDirection, local_state.m_slot))) {
		bool should_free;
		auto tuple = ExecFetchSlotHeapTuple(local_state.m_slot, false, &should_free);
		InsertTupleIntoChunk(output, global_state.m_global_state, local_state.m_local_state, tuple);
		ExecClearTuple(local_state.m_slot);
	}

	if (!next_index_tuple) {
		local_state.m_local_state->m_exhausted_scan = true;
	}

	output.SetCardinality(local_state.m_local_state->m_output_vector_size);
}

duckdb::unique_ptr<duckdb::NodeStatistics>
PostgresIndexScanFunction::PostgresIndexScanCardinality(duckdb::ClientContext &context,
                                                        const duckdb::FunctionData *data) {
	auto &bind_data = data->Cast<PostgresIndexScanFunctionData>();
	return duckdb::make_uniq<duckdb::NodeStatistics>(bind_data.m_cardinality, bind_data.m_cardinality);
}

} // namespace pgduckdb
