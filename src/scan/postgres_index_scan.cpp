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

#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/scan/index_scan_utils.hpp"
#include "pgduckdb/scan/postgres_index_scan.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_process_lock.hpp"
#include "pgduckdb/vendor/pg_list.hpp"

namespace pgduckdb {

//
// PostgresIndexScanGlobalState
//

PostgresIndexScanGlobalState::PostgresIndexScanGlobalState(IndexOptInfo *index, IndexScanState *index_scan_state,
                                                           bool indexonly, Relation relation,
                                                           duckdb::TableFunctionInitInput &input)
    : m_global_state(duckdb::make_shared_ptr<PostgresScanGlobalState>()), m_index(index),
      m_index_scan(index_scan_state), m_indexonly(indexonly), m_relation(relation) {
	m_global_state->InitGlobalState(input);

	if (m_indexonly) {
		m_global_state->m_tuple_desc = ExecTypeFromTL(index->indextlist);
	} else {
		m_global_state->m_tuple_desc = RelationGetDescr(m_relation);
	}
}

PostgresIndexScanGlobalState::~PostgresIndexScanGlobalState() {
	index_close(m_index_scan->iss_RelationDesc, NoLock);
	RelationClose(m_relation);
}

//
// PostgresIndexScanLocalState
//

PostgresIndexScanLocalState::PostgresIndexScanLocalState(IndexScanDesc index_scan_desc, TupleDesc desc,
                                                         Relation relation)
    : m_local_state(duckdb::make_shared_ptr<PostgresScanLocalState>()), m_index_scan_desc(index_scan_desc),
      m_relation(relation) {
	m_slot = MakeTupleTableSlot(CreateTupleDescCopy(desc), &TTSOpsBufferHeapTuple);
}

PostgresIndexScanLocalState::~PostgresIndexScanLocalState() {
	index_endscan(m_index_scan_desc);
}

//
// PostgresIndexScanFunctionData
//

PostgresIndexScanFunctionData::PostgresIndexScanFunctionData(uint64_t cardinality, bool indexonly, Path *path,
                                                             PlannerInfo *planner_info, Oid relation_oid,
                                                             Snapshot snapshot)
    : m_cardinality(cardinality), m_indexonly(indexonly), m_path(path), m_planner_info(planner_info),
      m_snapshot(snapshot), m_relation_oid(relation_oid) {
}

PostgresIndexScanFunctionData::~PostgresIndexScanFunctionData() {
}

//
// PostgresSeqScanFunction
//

PostgresIndexScanFunction::PostgresIndexScanFunction()
    : TableFunction("postgres_index_scan", {}, PostgresIndexScanFunc, nullptr, PostgresIndexScanInitGlobal,
                    PostgresIndexScanInitLocal) {
	named_parameters["cardinality"] = duckdb::LogicalType::UBIGINT;
	named_parameters["indexonly"] = duckdb::LogicalType::BOOLEAN;
	named_parameters["path"] = duckdb::LogicalType::POINTER;
	named_parameters["planner_info"] = duckdb::LogicalType::POINTER;
	named_parameters["snapshot"] = duckdb::LogicalType::POINTER;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
	cardinality = PostgresIndexScanCardinality;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PostgresIndexScanFunction::PostgresIndexScanInitGlobal(duckdb::ClientContext &context,
                                                       duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresIndexScanFunctionData>();

	auto old_stack_base = set_stack_base();

	IndexScanState *index_state = makeNode(IndexScanState);
	IndexPath *index_path = (IndexPath *)bind_data.m_path;
	bool indexonly = bind_data.m_indexonly;

	elog(WARNING, "--%d", index_path->indexinfo->indexoid);

	index_state->iss_RelationDesc =
	    PostgresFunctionGuard<Relation>(index_open, index_path->indexinfo->indexoid, AccessShareLock);
	index_state->iss_RuntimeKeysReady = false;
	index_state->iss_RuntimeKeys = NULL;
	index_state->iss_NumRuntimeKeys = 0;

	List *stripped_clause_list = NIL;
	IndexOptInfo *index = index_path->indexinfo;

	ListCell *lc;

	foreach(lc, index_path->indexclauses)
	{
		IndexClause *iclause = lfirst_node(IndexClause, lc);
		int			indexcol = iclause->indexcol;
		ListCell   *lc2;

		foreach(lc2, iclause->indexquals)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc2);
			Node	   *clause = (Node *) rinfo->clause;
			clause = FixIndexQualClause(bind_data.m_planner_info, index, indexcol, clause, iclause->indexcols);
			stripped_clause_list = lappend(stripped_clause_list, clause);
		}
	}

	PostgresFunctionGuard(ExecIndexBuildScanKeys, (PlanState *)index_state, index_state->iss_RelationDesc,
	                      stripped_clause_list, false, &index_state->iss_ScanKeys, &index_state->iss_NumScanKeys,
	                      &index_state->iss_RuntimeKeys, &index_state->iss_NumRuntimeKeys, nullptr, nullptr);

	restore_stack_base(old_stack_base);

	return duckdb::make_uniq<PostgresIndexScanGlobalState>(index, index_state, indexonly,
	                                                       RelationIdGetRelation(bind_data.m_relation_oid), input);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PostgresIndexScanFunction::PostgresIndexScanInitLocal(duckdb::ExecutionContext &context,
                                                      duckdb::TableFunctionInitInput &input,
                                                      duckdb::GlobalTableFunctionState *gstate) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresIndexScanFunctionData>();
	auto global_state = reinterpret_cast<PostgresIndexScanGlobalState *>(gstate);

	IndexScanDesc scandesc = PostgresFunctionGuard<IndexScanDesc>(
	    index_beginscan, global_state->m_relation, global_state->m_index_scan->iss_RelationDesc, bind_data.m_snapshot,
	    global_state->m_index_scan->iss_NumScanKeys, global_state->m_index_scan->iss_NumOrderByKeys);

	if (global_state->m_indexonly) {
		scandesc->xs_want_itup = true;
	}

	if (global_state->m_index_scan->iss_NumRuntimeKeys == 0 || global_state->m_index_scan->iss_RuntimeKeysReady) {
		PostgresFunctionGuard(index_rescan, scandesc, global_state->m_index_scan->iss_ScanKeys,
		                      global_state->m_index_scan->iss_NumScanKeys, global_state->m_index_scan->iss_OrderByKeys,
		                      global_state->m_index_scan->iss_NumOrderByKeys);
	}

	return duckdb::make_uniq<PostgresIndexScanLocalState>(scandesc, global_state->m_global_state->m_tuple_desc,
	                                                      global_state->m_relation);
}

void
PostgresIndexScanFunction::PostgresIndexScanFunc(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
                                                 duckdb::DataChunk &output) {
	auto &local_state = data.local_state->Cast<PostgresIndexScanLocalState>();
	auto &global_state = data.global_state->Cast<PostgresIndexScanGlobalState>();
	ItemPointer next_index_tid = nullptr;

	local_state.m_local_state->m_output_vector_size = 0;

	if (local_state.m_local_state->m_exhausted_scan) {
		output.SetCardinality(0);
		return;
	}

	while (local_state.m_local_state->m_output_vector_size < STANDARD_VECTOR_SIZE) {

		DuckdbProcessLock::GetLock().lock();
		next_index_tid = index_getnext_tid(local_state.m_index_scan_desc, ForwardScanDirection);
		DuckdbProcessLock::GetLock().unlock();

		/* No more index tuples to read */
		if (!next_index_tid) {
			break;
		}

		if (!global_state.m_indexonly) {
			DuckdbProcessLock::GetLock().lock();
			auto found = index_fetch_heap(local_state.m_index_scan_desc, local_state.m_slot);
			DuckdbProcessLock::GetLock().unlock();
			/* Tuple not found */
			if (!found) {
				continue;
			}
			InsertTupleIntoChunk(output, global_state.m_global_state, local_state.m_local_state,
			                     reinterpret_cast<BufferHeapTupleTableSlot *>(local_state.m_slot)->base.tuple);
		} else {
			if (local_state.m_index_scan_desc->xs_hitup) {
				InsertTupleIntoChunk(output, global_state.m_global_state, local_state.m_local_state,
				                     local_state.m_index_scan_desc->xs_hitup);
			} else if (local_state.m_index_scan_desc->xs_itup) {
				index_deform_tuple(local_state.m_index_scan_desc->xs_itup, local_state.m_index_scan_desc->xs_itupdesc,
				                   local_state.m_slot->tts_values, local_state.m_slot->tts_isnull);
				InsertTupleValuesIntoChunk(output, global_state.m_global_state, local_state.m_local_state,
				                           local_state.m_slot->tts_values, local_state.m_slot->tts_isnull);
			}
		}
		ExecClearTuple(local_state.m_slot);
	}

	if (!next_index_tid) {
		local_state.m_local_state->m_exhausted_scan = true;
	}

	output.SetCardinality(local_state.m_local_state->m_output_vector_size);
	output.Verify();
}

duckdb::unique_ptr<duckdb::NodeStatistics>
PostgresIndexScanFunction::PostgresIndexScanCardinality(duckdb::ClientContext &context,
                                                        const duckdb::FunctionData *data) {
	auto &bind_data = data->Cast<PostgresIndexScanFunctionData>();
	return duckdb::make_uniq<duckdb::NodeStatistics>(bind_data.m_cardinality, bind_data.m_cardinality);
}

} // namespace pgduckdb
