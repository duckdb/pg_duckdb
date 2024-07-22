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

#include "quack/scan/index_scan_utils.hpp"
#include "quack/scan/postgres_index_scan.hpp"
#include "quack/quack_types.hpp"

namespace quack {

//
// PostgresIndexScanGlobalState
//

PostgresIndexScanGlobalState::PostgresIndexScanGlobalState(IndexOptInfo *index, IndexScanState *indexScanState,
                                                           bool indexonly, Relation relation,
                                                           duckdb::TableFunctionInitInput &input)
    : m_global_state(duckdb::make_shared_ptr<PostgresScanGlobalState>()), m_index(index), m_index_scan(indexScanState),
      m_indexonly(indexonly), m_relation(relation) {
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

PostgresIndexScanLocalState::PostgresIndexScanLocalState(IndexScanDesc indexScanDesc, TupleDesc desc, Relation relation)
    : m_local_state(duckdb::make_shared_ptr<PostgresScanLocalState>()), m_index_scan_desc(indexScanDesc),
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
                                                             PlannerInfo *plannerInfo, Oid relationOid,
                                                             Snapshot snapshot)
    : m_cardinality(cardinality), m_indexonly(indexonly), m_path(path), m_planner_info(plannerInfo),
      m_snapshot(snapshot), m_relation_oid(relationOid) {
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
	named_parameters["indexonly"] = duckdb::LogicalType::BOOLEAN;
	named_parameters["path"] = duckdb::LogicalType::POINTER;
	named_parameters["plannerInfo"] = duckdb::LogicalType::POINTER;
	named_parameters["snapshot"] = duckdb::LogicalType::POINTER;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
	cardinality = PostgresIndexScanCardinality;
}


static TupleDesc
ExecTypeFromTLWithNames(List *targetList, TupleDesc relationTupleDesc)
{
	TupleDesc	typeInfo;
	ListCell   *l;
	int			len;
	int			cur_resno = 1;

	len = ExecTargetListLength(targetList);
	typeInfo = CreateTemplateTupleDesc(len);

	foreach(l, targetList)
	{
		TargetEntry *tle = (TargetEntry*) lfirst(l);
		const Var * var = (Var *) tle->expr;

		TupleDescInitEntry(typeInfo,
						   cur_resno,
						   relationTupleDesc->attrs[var->varattno-1].attname.data,
						   exprType((Node *) tle->expr),
						   exprTypmod((Node *) tle->expr),
						   0);

		TupleDescInitEntryCollation(typeInfo,
									cur_resno,
									exprCollation((Node *) tle->expr));
		cur_resno++;
	}

	return typeInfo;
}

duckdb::unique_ptr<duckdb::FunctionData>
PostgresIndexScanFunction::PostgresIndexScanBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                 duckdb::vector<duckdb::LogicalType> &return_types,
                                                 duckdb::vector<duckdb::string> &names) {
	auto cardinality = input.named_parameters["cardinality"].GetValue<uint64_t>();
	auto indexonly = input.named_parameters["indexonly"].GetValue<bool>();
	auto path = (reinterpret_cast<Path *>(input.named_parameters["path"].GetPointer()));
	auto plannerInfo = (reinterpret_cast<PlannerInfo *>(input.named_parameters["plannerInfo"].GetPointer()));
	auto snapshot = (reinterpret_cast<Snapshot>(input.named_parameters["snapshot"].GetPointer()));

	RangeTblEntry *rte = planner_rt_fetch(path->parent->relid, plannerInfo);

	auto rel = RelationIdGetRelation(rte->relid);
	TupleDesc tupleDesc = nullptr;

	if (indexonly) {
		tupleDesc = ExecTypeFromTLWithNames(((IndexPath *)path)->indexinfo->indextlist,RelationGetDescr(rel));
	} else {
		tupleDesc = RelationGetDescr(rel);
	}

	if (!tupleDesc) {
		elog(ERROR, "Failed to get tuple descriptor for relation with OID %u", rel->rd_id);
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

	RelationClose(rel);

	return duckdb::make_uniq<PostgresIndexScanFunctionData>(cardinality, indexonly, path, plannerInfo, rte->relid,
	                                                        snapshot);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PostgresIndexScanFunction::PostgresIndexScanInitGlobal(duckdb::ClientContext &context,
                                                       duckdb::TableFunctionInitInput &input) {
	auto &bindData = input.bind_data->CastNoConst<PostgresIndexScanFunctionData>();

	IndexScanState *indexstate = makeNode(IndexScanState);
	IndexPath *indexPath = (IndexPath *)bindData.m_path;
	bool indexonly = bindData.m_indexonly;

	indexstate->iss_RelationDesc = index_open(indexPath->indexinfo->indexoid, AccessShareLock);
	indexstate->iss_RuntimeKeysReady = false;
	indexstate->iss_RuntimeKeys = NULL;
	indexstate->iss_NumRuntimeKeys = 0;

	List *stripped_list_clauses = NIL;
	IndexOptInfo *index = indexPath->indexinfo;

	ListCell *lc;
	foreach (lc, indexPath->indexclauses) {
		IndexClause *iclause = lfirst_node(IndexClause, lc);
		int indexcol = iclause->indexcol;
		ListCell *lc2;
		foreach (lc2, iclause->indexquals) {
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc2);
			Node *clause = (Node *)rinfo->clause;
			clause = fix_indexqual_clause(bindData.m_planner_info, index, indexcol, clause, iclause->indexcols);
			stripped_list_clauses = lappend(stripped_list_clauses, clause);
		}
	}

	ExecIndexBuildScanKeys((PlanState *)indexstate, indexstate->iss_RelationDesc, stripped_list_clauses, false,
	                       &indexstate->iss_ScanKeys, &indexstate->iss_NumScanKeys, &indexstate->iss_RuntimeKeys,
	                       &indexstate->iss_NumRuntimeKeys, NULL, /* no ArrayKeys */
	                       NULL);

	return duckdb::make_uniq<PostgresIndexScanGlobalState>(index, indexstate, indexonly,
	                                                       RelationIdGetRelation(bindData.m_relation_oid), input);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PostgresIndexScanFunction::PostgresIndexScanInitLocal(duckdb::ExecutionContext &context,
                                                      duckdb::TableFunctionInitInput &input,
                                                      duckdb::GlobalTableFunctionState *gstate) {

	auto &bindData = input.bind_data->CastNoConst<PostgresIndexScanFunctionData>();
	auto globalState = reinterpret_cast<PostgresIndexScanGlobalState *>(gstate);

	IndexScanDesc scandesc =
	    index_beginscan(globalState->m_relation, globalState->m_index_scan->iss_RelationDesc, bindData.m_snapshot,
	                    globalState->m_index_scan->iss_NumScanKeys, globalState->m_index_scan->iss_NumOrderByKeys);

	if (globalState->m_indexonly)
		scandesc->xs_want_itup = true;

	if (globalState->m_index_scan->iss_NumRuntimeKeys == 0 || globalState->m_index_scan->iss_RuntimeKeysReady)
		index_rescan(scandesc, globalState->m_index_scan->iss_ScanKeys, globalState->m_index_scan->iss_NumScanKeys,
		             globalState->m_index_scan->iss_OrderByKeys, globalState->m_index_scan->iss_NumOrderByKeys);

	return duckdb::make_uniq<PostgresIndexScanLocalState>(scandesc, globalState->m_global_state->m_tuple_desc, globalState->m_relation);
}

void
PostgresIndexScanFunction::PostgresIndexScanFunc(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
                                                 duckdb::DataChunk &output) {
	auto &localState = data.local_state->Cast<PostgresIndexScanLocalState>();
	auto &globalState = data.global_state->Cast<PostgresIndexScanGlobalState>();
	bool nextIndexTuple = false;

	localState.m_local_state->m_output_vector_size = 0;

	if (localState.m_local_state->m_exhausted_scan) {
		output.SetCardinality(0);
		return;
	}

	while (
	    localState.m_local_state->m_output_vector_size < STANDARD_VECTOR_SIZE &&
	    (nextIndexTuple = index_getnext_slot(localState.m_index_scan_desc, ForwardScanDirection, localState.m_slot))) {
		bool shouldFree;
		if (!globalState.m_indexonly) {
			auto tuple = ExecFetchSlotHeapTuple(localState.m_slot, false, &shouldFree);
			InsertTupleIntoChunk(output, globalState.m_global_state, localState.m_local_state, tuple);
		} else {
			if (localState.m_index_scan_desc->xs_hitup) {
				InsertTupleIntoChunk(output, globalState.m_global_state, localState.m_local_state,
				                     localState.m_index_scan_desc->xs_hitup);
			} else if (localState.m_index_scan_desc->xs_itup) {
				index_deform_tuple(localState.m_index_scan_desc->xs_itup, localState.m_index_scan_desc->xs_itupdesc,
				                   localState.m_slot->tts_values, localState.m_slot->tts_isnull);
				HeapTuple resultTuple = heap_form_tuple(localState.m_index_scan_desc->xs_itupdesc,
				                                        localState.m_slot->tts_values, localState.m_slot->tts_isnull);
				InsertTupleIntoChunk(output, globalState.m_global_state, localState.m_local_state,
				                     resultTuple);
			}
		}
		ExecClearTuple(localState.m_slot);
	}

	if (!nextIndexTuple) {
		localState.m_local_state->m_exhausted_scan = true;
	}

	output.SetCardinality(localState.m_local_state->m_output_vector_size);
}

duckdb::unique_ptr<duckdb::NodeStatistics>
PostgresIndexScanFunction::PostgresIndexScanCardinality(duckdb::ClientContext &context,
                                                        const duckdb::FunctionData *data) {
	auto &bindData = data->Cast<PostgresIndexScanFunctionData>();
	return duckdb::make_uniq<duckdb::NodeStatistics>(bindData.m_cardinality, bindData.m_cardinality);
}

} // namespace quack