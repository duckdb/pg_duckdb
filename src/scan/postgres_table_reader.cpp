#include "pgduckdb/scan/postgres_table_reader.hpp"
#include "pgduckdb/pgduckdb_process_latch.hpp"
#include "pgduckdb/pgduckdb_process_lock.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/execParallel.h"
#include "executor/tqueue.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "tcop/tcopprot.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"

#include "pgduckdb/pgduckdb_guc.h"
}

#include <cmath>

namespace pgduckdb {

PostgresTableReader::PostgresTableReader(const char *table_scan_query, bool count_tuples_only)
    : parallel_executor_info(nullptr), parallel_worker_readers(nullptr), nreaders(0), next_parallel_reader(0),
      entered_parallel_mode(false) {
	std::lock_guard<std::mutex> lock(GlobalProcessLock::GetLock());
	PostgresScopedStackReset scoped_stack_reset;

	List *raw_parsetree_list = PostgresFunctionGuard(pg_parse_query, table_scan_query);
	Assert(list_length(raw_parsetree_list) == 1);
	RawStmt *raw_parsetree = linitial_node(RawStmt, raw_parsetree_list);

	List *query_list =
	    PostgresFunctionGuard(pg_analyze_and_rewrite_fixedparams, raw_parsetree, table_scan_query, nullptr, 0, nullptr);

	Assert(list_length(query_list) == 1);
	Query *query = linitial_node(Query, query_list);

	Assert(list_length(query->rtable) == 1);
	RangeTblEntry *rte = linitial_node(RangeTblEntry, query->rtable);

	char persistence = get_rel_persistence(rte->relid);

	PlannedStmt *planned_stmt = PostgresFunctionGuard(standard_planner, query, table_scan_query, 0, nullptr);

	table_scan_query_desc = PostgresFunctionGuard(CreateQueryDesc, planned_stmt, table_scan_query, GetActiveSnapshot(),
	                                              InvalidSnapshot, None_Receiver, nullptr, nullptr, 0);

	PostgresFunctionGuard(ExecutorStart, table_scan_query_desc, 0);

	table_scan_planstate =
	    PostgresFunctionGuard(ExecInitNode, planned_stmt->planTree, table_scan_query_desc->estate, 0);

	bool marked_parallel_aware = false;

	if (persistence != RELPERSISTENCE_TEMP) {
		if (count_tuples_only) {
			/* For count_tuples_only we will try to execute aggregate node on table scan */
			planned_stmt->planTree->parallel_aware = true;
			marked_parallel_aware = MarkPlanParallelAware((Plan *)table_scan_query_desc->planstate->plan->lefttree);
		} else {
			marked_parallel_aware = MarkPlanParallelAware(table_scan_query_desc->planstate->plan);
		}
	}

	/* Temp tables can be excuted with parallel workers, and whole plan should be parallel aware */
	if (persistence != RELPERSISTENCE_TEMP && marked_parallel_aware) {

		int parallel_workers = ParallelWorkerNumber(planned_stmt->planTree->plan_rows);
		bool interrupts_can_be_process = INTERRUPTS_CAN_BE_PROCESSED();

		if (!interrupts_can_be_process) {
			RESUME_CANCEL_INTERRUPTS();
		}

		if (!IsInParallelMode()) {
			EnterParallelMode();
			entered_parallel_mode = true;
		}

		ParallelContext *pcxt;
		parallel_executor_info = PostgresFunctionGuard(ExecInitParallelPlan, table_scan_planstate,
		                                               table_scan_query_desc->estate, nullptr, parallel_workers, -1);
		pcxt = parallel_executor_info->pcxt;
		PostgresFunctionGuard(LaunchParallelWorkers, pcxt);
		nworkers_launched = pcxt->nworkers_launched;

		if (pcxt->nworkers_launched > 0) {
			PostgresFunctionGuard(ExecParallelCreateReaders, parallel_executor_info);
			nreaders = pcxt->nworkers_launched;
			parallel_worker_readers = (void **)palloc(nreaders * sizeof(TupleQueueReader *));
			memcpy(parallel_worker_readers, parallel_executor_info->reader, nreaders * sizeof(TupleQueueReader *));
		}

		if (!interrupts_can_be_process) {
			HOLD_CANCEL_INTERRUPTS();
		}
	}

	elog(DEBUG1, "(PGDuckdDB/PostgresTableReader)\n\nQUERY: %s\nRUNNING: %s.\nEXECUTING: \n%s", table_scan_query,
	     !nreaders ? "IN PROCESS THREAD" : psprintf("ON %d PARALLEL WORKER(S)", nreaders),
	     ExplainScanPlan(table_scan_query_desc).c_str());

	slot = PostgresFunctionGuard(ExecInitExtraTupleSlot, table_scan_query_desc->estate,
	                             table_scan_planstate->ps_ResultTupleDesc, &TTSOpsMinimalTuple);
}

PostgresTableReader::~PostgresTableReader() {
	std::lock_guard<std::mutex> lock(GlobalProcessLock::GetLock());

	PostgresScopedStackReset scoped_stack_reset;

	PostgresFunctionGuard(ExecEndNode, table_scan_planstate);

	if (parallel_executor_info != NULL) {
		PostgresFunctionGuard(ExecParallelFinish, parallel_executor_info);
		PostgresFunctionGuard(ExecParallelCleanup, parallel_executor_info);
	}

	parallel_executor_info = NULL;

	if (parallel_worker_readers) {
		PostgresFunctionGuard(pfree, parallel_worker_readers);
	}

	parallel_worker_readers = NULL;

	PostgresFunctionGuard(ExecutorFinish, table_scan_query_desc);
	PostgresFunctionGuard(ExecutorEnd, table_scan_query_desc);
	PostgresFunctionGuard(FreeQueryDesc, table_scan_query_desc);

	if (entered_parallel_mode) {
		ExitParallelMode();
	}
}

int
PostgresTableReader::ParallelWorkerNumber(Cardinality cardinality) {
	static const int base_log = 8;
	int cardinality_log = std::log2(cardinality);
	int base = cardinality_log / base_log;
	return std::max(1, std::min(base, std::max(max_workers_per_postgres_scan, max_parallel_workers)));
}

std::string
PostgresTableReader::ExplainScanPlan(QueryDesc *query_desc) {
	ExplainState *es = (ExplainState *)PostgresFunctionGuard(palloc0, sizeof(ExplainState));
	es->str = makeStringInfo();
	es->format = EXPLAIN_FORMAT_TEXT;
	PostgresFunctionGuard(ExplainPrintPlan, es, query_desc);
	std::string explain_scan(es->str->data);
	return explain_scan;
}

bool
PostgresTableReader::MarkPlanParallelAware(Plan *plan) {
	switch (nodeTag(plan)) {
	case T_SeqScan:
	case T_IndexScan:
	case T_IndexOnlyScan: {
		plan->parallel_aware = true;
		return true;
	}
	case T_BitmapHeapScan: {
		plan->parallel_aware = true;
		return MarkPlanParallelAware(plan->lefttree);
	}
	case T_BitmapIndexScan: {
		((BitmapIndexScan *)plan)->isshared = true;
		return true;
	}
	case T_BitmapAnd: {
		return MarkPlanParallelAware((Plan *)linitial(((BitmapAnd *)plan)->bitmapplans));
	}
	case T_BitmapOr: {
		((BitmapOr *)plan)->isshared = true;
		return MarkPlanParallelAware((Plan *)linitial(((BitmapOr *)plan)->bitmapplans));
	}
	default: {
		std::ostringstream oss;
		oss << "Unknown postgres scan query plan node: " << nodeTag(plan);
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, oss.str().c_str());
	}
	}
}

TupleTableSlot *
PostgresTableReader::GetNextTuple() {
	MinimalTuple worker_minmal_tuple;
	TupleTableSlot *thread_scan_slot;
	if (nreaders > 0) {
		worker_minmal_tuple = GetNextWorkerTuple();
		if (HeapTupleIsValid(worker_minmal_tuple)) {
			PostgresFunctionGuard(ExecStoreMinimalTuple, worker_minmal_tuple, slot, false);
			return slot;
		}
	} else {
		std::lock_guard<std::mutex> lock(GlobalProcessLock::GetLock());
		PostgresScopedStackReset scoped_stack_reset;
		table_scan_query_desc->estate->es_query_dsa = parallel_executor_info ? parallel_executor_info->area : NULL;
		thread_scan_slot = ExecProcNode(table_scan_planstate);
		table_scan_query_desc->estate->es_query_dsa = NULL;
		if (!TupIsNull(thread_scan_slot)) {
			return thread_scan_slot;
		}
	}

	return PostgresFunctionGuard(ExecClearTuple, slot);
}

MinimalTuple
PostgresTableReader::GetNextWorkerTuple() {
	int nvisited = 0;
	for (;;) {
		TupleQueueReader *reader;
		MinimalTuple minimal_tuple;
		bool readerdone;

		reader = (TupleQueueReader *)parallel_worker_readers[next_parallel_reader];
		minimal_tuple = PostgresFunctionGuard(TupleQueueReaderNext, reader, true, &readerdone);

		if (readerdone) {
			--nreaders;
			if (nreaders == 0) {
				return NULL;
			}
			memmove(&parallel_worker_readers[next_parallel_reader], &parallel_worker_readers[next_parallel_reader + 1],
			        sizeof(TupleQueueReader *) * (nreaders - next_parallel_reader));
			if (next_parallel_reader >= nreaders) {
				next_parallel_reader = 0;
			}
			continue;
		}

		if (minimal_tuple) {
			return minimal_tuple;
		}

		next_parallel_reader++;
		if (next_parallel_reader >= nreaders)
			next_parallel_reader = 0;

		nvisited++;
		if (nvisited >= nreaders) {
			GlobalProcessLatch::WaitGlobalLatch();
			nvisited = 0;
		}
	}
}

} // namespace pgduckdb
