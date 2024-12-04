#include "pgduckdb/scan/postgres_table_reader.hpp"
#include "pgduckdb/pgduckdb_process_lock.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "tcop/tcopprot.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/execParallel.h"
#include "executor/tqueue.h"
#include "utils/wait_event.h"
#include "access/xact.h"
#include "pgduckdb/pgduckdb_guc.h"
}

#include <cmath>

namespace pgduckdb {

PostgresTableReader::PostgresTableReader(Cardinality cardinality, const char *table_scan_query)
    : parallel_worker_readers(nullptr), nreaders(0), next_parallel_reader(0) {

	std::lock_guard<std::mutex> lock(DuckdbProcessLock::GetLock());

	pg_stack_base_t current_stack = set_stack_base();
	List *raw_parsetree_list;
	raw_parsetree_list = pg_parse_query(table_scan_query);
	List *query_list =
	    pg_analyze_and_rewrite_fixedparams(linitial_node(RawStmt, raw_parsetree_list), table_scan_query, NULL, 0, NULL);
	PlannedStmt *planned_stmt = standard_planner(linitial_node(Query, query_list), table_scan_query, 0, NULL);
	table_scan_query_desc = CreateQueryDesc(planned_stmt, table_scan_query, GetActiveSnapshot(), InvalidSnapshot,
	                                        None_Receiver, NULL, NULL, 0);
	ExecutorStart(table_scan_query_desc, 0);

	table_scan_planstate = ExecInitNode(planned_stmt->planTree, table_scan_query_desc->estate, 0);

	table_scan_query_desc->planstate->plan->parallel_aware = true;

	int parallel_workers = ParalleWorkerNumber(cardinality);

	RESUME_CANCEL_INTERRUPTS();

	ParallelContext *pcxt;
	pei = ExecInitParallelPlan(table_scan_planstate, table_scan_query_desc->estate, NULL, parallel_workers, -1);
	pcxt = pei->pcxt;
	LaunchParallelWorkers(pcxt);
	/* We save # workers launched for the benefit of EXPLAIN */
	nworkers_launched = pcxt->nworkers_launched;

	/* Set up tuple queue readers to read the results. */
	if (pcxt->nworkers_launched > 0) {
		ExecParallelCreateReaders(pei);
		/* Make a working array showing the active readers */
		nreaders = pcxt->nworkers_launched;
		parallel_worker_readers = (void **)palloc(nreaders * sizeof(TupleQueueReader *));
		memcpy(parallel_worker_readers, pei->reader, nreaders * sizeof(TupleQueueReader *));
	}

	HOLD_CANCEL_INTERRUPTS();

	slot = ExecInitExtraTupleSlot(table_scan_query_desc->estate, table_scan_planstate->ps_ResultTupleDesc, &TTSOpsMinimalTuple);

	restore_stack_base(current_stack);
}

PostgresTableReader::~PostgresTableReader() {
	std::lock_guard<std::mutex> lock(DuckdbProcessLock::GetLock());

	ExecEndNode(table_scan_planstate);

	if (pei != NULL)
		ExecParallelFinish(pei);

	if (parallel_worker_readers) {
		pfree(parallel_worker_readers);
	}

	parallel_worker_readers = NULL;

	ExecParallelCleanup(pei);

	pei = NULL;

	ExecutorFinish(table_scan_query_desc);
	ExecutorEnd(table_scan_query_desc);
	FreeQueryDesc(table_scan_query_desc);
}

/*
 * This is simple calculation how much postgresql workers we will wanted to spawn for this
 * postgres table scan.
 */
int
PostgresTableReader::ParalleWorkerNumber(Cardinality cardinality) {
	static const int base_log = 8;
	int cardinality_log = std::log2(cardinality);
	int base = cardinality_log / base_log;
	return std::max(1, std::min(base, std::min(max_workers_per_postgres_scan, max_parallel_workers)));
}

TupleTableSlot *
PostgresTableReader::GetNextTuple() {
	MinimalTuple worker_minmal_tuple;
	if (nreaders > 0) {
		worker_minmal_tuple = GetNextWorkerTuple();
		if (HeapTupleIsValid(worker_minmal_tuple)) {
			ExecStoreMinimalTuple(worker_minmal_tuple, slot, false);
			return slot;
		}
	} else {
		std::lock_guard<std::mutex> lock(DuckdbProcessLock::GetLock());
		pg_stack_base_t current_stack = set_stack_base();
		table_scan_query_desc->estate->es_query_dsa = pei ? pei->area : NULL;
		slot = ExecProcNode(table_scan_planstate);
		restore_stack_base(current_stack);
		table_scan_query_desc->estate->es_query_dsa = NULL;
		if (!TupIsNull(slot)) {
			return slot;
		}
	}

	return ExecClearTuple(slot);
}

MinimalTuple
PostgresTableReader::GetNextWorkerTuple() {
	int nvisited = 0;
	for (;;) {
		TupleQueueReader *reader;
		MinimalTuple minimal_tuple;
		bool readerdone;

		reader = (TupleQueueReader *) parallel_worker_readers[next_parallel_reader];
		minimal_tuple = TupleQueueReaderNext(reader, true, &readerdone);

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
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, WAIT_EVENT_EXECUTE_GATHER);
			ResetLatch(MyLatch);
			nvisited = 0;
		}
	}
}

} // namespace pgduckdb
