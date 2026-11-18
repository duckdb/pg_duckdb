#include "pgduckdb/scan/postgres_table_reader.hpp"
#include "pgduckdb/pgduckdb_process_lock.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"
#include "pgduckdb/pgduckdb_hooks.hpp"
#include "pgduckdb/utility/signal_guard.hpp"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "commands/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif
#include "executor/executor.h"
#include "executor/execParallel.h"
#include "executor/tqueue.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"
#include "storage/latch.h"
#if PG_VERSION_NUM >= 190000
#include "storage/waiteventset.h"
#endif
}

#include "pgduckdb/vendor/pg_list.hpp"

#include <cmath>

namespace pgduckdb {

PostgresTableReader::PostgresTableReader()
    : table_scan_query_desc(nullptr), table_scan_planstate(nullptr), parallel_executor_info(nullptr),
      parallel_worker_readers(nullptr), slot(nullptr), nworkers_launched(0), nreaders(0), next_parallel_reader(0),
      entered_parallel_mode(false), cleaned_up(false) {
}

void
PostgresTableReader::Init(const char *table_scan_query, bool count_tuples_only) {
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	// Setting up a parallel scan creates DSM segments, which saves and restores the signal mask
	ThreadSignalUnblockGuard signal_mask_guard;
	PostgresScopedStackReset scoped_stack_reset;
	PostgresMemberGuard(PostgresTableReader::InitUnsafe, table_scan_query, count_tuples_only);
}

void
PostgresTableReader::InitUnsafe(const char *table_scan_query, bool count_tuples_only) {
	List *raw_parsetree_list = pg_parse_query(table_scan_query);
	Assert(list_length(raw_parsetree_list) == 1);
	RawStmt *raw_parsetree = linitial_node(RawStmt, raw_parsetree_list);

#if PG_VERSION_NUM >= 150000
	List *query_list = pg_analyze_and_rewrite_fixedparams(raw_parsetree, table_scan_query, nullptr, 0, nullptr);
#else
	List *query_list = pg_analyze_and_rewrite(raw_parsetree, table_scan_query, nullptr, 0, nullptr);
#endif

	Assert(list_length(query_list) == 1);
	Query *query = linitial_node(Query, query_list);

	Assert(list_length(query->rtable) == 1);
	RangeTblEntry *rte = linitial_node(RangeTblEntry, query->rtable);

	char persistence = get_rel_persistence(rte->relid);

	PlannedStmt *planned_stmt = PlanPostgresQuery(query, table_scan_query, 0, nullptr);

	table_scan_query_desc = CreateQueryDesc(planned_stmt, table_scan_query, GetActiveSnapshot(), InvalidSnapshot,
	                                        None_Receiver, nullptr, nullptr, 0);

	ExecutorStart(table_scan_query_desc, 0);

	table_scan_planstate = ExecInitNode(planned_stmt->planTree, table_scan_query_desc->estate, 0);

	bool run_scan_with_parallel_workers = persistence != RELPERSISTENCE_TEMP;
	run_scan_with_parallel_workers &= CanTableScanRunInParallel(table_scan_query_desc->planstate->plan);

	/* Temp tables cannot be excuted with parallel workers, and whole plan should be parallel aware */
	if (run_scan_with_parallel_workers) {
		InitRunWithParallelScan(planned_stmt, count_tuples_only);
	}

	if (duckdb_log_pg_explain) {
		ExplainState *es = (ExplainState *)palloc0(sizeof(ExplainState));
		es->str = makeStringInfo();
		es->format = EXPLAIN_FORMAT_TEXT;
		ExplainPrintPlan(es, table_scan_query_desc);
		elog(NOTICE, "(PGDuckDB/PostgresTableReader)\n\nQUERY: %s\nRUNNING: %s.\nEXECUTING: \n%s", table_scan_query,
		     !nreaders ? "IN PROCESS THREAD" : psprintf("ON %d PARALLEL WORKER(S)", nreaders), es->str->data);
	}

	slot = ExecInitExtraTupleSlot(table_scan_query_desc->estate, table_scan_planstate->ps_ResultTupleDesc,
	                              &TTSOpsMinimalTuple);
}

void
PostgresTableReader::InitRunWithParallelScan(PlannedStmt *planned_stmt, bool count_tuples_only) {
	int parallel_workers = 0;
	if (count_tuples_only) {
		/* For count_tuples_only we will try to execute aggregate node on table scan */
		planned_stmt->planTree->parallel_aware = true;
		MarkPlanParallelAware((Plan *)table_scan_query_desc->planstate->plan->lefttree);
		parallel_workers = ParallelWorkerNumber(planned_stmt->planTree->lefttree->plan_rows);
	} else {
		MarkPlanParallelAware(table_scan_query_desc->planstate->plan);
		parallel_workers = ParallelWorkerNumber(planned_stmt->planTree->plan_rows);
	}

	bool interrupts_can_be_process = INTERRUPTS_CAN_BE_PROCESSED();
	if (!interrupts_can_be_process) {
		RESUME_CANCEL_INTERRUPTS();
	}

	EnterParallelMode();
	entered_parallel_mode = true;

	ParallelContext *pcxt;
	parallel_executor_info =
	    ExecInitParallelPlan(table_scan_planstate, table_scan_query_desc->estate, nullptr, parallel_workers, -1);
	pcxt = parallel_executor_info->pcxt;
	LaunchParallelWorkers(pcxt);
	nworkers_launched = pcxt->nworkers_launched;

	if (pcxt->nworkers_launched > 0) {
		ExecParallelCreateReaders(parallel_executor_info);
		nreaders = pcxt->nworkers_launched;
		parallel_worker_readers = (void **)palloc(nreaders * sizeof(TupleQueueReader *));
		memcpy(parallel_worker_readers, parallel_executor_info->reader, nreaders * sizeof(TupleQueueReader *));
	}

	if (!interrupts_can_be_process) {
		HOLD_CANCEL_INTERRUPTS();
	}
}

/*
 * Initializes a tuple slot for the current query.
 *
 * The returned TupleTableSlot is allocated in the context of `table_scan_query_desc->estate`
 * and will be automatically cleaned up when `table_scan_query_desc` is destroyed.
 *
 * Returns NULL if the table reader has already been cleaned up, e.g., if concurrent threads
 * have finished the scan and triggered cleanup).
 */
TupleTableSlot *
PostgresTableReader::InitTupleSlot() {
	D_ASSERT(!cleaned_up);
	return PostgresMemberGuard(PostgresTableReader::InitTupleSlotUnsafe);
}

/*
 * The slots are deformed by DuckDB threads without holding the GlobalProcessLock. Deforming a tuple caches the
 * attribute offsets in the tuple descriptor of the slot, so every slot gets its own copy of the descriptor.
 */
TupleTableSlot *
PostgresTableReader::InitTupleSlotUnsafe() {
	EState *estate = table_scan_query_desc->estate;
	MemoryContext old_context = MemoryContextSwitchTo(estate->es_query_cxt);
	TupleDesc tuple_desc = CreateTupleDescCopy(table_scan_planstate->ps_ResultTupleDesc);
	MemoryContextSwitchTo(old_context);
	return ExecInitExtraTupleSlot(estate, tuple_desc, &TTSOpsMinimalTuple);
}

PostgresTableReader::~PostgresTableReader() {
	if (cleaned_up) {
		return;
	}
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	// An exception escaping a destructor terminates the process. Cleanup can raise a Postgres error, e.g. when the
	// scan is torn down after a cancelled query and a postgres_fdw connection is still busy with the aborted fetch.
	// Whatever is left behind is released by the transaction abort that follows the failed query. That query
	// reports its own error, and a failed query can fail the cleanup of each of its scans, so only log this.
	try {
		Cleanup();
	} catch (std::exception &ex) {
		pd_log(LOG, "(PGDuckDB/~PostgresTableReader) Failed to clean up Postgres scan: %s",
		       duckdb::ErrorData(ex).RawMessage().c_str());
	}
}

void
PostgresTableReader::Cleanup() {
	D_ASSERT(!cleaned_up);
	cleaned_up = true;
	ThreadSignalUnblockGuard signal_mask_guard;
	PostgresScopedStackReset scoped_stack_reset;
	try {
		PostgresMemberGuard(PostgresTableReader::CleanupUnsafe);
	} catch (...) {
		// Postgres would abort the transaction now, but the query can still succeed, e.g. when the error comes from a
		// parallel worker after DuckDB got all rows it needs for a LIMIT. So finish the cleanup like an aborted
		// transaction does. This also releases our parallel context, which Postgres otherwise keeps in its list of
		// active contexts: processing its messages would raise "lost connection to parallel worker" again and again
		// and make the cleanup of every other scan fail too.
		try {
			PostgresMemberGuard(PostgresTableReader::CleanupAfterErrorUnsafe);
		} catch (std::exception &ex) {
			pd_log(LOG, "(PGDuckDB/Cleanup) Failed to finish the cleanup of a Postgres scan: %s",
			       duckdb::ErrorData(ex).RawMessage().c_str());
		}
		throw;
	}
}

/*
 * Every cleanup step clears its member before it starts, so that a step that
 * failed is not retried after the error. Whatever it left behind is released
 * by the transaction abort.
 */
void
PostgresTableReader::CleanupUnsafe() {
	if (table_scan_planstate) {
		PlanState *planstate = table_scan_planstate;
		table_scan_planstate = nullptr;
		ExecEndNode(planstate);
	}

	if (parallel_executor_info != NULL) {
		// ExecParallelFinish waits until our workers report that they are done, and only reads their messages when
		// ParallelMessagePending is set. That flag can be consumed without our messages being read: a Postgres error
		// raised while processing the messages of another scan's workers, or a cancel error raised before them,
		// stops processing early. Postgres would then abort the transaction, but we catch such errors and continue
		// with the other scans. Without the flag the wait would never end, so make sure our messages are read.
		ParallelMessagePending = true;
		InterruptPending = true;
		ExecParallelFinish(parallel_executor_info);
	}

	FinishCleanupUnsafe();
}

/*
 * The part of the cleanup that does not wait for the parallel workers to finish.
 */
void
PostgresTableReader::FinishCleanupUnsafe() {
	if (parallel_executor_info != NULL) {
		ParallelExecutorInfo *pei = parallel_executor_info;
		parallel_executor_info = nullptr;
		// If this fails, our parallel context can still be active, so we have to stay in parallel mode
		bool exit_parallel_mode = entered_parallel_mode;
		entered_parallel_mode = false;
		// Terminates the workers if they are still running
		ExecParallelCleanup(pei);
		entered_parallel_mode = exit_parallel_mode;
	}

	if (parallel_worker_readers) {
		pfree(parallel_worker_readers);
		parallel_worker_readers = nullptr;
	}

	if (table_scan_query_desc) {
		QueryDesc *query_desc = table_scan_query_desc;
		table_scan_query_desc = nullptr;
		ExecutorFinish(query_desc);
		ExecutorEnd(query_desc);
		FreeQueryDesc(query_desc);
	}

	if (entered_parallel_mode) {
		entered_parallel_mode = false;
		ExitParallelMode();
	}
}

void
PostgresTableReader::CleanupAfterErrorUnsafe() {
	// Do not process interrupts: pending parallel messages would raise the same errors again
	HOLD_INTERRUPTS();
	FinishCleanupUnsafe();
	RESUME_INTERRUPTS();
}

/*
 * Logic is straightforward, if `duckdb_max_workers_per_postgres_scan` is set to 0 we don't want any
 * parallelization. For cardinality less equal than 2^16 we only try to run one parallel process. When cardinality
 * is bigger than we should spawn numer of parallel processes set by `duckdb_max_workers_per_postgres_scan` but
 * not bigger than `max_parallel_workers`.
 */

int
PostgresTableReader::ParallelWorkerNumber(Cardinality cardinality) {
	static const int cardinality_threshold = 1 << 16;
	/* No parallel worker scan wanted */
	if (!duckdb_max_workers_per_postgres_scan) {
		return 0;
	}
	/* Use only one worker when scan is done on low cardinality */
	if (cardinality <= cardinality_threshold) {
		return 1;
	}
	return std::min(duckdb_max_workers_per_postgres_scan, max_parallel_workers);
}

bool
PostgresTableReader::CanTableScanRunInParallel(Plan *plan) {
	switch (nodeTag(plan)) {
	case T_SeqScan:
	case T_IndexScan:
	case T_IndexOnlyScan:
	case T_BitmapHeapScan:
		return true;
	case T_Append: {
		ListCell *l;
		foreach (l, ((Append *)plan)->appendplans) {
			if (!CanTableScanRunInParallel((Plan *)lfirst(l))) {
				return false;
			}
		}
		return true;
	}
	/*
	 * This is special case for COUNT(*). Other extensions can plan the scan
	 * below it differently, e.g. Citus uses its own node for distributed tables.
	 */
	case T_Agg:
		return CanTableScanRunInParallel(outerPlan(plan));
	default:
		return false;
	}
}

bool
PostgresTableReader::MarkPlanParallelAware(Plan *plan) {
	switch (nodeTag(plan)) {
	case T_SeqScan:
	case T_IndexScan:
	case T_IndexOnlyScan:
	case T_Append: {
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

/*
 * GlobalProcessLock should be held before calling this.
 */
TupleTableSlot *
PostgresTableReader::GetNextTuple() {
	// Must be outside of the guarded function: a Postgres error longjmps out of it without running destructors, which
	// would leave the stack base pointing to the stack of this (possibly DuckDB) thread.
	PostgresScopedStackReset scoped_stack_reset;
	return PostgresMemberGuard(PostgresTableReader::GetNextTupleUnsafe);
}

TupleTableSlot *
PostgresTableReader::GetNextTupleUnsafe() {
	if (nreaders > 0) {
		MinimalTuple worker_minmal_tuple = GetNextWorkerTuple();
		if (HeapTupleIsValid(worker_minmal_tuple)) {
			ExecStoreMinimalTuple(worker_minmal_tuple, slot, false);
			return slot;
		}
	}

	table_scan_query_desc->estate->es_query_dsa = parallel_executor_info ? parallel_executor_info->area : NULL;
	TupleTableSlot *thread_scan_slot = ExecProcNode(table_scan_planstate);
	table_scan_query_desc->estate->es_query_dsa = NULL;
	return TupIsNull(thread_scan_slot) ? ExecClearTuple(slot) : thread_scan_slot;
}

/*
 * Reads the next minimal tuple from a Postgres parallel worker and copies it into the provided buffer.
 * This function should only be called when the table scan is running with parallel workers.
 *
 * @param minimal_tuple_buffer Buffer to store the copied minimal tuple.
 * @return true if a tuple was read and copied; false if the scan is complete and no more tuples are available.
 *
 * Note: The caller must hold the GlobalProcessLock before invoking this function.
 */
bool
PostgresTableReader::GetNextMinimalWorkerTuple(std::vector<uint8_t> &minimal_tuple_buffer) {
	MinimalTuple worker_minmal_tuple = GetNextWorkerTuple();
	if (HeapTupleIsValid(worker_minmal_tuple)) {
		// deep copy worker_minmal_tuple to destination buffer, a queue message is exactly t_len bytes long
		Size tuple_size = worker_minmal_tuple->t_len;
		minimal_tuple_buffer.resize(tuple_size);
		memcpy(minimal_tuple_buffer.data(), worker_minmal_tuple, tuple_size);
		return true;
	}

	minimal_tuple_buffer.resize(0);
	return false;
}

MinimalTuple
PostgresTableReader::GetNextWorkerTuple() {
	int nvisited = 0;
	TupleQueueReader *reader = NULL;
	MinimalTuple minimal_tuple = NULL;
	bool readerdone = false;
	// The loop's stop condition acts as a safeguard in multithreaded scans, ensuring that if one thread calls this
	// function after another thread has already completed the scan, we do not access invalid readers.
	for (; next_parallel_reader < nreaders;) {
		reader = (TupleQueueReader *)parallel_worker_readers[next_parallel_reader];

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
		if (next_parallel_reader >= nreaders) {
			next_parallel_reader = 0;
		}

		nvisited++;
		if (nvisited >= nreaders) {
			/*
			 * It should be safe to make this call because function calling GetNextTuple() and transitively
			 * GetNextWorkerTuple() should held GlobalProcesLock.
			 */
			WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, PG_WAIT_EXTENSION);
			/* No need to use PostgresFunctionGuard here, because ResetLatch is a trivial function */
			ResetLatch(MyLatch);
			nvisited = 0;
		}
	}

	return NULL;
}

} // namespace pgduckdb
