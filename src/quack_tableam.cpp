#include "duckdb.hpp"
#include "quack/quack.hpp"

extern "C" {

#include "postgres.h"

#include "access/detoast.h"
#include "access/relation.h"
#include "access/tableam.h"
#include "access/multixact.h"
#include "access/table.h"
#include "catalog/index.h"
#include "catalog/objectaccess.h"
#include "commands/vacuum.h"
#include "pgstat.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/relmapper.h"

static const TupleTableSlotOps *
quack_slot_callbacks(Relation rel) {
	return &TTSOpsVirtual;
}

static TableScanDesc
quack_begin_scan(Relation rel, Snapshot snapshot, int nkeys, struct ScanKeyData *key,
                 ParallelTableScanDesc parallel_scan, uint32 flags) {
	TableScanDesc scan = (TableScanDesc)palloc0(sizeof(TableScanDesc));

	scan->rs_rd = rel;
	scan->rs_snapshot = snapshot;
	scan->rs_nkeys = 0;
	scan->rs_key = key;
	scan->rs_flags = flags;
	scan->rs_parallel = parallel_scan;

	return scan;
}

static void
quack_end_scan(TableScanDesc scan) {
}

static void
quack_rescan(TableScanDesc scan, struct ScanKeyData *key, bool set_params, bool allow_strat, bool allow_sync,
             bool allow_pagemode) {
	ereport(ERROR, (errmsg("quack_scan_rescan is not implemented")));
}

static bool
quack_getnextslot(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot) {
	return false;
}

static Size
quack_parallelscan_estimate(Relation rel) {
	elog(ERROR, "quack_parallelscan_estimate not implemented");
}

static Size
quack_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan) {
	elog(ERROR, "quack_parallelscan_initialize not implemented");
}

static void
quack_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan) {
	elog(ERROR, "quack_parallelscan_reinitialize not implemented");
}

static IndexFetchTableData *
quack_index_fetch_begin(Relation rel) {
	elog(ERROR, "quack_index_fetch_begin not implemented");
}

static void
quack_index_fetch_reset(IndexFetchTableData *sscan) {
	elog(ERROR, "quack_index_fetch_reset not implemented");
}

static void
quack_index_fetch_end(IndexFetchTableData *sscan) {
	elog(ERROR, "quack_index_fetch_end not implemented");
}

static bool
quack_index_fetch_tuple(struct IndexFetchTableData *sscan, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
                        bool *call_again, bool *all_dead) {
	elog(ERROR, "quack_index_fetch_tuple not implemented");
}

static bool
quack_fetch_row_version(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot) {
	elog(ERROR, "quack_fetch_row_version not implemented");
}

static void
quack_get_latest_tid(TableScanDesc sscan, ItemPointer tid) {
	elog(ERROR, "quack_get_latest_tid not implemented");
}

static bool
quack_tuple_tid_valid(TableScanDesc scan, ItemPointer tid) {
	elog(ERROR, "quack_tuple_tid_valid not implemented");
}

static bool
quack_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot) {
	elog(ERROR, "quack_tuple_satisfies_snapshot not implemented");
}

static TransactionId
quack_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate) {
	elog(ERROR, "quack_index_delete_tuples not implemented");
}

static void
quack_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid, int options,
                   struct BulkInsertStateData *bistate) {
	duckdb::QuackWriteState *write_state = quack_init_write_state(rel, MyDatabaseId, GetCurrentSubTransactionId());

	slot_getallattrs(slot);

	for (int i = 0; i < slot->tts_nvalid; i++) {
		if (slot->tts_isnull[i]) {
			write_state->appender->Append(nullptr);
		} else {
			duckdb::quack_append_value(*write_state->appender, slot->tts_tupleDescriptor->attrs[i].atttypid,
			                           slot->tts_values[i]);
		}
	}

	write_state->appender->EndRow();

	pgstat_count_heap_insert(rel, 1);
}

static void
quack_tuple_insert_speculative(Relation rel, TupleTableSlot *slot, CommandId cid, int options,
                               struct BulkInsertStateData *bistate, uint32 specToken) {
	ereport(ERROR, (errmsg("quack_tuple_insert_speculative is not implemented")));
}

static void
quack_tuple_complete_speculative(Relation rel, TupleTableSlot *slot, uint32 specToken, bool succeeded) {
	ereport(ERROR, (errmsg("quack_tuple_complete_speculative is not implemented")));
}

static void
quack_multi_insert(Relation rel, TupleTableSlot **slots, int ntuples, CommandId cid, int options,
                   struct BulkInsertStateData *bistate) {
	duckdb::QuackWriteState *write_state = quack_init_write_state(rel, MyDatabaseId, GetCurrentSubTransactionId());

	for (int n = 0; n < ntuples; n++) {
		TupleTableSlot *slot = slots[n];

		slot_getallattrs(slot);

		for (int i = 0; i < slot->tts_nvalid; i++) {
			if (slot->tts_isnull[i]) {
				write_state->appender->Append(nullptr);
			} else {
				duckdb::quack_append_value(*write_state->appender, slot->tts_tupleDescriptor->attrs[i].atttypid,
				                           slot->tts_values[i]);
			}
		}

		write_state->appender->EndRow();
	}

	pgstat_count_heap_insert(rel, ntuples);
}

static TM_Result
quack_tuple_delete(Relation rel, ItemPointer tip, CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait,
                   TM_FailureData *tmfd, bool changingPart) {
	ereport(ERROR, (errmsg("quack_tuple_delete is not implemented")));
}

static TM_Result
quack_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot, CommandId cid, Snapshot snapshot,
                   Snapshot crosscheck, bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode,
                   TU_UpdateIndexes *update_indexes) {
	ereport(ERROR, (errmsg("quack_tuple_update is not implemented")));
}

static TM_Result
quack_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, CommandId cid,
                 LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd) {
	ereport(ERROR, (errmsg("quack_tuple_lock is not implemented")));
}

static void
quack_finish_bulk_insert(Relation rel, int options) {
}

static void
quack_relation_set_new_filenode(Relation rel, const RelFileNumber newrnode, char persistence, TransactionId *freezeXid,
                                MultiXactId *minmulti) {

	if (persistence == RELPERSISTENCE_UNLOGGED) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Unlogged columnar tables are not supported")));
	}

	/*
	 * If existing and new relfilenode are different, that means the existing
	 * storage was dropped and we also need to clean up the metadata and
	 * state. If they are equal, this is a new relation object and we don't
	 * need to clean anything.
	 */
	if (RelationMapOidToFilenumber(rel->rd_id, false) != newrnode) {
		// acropolis_drop_table(rel);
	}
}

static void
quack_relation_nontransactional_truncate(Relation rel) {
	elog(ERROR, "quack_relation_nontransactional_truncate not implemented");
}

static void
quack_relation_copy_data(Relation rel, const RelFileLocator *newrlocator) {
	elog(ERROR, "quack_relation_copy_data not implemented");
}

static void
quack_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap, Relation OldIndex, bool use_sort,
                                TransactionId OldestXmin, TransactionId *xid_cutoff, MultiXactId *multi_cutoff,
                                double *num_tuples, double *tups_vacuumed, double *tups_recently_dead) {
	elog(ERROR, "quack_relation_copy_for_cluster not implemented");
}

static void
quack_vacuum_rel(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy) {
}

static bool
quack_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno, BufferAccessStrategy bstrategy) {
	elog(ERROR, "quack_scan_analyze_next_block not implemented");
}

static bool
quack_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin, double *liverows, double *deadrows,
                              TupleTableSlot *slot) {
	elog(ERROR, "quack_scan_analyze_next_tuple not implemented");
}

static double
quack_index_build_range_scan(Relation columnarRelation, Relation indexRelation, IndexInfo *indexInfo, bool allow_sync,
                             bool anyvisible, bool progress, BlockNumber start_blockno, BlockNumber numblocks,
                             IndexBuildCallback callback, void *callback_state, TableScanDesc scan) {
	elog(ERROR, "quack_index_build_range_scan not implemented");
}

static void
quack_index_validate_scan(Relation columnarRelation, Relation indexRelation, IndexInfo *indexInfo, Snapshot snapshot,
                          ValidateIndexState *validateIndexState) {
	elog(ERROR, "quack_index_validate_scan not implemented");
}

static bool
quack_relation_needs_toast_table(Relation rel) {
	return false;
}

static uint64
quack_relation_size(Relation rel, ForkNumber forkNumber) {
	return 4096 * BLCKSZ;
}

static void
quack_estimate_rel_size(Relation rel, int32 *attr_widths, BlockNumber *pages, double *tuples, double *allvisfrac) {
}

static bool
quack_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate) {
	elog(ERROR, "quack_scan_sample_next_block not implemented");
}

static bool
quack_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate, TupleTableSlot *slot) {
	elog(ERROR, "quack_scan_sample_next_tuple not implemented");
}

static const TableAmRoutine quack_am_methods = {.type = T_TableAmRoutine,

                                                .slot_callbacks = quack_slot_callbacks,

                                                .scan_begin = quack_begin_scan,
                                                .scan_end = quack_end_scan,
                                                .scan_rescan = quack_rescan,
                                                .scan_getnextslot = quack_getnextslot,

                                                .parallelscan_estimate = quack_parallelscan_estimate,
                                                .parallelscan_initialize = quack_parallelscan_initialize,
                                                .parallelscan_reinitialize = quack_parallelscan_reinitialize,

                                                .index_fetch_begin = quack_index_fetch_begin,
                                                .index_fetch_reset = quack_index_fetch_reset,
                                                .index_fetch_end = quack_index_fetch_end,
                                                .index_fetch_tuple = quack_index_fetch_tuple,

                                                .tuple_fetch_row_version = quack_fetch_row_version,
                                                .tuple_tid_valid = quack_tuple_tid_valid,
                                                .tuple_get_latest_tid = quack_get_latest_tid,

                                                .tuple_satisfies_snapshot = quack_tuple_satisfies_snapshot,
                                                .index_delete_tuples = quack_index_delete_tuples,

                                                .tuple_insert = quack_tuple_insert,
                                                .tuple_insert_speculative = quack_tuple_insert_speculative,
                                                .tuple_complete_speculative = quack_tuple_complete_speculative,
                                                .multi_insert = quack_multi_insert,
                                                .tuple_delete = quack_tuple_delete,
                                                .tuple_update = quack_tuple_update,
                                                .tuple_lock = quack_tuple_lock,
                                                .finish_bulk_insert = quack_finish_bulk_insert,

                                                .relation_nontransactional_truncate =
                                                    quack_relation_nontransactional_truncate,
                                                .relation_copy_data = quack_relation_copy_data,
                                                .relation_copy_for_cluster = quack_relation_copy_for_cluster,
                                                .relation_vacuum = quack_vacuum_rel,
                                                .scan_analyze_next_block = quack_scan_analyze_next_block,
                                                .scan_analyze_next_tuple = quack_scan_analyze_next_tuple,
                                                .index_build_range_scan = quack_index_build_range_scan,
                                                .index_validate_scan = quack_index_validate_scan,

                                                .relation_size = quack_relation_size,
                                                .relation_needs_toast_table = quack_relation_needs_toast_table,

                                                .relation_estimate_size = quack_estimate_rel_size,

                                                .scan_bitmap_next_block = NULL,
                                                .scan_bitmap_next_tuple = NULL,
                                                .scan_sample_next_block = quack_scan_sample_next_block,
                                                .scan_sample_next_tuple = quack_scan_sample_next_tuple};

/*
 * Implementation of the object_access_hook.
 */
static object_access_hook_type PrevObjectAccessHook = NULL;

static void
TableAMObjectAccessHook(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg) {
	if (PrevObjectAccessHook) {
		PrevObjectAccessHook(access, classId, objectId, subId, arg);
	}

	/* dispatch to the proper action */
	if (access == OAT_DROP && classId == RelationRelationId && !OidIsValid(subId)) {
		Relation rel;
		rel = relation_open(objectId, AccessExclusiveLock);

		if (rel->rd_rel->relkind == RELKIND_RELATION && rel->rd_tableam == &quack_am_methods) {
		}
		relation_close(rel, NoLock);
	}
}

static void
QuackXactCallback(XactEvent event, void *arg) {
	switch (event) {
	case XACT_EVENT_COMMIT:
	case XACT_EVENT_PARALLEL_COMMIT:
	case XACT_EVENT_PREPARE: {
		/* nothing to do */
		break;
	}

	case XACT_EVENT_ABORT:
	case XACT_EVENT_PARALLEL_ABORT: {
		break;
	}

	case XACT_EVENT_PRE_COMMIT:
	case XACT_EVENT_PARALLEL_PRE_COMMIT:
	case XACT_EVENT_PRE_PREPARE: {
		quack_flush_write_state(GetCurrentSubTransactionId(), 0, true);
		break;
	}
	}
}

void
quack_init_tableam() {
	RegisterXactCallback(QuackXactCallback, NULL);
	object_access_hook = TableAMObjectAccessHook;
}

const TableAmRoutine *
quack_get_table_am_routine(void) {
	return &quack_am_methods;
}

PG_FUNCTION_INFO_V1(quack_am_handler);
Datum
quack_am_handler(PG_FUNCTION_ARGS) {
	PG_RETURN_POINTER(&quack_am_methods);
}
}
