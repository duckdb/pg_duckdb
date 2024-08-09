/*-------------------------------------------------------------------------
 *
 * duckdb_table_am.c
 *	  duckdb table access method code
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */
extern "C" {
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/tableam.h"
#include "access/heapam.h"
#include "access/amapi.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"

#define NOT_IMPLEMENTED() elog(ERROR, "duckdb does not implement %s", __func__)

PG_FUNCTION_INFO_V1(duckdb_am_handler);

/* ------------------------------------------------------------------------
 * Slot related callbacks for duckdb AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *
duckdb_slot_callbacks(Relation relation) {
	NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Table Scan Callbacks for duckdb AM
 * ------------------------------------------------------------------------
 */

static TableScanDesc
duckdb_scan_begin(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, ParallelTableScanDesc parallel_scan,
                  uint32 flags) {
	NOT_IMPLEMENTED();
}

static void
duckdb_scan_end(TableScanDesc sscan) {
	NOT_IMPLEMENTED();
}

static void
duckdb_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params, bool allow_strat, bool allow_sync,
                   bool allow_pagemode) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_scan_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot) {
	NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for duckdb AM
 * ------------------------------------------------------------------------
 */

static IndexFetchTableData *
duckdb_index_fetch_begin(Relation rel) {
	NOT_IMPLEMENTED();
}

static void
duckdb_index_fetch_reset(IndexFetchTableData *scan) {
	NOT_IMPLEMENTED();
}

static void
duckdb_index_fetch_end(IndexFetchTableData *scan) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
                         bool *call_again, bool *all_dead) {
	NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for
 * duckdb AM.
 * ------------------------------------------------------------------------
 */

static bool
duckdb_fetch_row_version(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot) {
	NOT_IMPLEMENTED();
}

static void
duckdb_get_latest_tid(TableScanDesc sscan, ItemPointer tid) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_tuple_tid_valid(TableScanDesc scan, ItemPointer tid) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot) {
	NOT_IMPLEMENTED();
}

static TransactionId
duckdb_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate) {
	NOT_IMPLEMENTED();
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for duckdb AM.
 * ----------------------------------------------------------------------------
 */

static void
duckdb_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid, int options, BulkInsertState bistate) {
	NOT_IMPLEMENTED();
}

static void
duckdb_tuple_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid, int options,
                                BulkInsertState bistate, uint32 specToken) {
	NOT_IMPLEMENTED();
}

static void
duckdb_tuple_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 spekToken, bool succeeded) {
	NOT_IMPLEMENTED();
}

static void
duckdb_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples, CommandId cid, int options,
                    BulkInsertState bistate) {
	NOT_IMPLEMENTED();
}

static TM_Result
duckdb_tuple_delete(Relation relation, ItemPointer tid, CommandId cid, Snapshot snapshot, Snapshot crosscheck,
                    bool wait, TM_FailureData *tmfd, bool changingPart) {
	NOT_IMPLEMENTED();
}

static TM_Result
duckdb_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot, CommandId cid, Snapshot snapshot,
                    Snapshot crosscheck, bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode,
                    TU_UpdateIndexes *update_indexes) {
	NOT_IMPLEMENTED();
}

static TM_Result
duckdb_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, CommandId cid,
                  LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd) {
	NOT_IMPLEMENTED();
}

static void
duckdb_finish_bulk_insert(Relation relation, int options) {
	/*
	 * This function needs to succeed for CREATE TABLE AS to work. It's a bit
	 * strange that postgres still calls this function even if no rows are
	 * inserted by the query from CREATE TABLE AS. Luckily we know
	 * duckdb_tuple_insert is not actually called because that fails with a
	 * "not implemented" error. So we know nothing was actually meant to be
	 * inserted and thus it's fine to let duckdb_finish_bulk_insert be a
	 * no-op.
	 */
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for duckdb AM.
 * ------------------------------------------------------------------------
 */

static void
duckdb_relation_set_new_filelocator(Relation rel, const RelFileLocator *newrnode, char persistence,
                                    TransactionId *freezeXid, MultiXactId *minmulti) {
	/* nothing to do */
}

static void
duckdb_relation_nontransactional_truncate(Relation rel) {
	NOT_IMPLEMENTED();
}

static void
duckdb_copy_data(Relation rel, const RelFileLocator *newrnode) {
	NOT_IMPLEMENTED();
}

static void
duckdb_copy_for_cluster(Relation OldTable, Relation NewTable, Relation OldIndex, bool use_sort,
                        TransactionId OldestXmin, TransactionId *xid_cutoff, MultiXactId *multi_cutoff,
                        double *num_tuples, double *tups_vacuumed, double *tups_recently_dead) {
	NOT_IMPLEMENTED();
}

static void
duckdb_vacuum(Relation onerel, VacuumParams *params, BufferAccessStrategy bstrategy) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno, BufferAccessStrategy bstrategy) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin, double *liverows, double *deadrows,
                               TupleTableSlot *slot) {
	NOT_IMPLEMENTED();
}

static double
duckdb_index_build_range_scan(Relation tableRelation, Relation indexRelation, IndexInfo *indexInfo, bool allow_sync,
                              bool anyvisible, bool progress, BlockNumber start_blockno, BlockNumber numblocks,
                              IndexBuildCallback callback, void *callback_state, TableScanDesc scan) {
	NOT_IMPLEMENTED();
}

static void
duckdb_index_validate_scan(Relation tableRelation, Relation indexRelation, IndexInfo *indexInfo, Snapshot snapshot,
                           ValidateIndexState *state) {
	NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the duckdb AM
 * ------------------------------------------------------------------------
 */

static uint64
duckdb_relation_size(Relation rel, ForkNumber forkNumber) {
	NOT_IMPLEMENTED();
}

/*
 * Check to see whether the table needs a TOAST table.
 */
static bool
duckdb_relation_needs_toast_table(Relation rel) {

	/* we don't need toast, because everything is stored in duckdb */
	return false;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the duckdb AM
 * ------------------------------------------------------------------------
 */

static void
duckdb_estimate_rel_size(Relation rel, int32 *attr_widths, BlockNumber *pages, double *tuples, double *allvisfrac) {
	/* no data available */
	if (attr_widths)
		*attr_widths = 0;
	if (pages)
		*pages = 0;
	if (tuples)
		*tuples = 0;
	if (allvisfrac)
		*allvisfrac = 0;
}

/* ------------------------------------------------------------------------
 * Executor related callbacks for the duckdb AM
 * ------------------------------------------------------------------------
 */

static bool
duckdb_scan_bitmap_next_block(TableScanDesc scan, TBMIterateResult *tbmres) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_scan_bitmap_next_tuple(TableScanDesc scan, TBMIterateResult *tbmres, TupleTableSlot *slot) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate, TupleTableSlot *slot) {
	NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Definition of the duckdb table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine duckdb_methods = {.type = T_TableAmRoutine,

                                              .slot_callbacks = duckdb_slot_callbacks,

                                              .scan_begin = duckdb_scan_begin,
                                              .scan_end = duckdb_scan_end,
                                              .scan_rescan = duckdb_scan_rescan,
                                              .scan_getnextslot = duckdb_scan_getnextslot,

                                              /* optional callbacks */
                                              .scan_set_tidrange = NULL,
                                              .scan_getnextslot_tidrange = NULL,

                                              /* these are common helper functions */
                                              .parallelscan_estimate = table_block_parallelscan_estimate,
                                              .parallelscan_initialize = table_block_parallelscan_initialize,
                                              .parallelscan_reinitialize = table_block_parallelscan_reinitialize,

                                              .index_fetch_begin = duckdb_index_fetch_begin,
                                              .index_fetch_reset = duckdb_index_fetch_reset,
                                              .index_fetch_end = duckdb_index_fetch_end,
                                              .index_fetch_tuple = duckdb_index_fetch_tuple,

                                              .tuple_fetch_row_version = duckdb_fetch_row_version,
                                              .tuple_tid_valid = duckdb_tuple_tid_valid,
                                              .tuple_get_latest_tid = duckdb_get_latest_tid,
                                              .tuple_satisfies_snapshot = duckdb_tuple_satisfies_snapshot,
                                              .index_delete_tuples = duckdb_index_delete_tuples,

                                              .tuple_insert = duckdb_tuple_insert,
                                              .tuple_insert_speculative = duckdb_tuple_insert_speculative,
                                              .tuple_complete_speculative = duckdb_tuple_complete_speculative,
                                              .multi_insert = duckdb_multi_insert,
                                              .tuple_delete = duckdb_tuple_delete,
                                              .tuple_update = duckdb_tuple_update,
                                              .tuple_lock = duckdb_tuple_lock,
                                              .finish_bulk_insert = duckdb_finish_bulk_insert,

                                              .relation_set_new_filelocator = duckdb_relation_set_new_filelocator,
                                              .relation_nontransactional_truncate =
                                                  duckdb_relation_nontransactional_truncate,
                                              .relation_copy_data = duckdb_copy_data,
                                              .relation_copy_for_cluster = duckdb_copy_for_cluster,
                                              .relation_vacuum = duckdb_vacuum,
                                              .scan_analyze_next_block = duckdb_scan_analyze_next_block,
                                              .scan_analyze_next_tuple = duckdb_scan_analyze_next_tuple,
                                              .index_build_range_scan = duckdb_index_build_range_scan,
                                              .index_validate_scan = duckdb_index_validate_scan,

                                              .relation_size = duckdb_relation_size,
                                              .relation_needs_toast_table = duckdb_relation_needs_toast_table,
                                              /* can be null because relation_needs_toast_table returns false */
                                              .relation_fetch_toast_slice = NULL,

                                              .relation_estimate_size = duckdb_estimate_rel_size,

                                              .scan_bitmap_next_block = duckdb_scan_bitmap_next_block,
                                              .scan_bitmap_next_tuple = duckdb_scan_bitmap_next_tuple,
                                              .scan_sample_next_block = duckdb_scan_sample_next_block,
                                              .scan_sample_next_tuple = duckdb_scan_sample_next_tuple};

Datum
duckdb_am_handler(PG_FUNCTION_ARGS) {
	PG_RETURN_POINTER(&duckdb_methods);
}
}

bool
is_duckdb_table_am(const TableAmRoutine *am) {
	return am == &duckdb_methods;
}
