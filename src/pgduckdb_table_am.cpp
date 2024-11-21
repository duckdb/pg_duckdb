/*-------------------------------------------------------------------------
 *
 * duckdb_table_am.c
 *	  duckdb table access method code
 *
 *	  All the functions use snake_case naming on purpose to match the Postgres
 *	  style for easy grepping.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */

extern "C" {
#include "postgres.h"

#include "access/tableam.h"
#include "access/heapam.h"
#include "access/amapi.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "utils/syscache.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

#include "pgduckdb/pgduckdb_ddl.hpp"

extern "C" {

#define NOT_IMPLEMENTED()                                                                                              \
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("duckdb does not implement %s", __func__)))

PG_FUNCTION_INFO_V1(duckdb_am_handler);

/* ------------------------------------------------------------------------
 * Slot related callbacks for duckdb AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *
duckdb_slot_callbacks(Relation) {
	/*
	 * Here we would most likely want to invent your own set of slot
	 * callbacks for our AM. For now we just use the minimal tuple slot, we
	 * only implement this function to make sure ANALYZE does not fail.
	 */
	return &TTSOpsMinimalTuple;
}

/* ------------------------------------------------------------------------
 * Table Scan Callbacks for duckdb AM
 * ------------------------------------------------------------------------
 */

typedef struct DuckdbScanDescData {
	TableScanDescData rs_base; /* AM independent part of the descriptor */

	/* Add more fields here as needed by the AM. */
} DuckdbScanDescData;
typedef struct DuckdbScanDescData *DuckdbScanDesc;

static TableScanDesc
duckdb_scan_begin(Relation relation, Snapshot snapshot, int nkeys, ScanKey, ParallelTableScanDesc parallel_scan,
                  uint32 flags) {
	DuckdbScanDesc scan = (DuckdbScanDesc)palloc(sizeof(DuckdbScanDescData));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	return (TableScanDesc)scan;
}

static void
duckdb_scan_end(TableScanDesc sscan) {
	DuckdbScanDesc scan = (DuckdbScanDesc)sscan;

	pfree(scan);
}

static void
duckdb_scan_rescan(TableScanDesc, ScanKey, bool, bool, bool, bool) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_scan_getnextslot(TableScanDesc, ScanDirection, TupleTableSlot *) {
	NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for duckdb AM
 * ------------------------------------------------------------------------
 */

static IndexFetchTableData *
duckdb_index_fetch_begin(Relation) {
	NOT_IMPLEMENTED();
}

static void
duckdb_index_fetch_reset(IndexFetchTableData *) {
	NOT_IMPLEMENTED();
}

static void
duckdb_index_fetch_end(IndexFetchTableData *) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_index_fetch_tuple(struct IndexFetchTableData *, ItemPointer, Snapshot, TupleTableSlot *, bool *, bool *) {
	NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for
 * duckdb AM.
 * ------------------------------------------------------------------------
 */

static bool
duckdb_fetch_row_version(Relation, ItemPointer, Snapshot, TupleTableSlot *) {
	NOT_IMPLEMENTED();
}

static void
duckdb_get_latest_tid(TableScanDesc, ItemPointer) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_tuple_tid_valid(TableScanDesc, ItemPointer) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_tuple_satisfies_snapshot(Relation, TupleTableSlot *, Snapshot) {
	NOT_IMPLEMENTED();
}

static TransactionId
duckdb_index_delete_tuples(Relation, TM_IndexDeleteOp *) {
	NOT_IMPLEMENTED();
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for duckdb AM.
 * ----------------------------------------------------------------------------
 */

static void
duckdb_tuple_insert(Relation, TupleTableSlot *, CommandId, int, BulkInsertState) {
	NOT_IMPLEMENTED();
}

static void
duckdb_tuple_insert_speculative(Relation, TupleTableSlot *, CommandId, int, BulkInsertState, uint32) {
	NOT_IMPLEMENTED();
}

static void
duckdb_tuple_complete_speculative(Relation, TupleTableSlot *, uint32, bool) {
	NOT_IMPLEMENTED();
}

static void
duckdb_multi_insert(Relation, TupleTableSlot **, int, CommandId, int, BulkInsertState) {
	NOT_IMPLEMENTED();
}

static TM_Result
duckdb_tuple_delete(Relation, ItemPointer, CommandId, Snapshot, Snapshot, bool, TM_FailureData *, bool) {
	NOT_IMPLEMENTED();
}

#if PG_VERSION_NUM >= 160000

static TM_Result
duckdb_tuple_update(Relation, ItemPointer, TupleTableSlot *, CommandId, Snapshot, Snapshot, bool, TM_FailureData *,
                    LockTupleMode *, TU_UpdateIndexes *) {
	NOT_IMPLEMENTED();
}

#else

static TM_Result
duckdb_tuple_update(Relation, ItemPointer, TupleTableSlot *, CommandId, Snapshot, Snapshot, bool, TM_FailureData *,
                    LockTupleMode *, bool *) {
	NOT_IMPLEMENTED();
}

#endif

static TM_Result
duckdb_tuple_lock(Relation, ItemPointer, Snapshot, TupleTableSlot *, CommandId, LockTupleMode, LockWaitPolicy, uint8,
                  TM_FailureData *) {
	NOT_IMPLEMENTED();
}

static void
duckdb_finish_bulk_insert(Relation, int) {
	NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for duckdb AM.
 * ------------------------------------------------------------------------
 */

#if PG_VERSION_NUM >= 160000

static void
duckdb_relation_set_new_filelocator(Relation rel, const RelFileLocator *, char, TransactionId *, MultiXactId *) {
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(rel->rd_id));
	if (!HeapTupleIsValid(tp)) {
		/* nothing to do, the table will be created in DuckDB later by the
		 * duckdb_create_table_trigger event trigger */
		return;
	}
	ReleaseSysCache(tp);
	DuckdbTruncateTable(rel->rd_id);
}

#else

static void
duckdb_relation_set_new_filenode(Relation rel, const RelFileNode *, char, TransactionId *, MultiXactId *) {
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(rel->rd_id));
	if (!HeapTupleIsValid(tp)) {
		/* nothing to do, the table will be created in DuckDB later by the
		 * duckdb_create_table_trigger event trigger */
		return;
	}
	ReleaseSysCache(tp);
	DuckdbTruncateTable(rel->rd_id);
}

#endif

static void
duckdb_relation_nontransactional_truncate(Relation rel) {
	DuckdbTruncateTable(rel->rd_id);
}

#if PG_VERSION_NUM >= 160000

static void
duckdb_copy_data(Relation, const RelFileLocator *) {
	NOT_IMPLEMENTED();
}

#else

static void
duckdb_copy_data(Relation, const RelFileNode *) {
	NOT_IMPLEMENTED();
}

#endif

static void
duckdb_copy_for_cluster(Relation, Relation, Relation, bool, TransactionId, TransactionId *, MultiXactId *, double *,
                        double *, double *) {
	NOT_IMPLEMENTED();
}

static void
duckdb_vacuum(Relation, VacuumParams *, BufferAccessStrategy) {
	NOT_IMPLEMENTED();
}

#if PG_VERSION_NUM >= 170000

static bool
duckdb_scan_analyze_next_block(TableScanDesc, ReadStream *) {
	/* no data in postgres, so no point to analyze next block */
	return false;
}

#else

static bool
duckdb_scan_analyze_next_block(TableScanDesc, BlockNumber, BufferAccessStrategy) {
	/* no data in postgres, so no point to analyze next block */
	return false;
}
#endif

static bool
duckdb_scan_analyze_next_tuple(TableScanDesc, TransactionId, double *, double *, TupleTableSlot *) {
	NOT_IMPLEMENTED();
}

static double
duckdb_index_build_range_scan(Relation, Relation, IndexInfo *, bool, bool, bool, BlockNumber, BlockNumber,
                              IndexBuildCallback, void *, TableScanDesc) {
	NOT_IMPLEMENTED();
}

static void
duckdb_index_validate_scan(Relation, Relation, IndexInfo *, Snapshot, ValidateIndexState *) {
	NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the duckdb AM
 * ------------------------------------------------------------------------
 */

static uint64
duckdb_relation_size(Relation, ForkNumber) {
	/*
	 * For now we just return 0. We should probably want return something more
	 * useful in the future though.
	 */
	return 0;
}

/*
 * Check to see whether the table needs a TOAST table.
 */
static bool
duckdb_relation_needs_toast_table(Relation) {

	/* we don't need toast, because everything is stored in duckdb */
	return false;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the duckdb AM
 * ------------------------------------------------------------------------
 */

static void
duckdb_estimate_rel_size(Relation, int32 *attr_widths, BlockNumber *pages, double *tuples, double *allvisfrac) {
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
duckdb_scan_bitmap_next_block(TableScanDesc, TBMIterateResult *) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_scan_bitmap_next_tuple(TableScanDesc, TBMIterateResult *, TupleTableSlot *) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_scan_sample_next_block(TableScanDesc, SampleScanState *) {
	NOT_IMPLEMENTED();
}

static bool
duckdb_scan_sample_next_tuple(TableScanDesc, SampleScanState *, TupleTableSlot *) {
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

#if PG_VERSION_NUM >= 160000
                                              .relation_set_new_filelocator = duckdb_relation_set_new_filelocator,
#else
                                              .relation_set_new_filenode = duckdb_relation_set_new_filenode,
#endif
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
                                              .relation_toast_am = NULL,
                                              .relation_fetch_toast_slice = NULL,

                                              .relation_estimate_size = duckdb_estimate_rel_size,

                                              .scan_bitmap_next_block = duckdb_scan_bitmap_next_block,
                                              .scan_bitmap_next_tuple = duckdb_scan_bitmap_next_tuple,
                                              .scan_sample_next_block = duckdb_scan_sample_next_block,
                                              .scan_sample_next_tuple = duckdb_scan_sample_next_tuple};

Datum
duckdb_am_handler(FunctionCallInfo) {
	PG_RETURN_POINTER(&duckdb_methods);
}
}

namespace pgduckdb {
bool
IsDuckdbTableAm(const TableAmRoutine *am) {
	return am == &duckdb_methods;
}
} // namespace pgduckdb
