#include "pgduckdb/pgduckdb_types.hpp"

extern "C" {
#include "postgres.h"

#include "access/tableam.h"
#include "fmgr.h"
#include "utils/syscache.h"
}

#include "pgduckdb/utility/cpp_wrapper.hpp"
#include "pgducklake/pgducklake_table.hpp"

struct DuckLakeScanDescData {
	TableScanDescData rs_base;
};
using DuckLakeScanDesc = DuckLakeScanDescData *;

const TupleTableSlotOps *
ducklake_slot_callbacks(Relation /* rel */) {
	return &TTSOpsMinimalTuple;
}

TableScanDesc
ducklake_scan_begin(Relation rel, Snapshot snapshot, int nkeys, struct ScanKeyData *key, ParallelTableScanDesc pscan,
                    uint32 flags) {
	DuckLakeScanDesc scan = static_cast<DuckLakeScanDesc>(palloc(sizeof(DuckLakeScanDescData)));
	scan->rs_base.rs_rd = rel;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_key = key;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = pscan;
	return reinterpret_cast<TableScanDesc>(scan);
}

void
ducklake_scan_end(TableScanDesc scan) {
	DuckLakeScanDesc cscan = reinterpret_cast<DuckLakeScanDesc>(scan);
	pfree(cscan);
}

void
ducklake_scan_rescan(TableScanDesc /* scan */, struct ScanKeyData * /* key */, bool /* set_params */,
                     bool /* allow_strat */, bool /* allow_sync */, bool /* allow_pagemode */) {
	elog(ERROR, "ducklake_scan_rescan not implemented");
}

bool
ducklake_scan_getnextslot(TableScanDesc /* scan */, ScanDirection /* direction */, TupleTableSlot * /* slot */) {
	elog(ERROR, "ducklake_scan_getnextslot not implemented");
}

Size
ducklake_parallelscan_estimate(Relation /* rel */) {
	elog(ERROR, "ducklake_parallelscan_estimate not implemented");
}

Size
ducklake_parallelscan_initialize(Relation /* rel */, ParallelTableScanDesc /* pscan */) {
	elog(ERROR, "ducklake_parallelscan_initialize not implemented");
}

void
ducklake_parallelscan_reinitialize(Relation /* rel */, ParallelTableScanDesc /* pscan */) {
	elog(ERROR, "ducklake_parallelscan_reinitialize not implemented");
}

struct IndexFetchTableData *
ducklake_index_fetch_begin(Relation /* rel */) {
	elog(ERROR, "ducklake_index_fetch_begin not implemented");
}

void
ducklake_index_fetch_reset(struct IndexFetchTableData * /* data */) {
	elog(ERROR, "ducklake_index_fetch_reset not implemented");
}

void
ducklake_index_fetch_end(struct IndexFetchTableData * /* data */) {
	elog(ERROR, "ducklake_index_fetch_end not implemented");
}

bool
ducklake_index_fetch_tuple(struct IndexFetchTableData * /* scan */, ItemPointer /* tid */, Snapshot /* snapshot */,
                           TupleTableSlot * /* slot */, bool * /* call_again */, bool * /* all_dead */) {
	elog(ERROR, "ducklake_index_fetch_tuple not implemented");
}

bool
ducklake_tuple_fetch_row_version(Relation /* rel */, ItemPointer /* tid */, Snapshot /* snapshot */,
                                 TupleTableSlot * /* slot */) {
	elog(ERROR, "ducklake_tuple_fetch_row_version not implemented");
}

bool
ducklake_tuple_tid_valid(TableScanDesc /* scan */, ItemPointer /* tid */) {
	elog(ERROR, "ducklake_tuple_tid_valid not implemented");
}

void
ducklake_tuple_get_latest_tid(TableScanDesc /* scan */, ItemPointer /* tid */) {
	elog(ERROR, "ducklake_tuple_get_latest_tid not implemented");
}

bool
ducklake_tuple_satisfies_snapshot(Relation /* rel */, TupleTableSlot * /* slot */, Snapshot /* snapshot */) {
	elog(ERROR, "ducklake_tuple_satisfies_snapshot not implemented");
}

TransactionId
ducklake_index_delete_tuples(Relation /* rel */, TM_IndexDeleteOp * /* delstate */) {
	elog(ERROR, "ducklake_index_delete_tuples not implemented");
}

void
ducklake_tuple_insert(Relation /* rel */, TupleTableSlot * /* slot */, CommandId /* cid */, int /* options */,
                      struct BulkInsertStateData * /* bistate */) {
	elog(ERROR, "ducklake_tuple_insert not implemented");
}

void
ducklake_tuple_insert_speculative(Relation /* rel */, TupleTableSlot * /* slot */, CommandId /* cid */,
                                  int /* options */, struct BulkInsertStateData * /* bistate */,
                                  uint32 /* specToken */) {
	elog(ERROR, "ducklake_tuple_insert_speculative not implemented");
}

void
ducklake_tuple_complete_speculative(Relation /* rel */, TupleTableSlot * /* slot */, uint32 /* specToken */,
                                    bool /* succeeded */) {
	elog(ERROR, "ducklake_tuple_complete_speculative not implemented");
}

void
ducklake_multi_insert(Relation /* rel */, TupleTableSlot ** /* slots */, int /* nslots */, CommandId /* cid */,
                      int /* options */, struct BulkInsertStateData * /* bistate */) {
	elog(ERROR, "ducklake_multi_insert not implemented");
}

TM_Result
ducklake_tuple_delete(Relation /* rel */, ItemPointer /* tid */, CommandId /* cid */, Snapshot /* snapshot */,
                      Snapshot /* crosscheck */, bool /* wait */, TM_FailureData * /* tmfd */,
                      bool /* changingPart */) {
	elog(ERROR, "ducklake_tuple_delete not implemented");
}

#if PG_VERSION_NUM >= 160000
TM_Result
ducklake_tuple_update(Relation /* rel */, ItemPointer /* otid */, TupleTableSlot * /* slot */, CommandId /* cid */,
                      Snapshot /* snapshot */, Snapshot /* crosscheck */, bool /* wait */, TM_FailureData * /* tmfd */,
                      LockTupleMode * /* lockmode */, TU_UpdateIndexes * /* update_indexes */) {
#else
TM_Result
ducklake_tuple_update(Relation /* rel */, ItemPointer /* otid */, TupleTableSlot * /* slot */, CommandId /* cid */,
                      Snapshot /* snapshot */, Snapshot /* crosscheck */, bool /* wait */, TM_FailureData * /* tmfd */,
                      LockTupleMode * /* lockmode */, bool * /* update_indexes */) {
#endif
	elog(ERROR, "ducklake_tuple_update not implemented");
}

TM_Result
ducklake_tuple_lock(Relation /* rel */, ItemPointer /* tid */, Snapshot /* snapshot */, TupleTableSlot * /* slot */,
                    CommandId /* cid */, LockTupleMode /* mode */, LockWaitPolicy /* wait_policy */, uint8 /* flags */,
                    TM_FailureData * /* tmfd */) {
	elog(ERROR, "ducklake_tuple_lock not implemented");
}

#if PG_VERSION_NUM >= 160000
void
ducklake_relation_set_new_filelocator(Relation rel, const RelFileLocator * /* newrlocator */, char /* persistence */,
                                      TransactionId * /* freezeXid */, MultiXactId * /* minmulti */) {
#else
void
ducklake_relation_set_new_filenode(Relation rel, const RelFileNode * /* newrnode */, char /* persistence */,
                                   TransactionId * /* freezeXid */, MultiXactId * /* minmulti */) {
#endif
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(rel->rd_id));
	if (!HeapTupleIsValid(tp)) {
		TupleDesc desc = RelationGetDescr(rel);
		for (int i = 0; i < desc->natts; i++) {
			Form_pg_attribute attr = &desc->attrs[i];
			auto duck_type = pgduckdb::ConvertPostgresToDuckColumnType(attr);
			if (duck_type.id() == duckdb::LogicalTypeId::USER) {
				elog(ERROR, "column \"%s\" has unsupported type", NameStr(attr->attname));
			}
			if (attr->attgenerated) {
				elog(ERROR, "unsupported generated column \"%s\"", NameStr(attr->attname));
			}
		}

		InvokeCPPFunc(pgduckdb::PgDuckLakeTable::CreateTable, rel);
	} else {
		ReleaseSysCache(tp);
		elog(ERROR, "ducklake_relation_set_new_filenode not implemented");
	}
}

void
ducklake_relation_nontransactional_truncate(Relation /* rel */) {
	elog(ERROR, "ducklake_relation_nontransactional_truncate not implemented");
}

#if PG_VERSION_NUM >= 160000
void
ducklake_relation_copy_data(Relation /* rel */, const RelFileLocator * /* newrlocator */) {
#else
void
ducklake_relation_copy_data(Relation /* rel */, const RelFileNode * /* newrnode */) {
#endif
	elog(ERROR, "ducklake_relation_copy_data not implemented");
}

void
ducklake_relation_copy_for_cluster(Relation /* OldTable */, Relation /* NewTable */, Relation /* OldIndex */,
                                   bool /* use_sort */, TransactionId /* OldestXmin */,
                                   TransactionId * /* xid_cutoff */, MultiXactId * /* multi_cutoff */,
                                   double * /* num_tuples */, double * /* tups_vacuumed */,
                                   double * /* tups_recently_dead */) {
	elog(ERROR, "ducklake_relation_copy_for_cluster not implemented");
}

void
ducklake_relation_vacuum(Relation /* rel */, struct VacuumParams * /* params */, BufferAccessStrategy /* bstrategy */) {
	elog(ERROR, "ducklake_relation_vacuum not implemented");
}

#if PG_VERSION_NUM >= 170000
bool
ducklake_scan_analyze_next_block(TableScanDesc /* scan */, ReadStream * /* stream */) {
#else
bool
ducklake_scan_analyze_next_block(TableScanDesc /* scan */, BlockNumber /* blockno */,
                                 BufferAccessStrategy /* bstrategy */) {
#endif
	return false;
}

bool
ducklake_scan_analyze_next_tuple(TableScanDesc /* scan */, TransactionId /* OldestXmin */, double * /* liverows */,
                                 double * /* deadrows */, TupleTableSlot * /* slot */) {
	elog(ERROR, "ducklake_scan_analyze_next_tuple not implemented");
}

static double
ducklake_index_build_range_scan(Relation /* table_rel */, Relation /* index_rel */, struct IndexInfo * /* index_info */,
                                bool /* allow_sync */, bool /* anyvisible */, bool /* progress */,
                                BlockNumber /* start_blockno */, BlockNumber /* numblocks */,
                                IndexBuildCallback /* callback */, void * /* callback_state */,
                                TableScanDesc /* scan */) {
	elog(ERROR, "ducklake_index_build_range_scan not implemented");
}

static void
ducklake_index_validate_scan(Relation /* table_rel */, Relation /* index_rel */, struct IndexInfo * /* index_info */,
                             Snapshot /* snapshot */, struct ValidateIndexState * /* state */) {
	elog(ERROR, "ducklake_index_validate_scan not implemented");
}

static uint64
ducklake_relation_size(Relation /* rel */, ForkNumber /* forkNumber */) {
	return 0;
}

static bool
ducklake_relation_needs_toast_table(Relation /* rel */) {
	return false;
}

static void
ducklake_relation_estimate_size(Relation /* rel */, int32 *attr_widths, BlockNumber *pages, double *tuples,
                                double *allvisfrac) {
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

bool
ducklake_scan_sample_next_block(TableScanDesc /* scan */, struct SampleScanState * /* scanstate */) {
	elog(ERROR, "ducklake_scan_sample_next_block not implemented");
}

bool
ducklake_scan_sample_next_tuple(TableScanDesc /* scan */, struct SampleScanState * /* scanstate */,
                                TupleTableSlot * /* slot */) {
	elog(ERROR, "ducklake_scan_sample_next_tuple not implemented");
}

const TableAmRoutine ducklake_routine = {T_TableAmRoutine,

                                         ducklake_slot_callbacks,

                                         ducklake_scan_begin,
                                         ducklake_scan_end,
                                         ducklake_scan_rescan,
                                         ducklake_scan_getnextslot,

                                         NULL /*scan_set_tidrange*/,
                                         NULL /*scan_getnextslot_tidrange*/,

                                         ducklake_parallelscan_estimate,
                                         ducklake_parallelscan_initialize,
                                         ducklake_parallelscan_reinitialize,

                                         ducklake_index_fetch_begin,
                                         ducklake_index_fetch_reset,
                                         ducklake_index_fetch_end,
                                         ducklake_index_fetch_tuple,

                                         ducklake_tuple_fetch_row_version,
                                         ducklake_tuple_tid_valid,
                                         ducklake_tuple_get_latest_tid,
                                         ducklake_tuple_satisfies_snapshot,
                                         ducklake_index_delete_tuples,

                                         ducklake_tuple_insert,
                                         ducklake_tuple_insert_speculative,
                                         ducklake_tuple_complete_speculative,
                                         ducklake_multi_insert,
                                         ducklake_tuple_delete,
                                         ducklake_tuple_update,
                                         ducklake_tuple_lock,
                                         NULL /*finish_bulk_insert*/,

#if PG_VERSION_NUM >= 160000
                                         ducklake_relation_set_new_filelocator,
#else
                                         ducklake_relation_set_new_filenode,
#endif
                                         ducklake_relation_nontransactional_truncate,
                                         ducklake_relation_copy_data,
                                         ducklake_relation_copy_for_cluster,
                                         ducklake_relation_vacuum,
                                         ducklake_scan_analyze_next_block,
                                         ducklake_scan_analyze_next_tuple,
                                         ducklake_index_build_range_scan,
                                         ducklake_index_validate_scan,

                                         ducklake_relation_size,
                                         ducklake_relation_needs_toast_table,
                                         NULL /*relation_toast_am*/,
                                         NULL /*relation_fetch_toast_slice*/,

                                         ducklake_relation_estimate_size,

                                         NULL /*scan_bitmap_next_block*/,
                                         NULL /*scan_bitmap_next_tuple*/,
                                         ducklake_scan_sample_next_block,
                                         ducklake_scan_sample_next_tuple};

extern "C" {
PG_FUNCTION_INFO_V1(ducklake_handler);
Datum
ducklake_handler(PG_FUNCTION_ARGS) {
	PG_RETURN_POINTER(&ducklake_routine);
}
}

namespace pgduckdb {

bool
IsDuckLakeTable(Relation rel) {
	return rel->rd_tableam == &ducklake_routine;
}

bool
IsDuckLakeTable(Oid oid) {
	if (oid == InvalidOid) {
		return false;
	}

	Relation rel = RelationIdGetRelation(oid);
	bool result = IsDuckLakeTable(rel);
	RelationClose(rel);
	return result;
}

} // namespace pgduckdb
