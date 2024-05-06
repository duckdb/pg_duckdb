#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "pgstat.h"
#include "access/valid.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
}

#include "quack/quack_heap_seq_scan.hpp"
#include "quack/quack_types.hpp"

#include <thread>

namespace quack {

PostgresHeapSeqScan::PostgresHeapSeqScan(RangeTblEntry *table)
    : m_tableEntry(table), m_rel(nullptr), m_snapshot(nullptr) {
}

PostgresHeapSeqScan::~PostgresHeapSeqScan() {
	if (IsValid()) {
		RelationClose(m_rel);
	}
}

PostgresHeapSeqScan::PostgresHeapSeqScan(PostgresHeapSeqScan &&other)
    : m_tableEntry(other.m_tableEntry), m_rel(nullptr) {
	other.CloseRelation();
	other.m_tableEntry = nullptr;
}

Relation
PostgresHeapSeqScan::GetRelation() {
	if (m_tableEntry && m_rel == nullptr) {
		m_rel = RelationIdGetRelation(m_tableEntry->relid);
	}
	return m_rel;
}

void
PostgresHeapSeqScan::CloseRelation() {
	if (IsValid()) {
		RelationClose(m_rel);
	}
	m_rel = nullptr;
}

bool
PostgresHeapSeqScan::IsValid() const {
	return RelationIsValid(m_rel);
}

TupleDesc

PostgresHeapSeqScan::GetTupleDesc() {
	return RelationGetDescr(m_rel);
}

Page
PostgresHeapSeqScan::PreparePageRead(PostgresHeapSeqScanThreadInfo &threadScanInfo) {
	Page page = BufferGetPage(threadScanInfo.m_buffer);
	TestForOldSnapshot(m_snapshot, m_rel, page);
	threadScanInfo.m_page_tuples_all_visible = PageIsAllVisible(page) && !m_snapshot->takenDuringRecovery;
	threadScanInfo.m_page_tuples_left = PageGetMaxOffsetNumber(page) - FirstOffsetNumber + 1;
	threadScanInfo.m_current_tuple_index = FirstOffsetNumber;
	return page;
}

void
PostgresHeapSeqScan::InitParallelScanState(duckdb::TableFunctionInitInput &input) {
	(void) GetRelation();
	m_parallel_scan_state.m_nblocks = RelationGetNumberOfBlocks(m_rel);

	/* SELECT COUNT(*) FROM */
	if (input.column_ids.size() == 1 && input.column_ids[0] == UINT64_MAX) {
		m_parallel_scan_state.m_count_tuples_only = true;
		return;
	}

	/* We need ordered columns ids for tuple fetch */
	for (duckdb::idx_t i = 0; i < input.column_ids.size(); i++) {
		m_parallel_scan_state.m_columns[input.column_ids[i]] = i;
	}

	if (input.CanRemoveFilterColumns()) {
		for (duckdb::idx_t i = 0; i < input.projection_ids.size(); i++) {
			m_parallel_scan_state.m_projections[i] = input.column_ids[input.projection_ids[i]];
		}
	} else {
		for (duckdb::idx_t i = 0; i < input.projection_ids.size(); i++) {
			m_parallel_scan_state.m_projections[i] = input.column_ids[i];
		}
	}

	// m_parallel_scan_state.PrefetchNextRelationPages(m_rel);
	m_parallel_scan_state.m_filters = input.filters.get();
}

bool
PostgresHeapSeqScan::ReadPageTuples(duckdb::DataChunk &output, PostgresHeapSeqScanThreadInfo &threadScanInfo) {
	BlockNumber block = InvalidBlockNumber;
	Page page = nullptr;

	if (!threadScanInfo.m_inited) {
		block = threadScanInfo.m_block_number = m_parallel_scan_state.AssignNextBlockNumber();
		if (threadScanInfo.m_block_number == InvalidBlockNumber) {
			return false;
		}
		threadScanInfo.m_inited = true;
		threadScanInfo.m_read_next_page = true;
	} else {
		block = threadScanInfo.m_block_number;
		page = BufferGetPage(threadScanInfo.m_buffer);
	}

	while (block != InvalidBlockNumber) {
		if (threadScanInfo.m_read_next_page) {
			CHECK_FOR_INTERRUPTS();
			m_parallel_scan_state.m_lock.lock();
			block = threadScanInfo.m_block_number;
			threadScanInfo.m_buffer =
			    ReadBufferExtended(m_rel, MAIN_FORKNUM, block, RBM_NORMAL, m_parallel_scan_state.m_strategy);
			LockBuffer(threadScanInfo.m_buffer, BUFFER_LOCK_SHARE);
			// m_parallel_scan_state.PrefetchNextRelationPages(m_rel);
			m_parallel_scan_state.m_lock.unlock();
			page = PreparePageRead(threadScanInfo);
			threadScanInfo.m_read_next_page = false;
		}

		for (; threadScanInfo.m_page_tuples_left > 0 && threadScanInfo.m_output_vector_size < STANDARD_VECTOR_SIZE;
		     threadScanInfo.m_page_tuples_left--, threadScanInfo.m_current_tuple_index++) {
			bool visible = true;
			ItemId lpp = PageGetItemId(page, threadScanInfo.m_current_tuple_index);

			if (!ItemIdIsNormal(lpp))
				continue;

			threadScanInfo.m_tuple.t_data = (HeapTupleHeader)PageGetItem(page, lpp);
			threadScanInfo.m_tuple.t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&(threadScanInfo.m_tuple.t_self), block, threadScanInfo.m_current_tuple_index);

			if (!threadScanInfo.m_page_tuples_all_visible) {
				visible = HeapTupleSatisfiesVisibility(&threadScanInfo.m_tuple, m_snapshot, threadScanInfo.m_buffer);
				HeapCheckForSerializableConflictOut(visible, m_rel, &threadScanInfo.m_tuple, threadScanInfo.m_buffer,
				                                    m_snapshot);
				/* skip tuples not visible to this snapshot */
				if (!visible)
					continue;
			}

			pgstat_count_heap_getnext(m_rel);
			InsertTupleIntoChunk(output, threadScanInfo, m_parallel_scan_state);
		}

		/* No more items on current page */
		if (!threadScanInfo.m_page_tuples_left) {
			m_parallel_scan_state.m_lock.lock();
			UnlockReleaseBuffer(threadScanInfo.m_buffer);
			m_parallel_scan_state.m_lock.unlock();
			threadScanInfo.m_read_next_page = true;
			block = threadScanInfo.m_block_number = m_parallel_scan_state.AssignNextBlockNumber();
		}

		/* We have collected STANDARD_VECTOR_SIZE */
		if (threadScanInfo.m_output_vector_size == STANDARD_VECTOR_SIZE) {
			output.SetCardinality(threadScanInfo.m_output_vector_size);
			threadScanInfo.m_output_vector_size = 0;
			return true;
		}
	}

	/* Next assigned block number is InvalidBlockNumber so we check did we write any tuples in output vector */
	if (threadScanInfo.m_output_vector_size) {
		output.SetCardinality(threadScanInfo.m_output_vector_size);
		threadScanInfo.m_output_vector_size = 0;
	}

	threadScanInfo.m_buffer = InvalidBuffer;
	threadScanInfo.m_block_number = InvalidBlockNumber;
	threadScanInfo.m_tuple.t_data = NULL;
	threadScanInfo.m_read_next_page = false;

	return false;
}

BlockNumber
PostgresHeapSeqParallelScanState::AssignNextBlockNumber() {
	m_lock.lock();
	BlockNumber block_number = InvalidBlockNumber;
	if (m_last_assigned_block_number == InvalidBlockNumber) {
		block_number = m_last_assigned_block_number = 0;
	} else if (m_last_assigned_block_number < m_nblocks - 1) {
		block_number = ++m_last_assigned_block_number;
	}
	m_lock.unlock();
	return block_number;
}

void
PostgresHeapSeqParallelScanState::PrefetchNextRelationPages(Relation rel) {

	BlockNumber last_batch_prefetch_block_num = m_last_prefetch_block + k_max_prefetch_block_number > m_nblocks
	                                                ? m_nblocks
	                                                : m_last_prefetch_block + k_max_prefetch_block_number;

	if (m_last_assigned_block_number != InvalidBlockNumber &&
	    (m_last_prefetch_block - m_last_assigned_block_number) > 8)
		return;

	for (BlockNumber i = m_last_prefetch_block; i < last_batch_prefetch_block_num; i++) {
		PrefetchBuffer(rel, MAIN_FORKNUM, m_last_prefetch_block);
		m_last_prefetch_block++;
	}
}

PostgresHeapSeqScanThreadInfo::PostgresHeapSeqScanThreadInfo()
    : m_tuple_desc(NULL), m_inited(false), m_read_next_page(true), m_block_number(InvalidBlockNumber),
      m_buffer(InvalidBuffer), m_current_tuple_index(InvalidOffsetNumber), m_page_tuples_left(0) {
	m_tuple.t_data = NULL;
	ItemPointerSetInvalid(&m_tuple.t_self);
}

PostgresHeapSeqScanThreadInfo::~PostgresHeapSeqScanThreadInfo() {
}

void
PostgresHeapSeqScanThreadInfo::EndScan() {
}

} // namespace quack