#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "pgstat.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/rel.h"
}

#include "quack/scan/heap_reader.hpp"
#include "quack/quack_types.hpp"

namespace quack {

//
// HeapReaderGlobalState
//

BlockNumber
HeapReaderGlobalState::AssignNextBlockNumber(std::mutex &lock) {
	lock.lock();
	BlockNumber block_number = InvalidBlockNumber;
	if (m_last_assigned_block_number == InvalidBlockNumber) {
		block_number = m_last_assigned_block_number = 0;
	} else if (m_last_assigned_block_number < m_nblocks - 1) {
		block_number = ++m_last_assigned_block_number;
	}
	lock.unlock();
	return block_number;
}

//
// HeapReader
//

HeapReader::HeapReader(Relation relation, duckdb::shared_ptr<HeapReaderGlobalState> heapReaderGlobalState,
                       duckdb::shared_ptr<PostgresScanGlobalState> globalState,
                       duckdb::shared_ptr<PostgresScanLocalState> localState)
    : m_global_state(globalState), m_heap_reader_global_state(heapReaderGlobalState), m_local_state(localState),
      m_relation(relation), m_inited(false), m_read_next_page(true), m_block_number(InvalidBlockNumber),
      m_buffer(InvalidBuffer), m_current_tuple_index(InvalidOffsetNumber), m_page_tuples_left(0) {
	m_tuple.t_data = NULL;
	m_tuple.t_tableOid = RelationGetRelid(m_relation);
	ItemPointerSetInvalid(&m_tuple.t_self);
}

HeapReader::~HeapReader() {
}

Page
HeapReader::PreparePageRead() {
	Page page = BufferGetPage(m_buffer);
	TestForOldSnapshot(m_global_state->m_snapshot, m_relation, page);
	m_page_tuples_all_visible = PageIsAllVisible(page) && !m_global_state->m_snapshot->takenDuringRecovery;
	m_page_tuples_left = PageGetMaxOffsetNumber(page) - FirstOffsetNumber + 1;
	m_current_tuple_index = FirstOffsetNumber;
	return page;
}

bool
HeapReader::ReadPageTuples(duckdb::DataChunk &output) {
	BlockNumber block = InvalidBlockNumber;
	Page page = nullptr;

	if (!m_inited) {
		block = m_block_number = m_heap_reader_global_state->AssignNextBlockNumber(m_global_state->m_lock);
		if (m_block_number == InvalidBlockNumber) {
			return false;
		}
		m_inited = true;
		m_read_next_page = true;
	} else {
		block = m_block_number;
		if (block != InvalidBlockNumber) {
			page = BufferGetPage(m_buffer);
		}
	}

	while (block != InvalidBlockNumber) {
		if (m_read_next_page) {
			CHECK_FOR_INTERRUPTS();
			m_global_state->m_lock.lock();
			block = m_block_number;
			m_buffer = ReadBufferExtended(m_relation, MAIN_FORKNUM, block, RBM_NORMAL, GetAccessStrategy(BAS_BULKREAD));
			LockBuffer(m_buffer, BUFFER_LOCK_SHARE);
			m_global_state->m_lock.unlock();
			page = PreparePageRead();
			m_read_next_page = false;
		}

		for (; m_page_tuples_left > 0 && m_local_state->m_output_vector_size < STANDARD_VECTOR_SIZE;
		     m_page_tuples_left--, m_current_tuple_index++) {
			bool visible = true;
			ItemId lpp = PageGetItemId(page, m_current_tuple_index);

			if (!ItemIdIsNormal(lpp))
				continue;

			m_tuple.t_data = (HeapTupleHeader)PageGetItem(page, lpp);
			m_tuple.t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&(m_tuple.t_self), block, m_current_tuple_index);

			if (!m_page_tuples_all_visible) {
				visible = HeapTupleSatisfiesVisibility(&m_tuple, m_global_state->m_snapshot, m_buffer);
				HeapCheckForSerializableConflictOut(visible, m_relation, &m_tuple, m_buffer,
				                                    m_global_state->m_snapshot);
				/* skip tuples not visible to this snapshot */
				if (!visible)
					continue;
			}

			pgstat_count_heap_getnext(m_relation);
			InsertTupleIntoChunk(output, m_global_state, m_local_state, &m_tuple);
		}

		/* No more items on current page */
		if (!m_page_tuples_left) {
			m_global_state->m_lock.lock();
			UnlockReleaseBuffer(m_buffer);
			m_global_state->m_lock.unlock();
			m_read_next_page = true;
			/* Handle cancel request */
			if (QueryCancelPending) {
				block = m_block_number = InvalidBlockNumber;
			} else {
				block = m_block_number = m_heap_reader_global_state->AssignNextBlockNumber(m_global_state->m_lock);
			}
		}

		/* We have collected STANDARD_VECTOR_SIZE */
		if (m_local_state->m_output_vector_size == STANDARD_VECTOR_SIZE) {
			output.SetCardinality(m_local_state->m_output_vector_size);
			m_local_state->m_output_vector_size = 0;
			return true;
		}
	}

	/* Next assigned block number is InvalidBlockNumber so we check did we write any tuples in output vector */
	if (m_local_state->m_output_vector_size) {
		output.SetCardinality(m_local_state->m_output_vector_size);
		m_local_state->m_output_vector_size = 0;
	}

	m_buffer = InvalidBuffer;
	m_block_number = InvalidBlockNumber;
	m_tuple.t_data = NULL;
	m_read_next_page = false;

	return false;
}
} // namespace quack