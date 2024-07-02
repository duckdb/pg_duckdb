#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "pgstat.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/rel.h"
}

#include "pgduckdb/pgduckdb_process_lock.hpp"
#include "pgduckdb/scan/heap_reader.hpp"
#include "pgduckdb/scan/heap_tuple_visibility.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

#include <optional>
#include <thread>

namespace pgduckdb {

//
// HeapReaderGlobalState
//

HeapReaderGlobalState::HeapReaderGlobalState(Relation relation)
    :  m_last_assigned_local_block_number(0), m_relation(relation), m_nblocks(RelationGetNumberOfBlocks(relation)) {

	/* Empty table so nothing to do */
	if (m_nblocks == 0) {
		return;
	}

	m_segment = dsm_create(sizeof(PostgresScanWorkerSharedState), DSM_CREATE_NULL_IF_MAXSEGMENTS);

	m_worker_shared_state = (PostgresScanWorkerSharedState *)dsm_segment_address(m_segment);

	/* Relation */
	m_worker_shared_state->n_blocks = m_nblocks;
	m_worker_shared_state->next_scan_start_block = 0;
	m_worker_shared_state->database = relation->rd_locator.dbOid;
	m_worker_shared_state->relid = relation->rd_rel->oid;
	SpinLockInit(&m_worker_shared_state->worker_lock);
}

HeapReaderGlobalState::~HeapReaderGlobalState() {
	if (m_nblocks != 0) {
		dsm_detach(m_segment);
	}
}

//
// HeapReaderLocalState
//

HeapReaderLocalState::HeapReaderLocalState(Relation relation,
                                           duckdb::shared_ptr<HeapReaderGlobalState> heap_reader_global_state) {

	m_nblocks = heap_reader_global_state->m_nblocks;

	/* Empty table so nothing to do */
	if (heap_reader_global_state->m_nblocks == 0) {
		return;
	}

	m_relation = relation;

	DuckdbProcessLock::GetLock().lock();
	m_buffer_access_strategy = GetAccessStrategy(BAS_BULKREAD);
	DuckdbProcessLock::GetLock().unlock();

	if (RELATION_IS_LOCAL(relation)) {
		m_local_relation = true;
		return;
	}

	m_local_relation = false;
	m_segment = dsm_create(sizeof(PostgresScanThreadWorker), DSM_CREATE_NULL_IF_MAXSEGMENTS);
	m_thread_worker_shared_state = (PostgresScanThreadWorker *)dsm_segment_address(m_segment);

	m_thread_worker_shared_state->workers_global_shared_state = dsm_segment_handle(heap_reader_global_state->m_segment);
	m_thread_worker_shared_state->buffer = InvalidBuffer;
	pg_atomic_init_flag_impl(&m_thread_worker_shared_state->buffer_ready);
	pg_atomic_init_flag(&m_thread_worker_shared_state->worker_done);
	pg_atomic_test_set_flag(&m_thread_worker_shared_state->thread_running);

	DuckdbProcessLock::GetLock().lock();
	PGDuckDBStartHeapReaderWorker(dsm_segment_handle(m_segment));
	DuckdbProcessLock::GetLock().unlock();
}

HeapReaderLocalState::~HeapReaderLocalState() {
	DuckdbProcessLock::GetLock().lock();
	if (m_nblocks) {
		FreeAccessStrategy(m_buffer_access_strategy);
		if (!m_local_relation) {
			pg_atomic_clear_flag(&m_thread_worker_shared_state->thread_running);
			dsm_detach(m_segment);
		}
	}
	DuckdbProcessLock::GetLock().unlock();
}

void
HeapReaderLocalState::GetWorkerBuffer(Buffer *buffer) {
	if (m_nblocks == 0) {
		*buffer = InvalidBuffer;
		return;
	}

	/* No workers - scan done*/
	if (!pg_atomic_unlocked_test_flag(&m_thread_worker_shared_state->worker_done)) {
		*buffer = InvalidBuffer;
		return;
	}

	if (BufferIsValid(*buffer)) {
		pg_atomic_clear_flag(&m_thread_worker_shared_state->buffer_ready);
	}

	/* Is buffer ready for reading */
	while (pg_atomic_unlocked_test_flag(&m_thread_worker_shared_state->buffer_ready)) {
		/* No workers - scan done*/
		if (!pg_atomic_unlocked_test_flag(&m_thread_worker_shared_state->worker_done)) {
			*buffer = InvalidBuffer;
			return;
		}
	}

	*buffer = m_thread_worker_shared_state->buffer;
}

void
HeapReaderLocalState::GetLocalBuffer(Buffer *buffer, duckdb::shared_ptr<HeapReaderGlobalState> hrgs) {
	std::lock_guard<std::mutex> lock(DuckdbProcessLock::GetLock());

	if (BufferIsValid(*buffer)) {
		UnlockReleaseBuffer(*buffer);
	}

	if (hrgs->m_last_assigned_local_block_number == m_nblocks) {
		*buffer = InvalidBuffer;
		return;
	}

	*buffer = ReadBufferExtended(m_relation, MAIN_FORKNUM, hrgs->m_last_assigned_local_block_number, RBM_NORMAL, m_buffer_access_strategy);
	LockBuffer(*buffer, BUFFER_LOCK_SHARE);
	 hrgs->m_last_assigned_local_block_number++;
}

//
// HeapReader
//

HeapReader::HeapReader(Relation rel, duckdb::shared_ptr<HeapReaderGlobalState> heap_reader_global_state,
                       duckdb::shared_ptr<HeapReaderLocalState> heap_reader_local_state,
                       duckdb::shared_ptr<PostgresScanGlobalState> global_state,
                       duckdb::shared_ptr<PostgresScanLocalState> local_state)
    : m_global_state(global_state), m_heap_reader_global_state(heap_reader_global_state),
      m_heap_reader_local_state(heap_reader_local_state), m_local_state(local_state), m_rel(rel), m_inited(false),
      m_read_next_page(true), m_buffer(InvalidBuffer), m_block_number(InvalidBlockNumber),
      m_current_tuple_index(InvalidOffsetNumber), m_page_tuples_left(0) {
	m_tuple.t_data = NULL;
	m_tuple.t_tableOid = RelationGetRelid(m_rel);
	ItemPointerSetInvalid(&m_tuple.t_self);
}

HeapReader::~HeapReader() {
	DuckdbProcessLock::GetLock().lock();
	/* If execution is interrupted and buffer is still opened close it now */
	if (m_heap_reader_local_state->m_local_relation && m_buffer != InvalidBuffer) {
		UnlockReleaseBuffer(m_buffer);
	}
	DuckdbProcessLock::GetLock().unlock();
}

Page
HeapReader::PreparePageRead() {
	Page page = BufferGetPage(m_buffer);
#if PG_VERSION_NUM < 170000
	TestForOldSnapshot(m_global_state->m_snapshot, m_rel, page);
#endif
	m_page_tuples_all_visible = PageIsAllVisible(page) && !m_global_state->m_snapshot->takenDuringRecovery;
	m_page_tuples_left = PageGetMaxOffsetNumber(page) - FirstOffsetNumber + 1;
	m_current_tuple_index = FirstOffsetNumber;
	return page;
}

bool
HeapReader::ReadPageTuples(duckdb::DataChunk &output) {
	Buffer buffer = InvalidBlockNumber;
	Page page = nullptr;

	if (!m_inited) {
		if (m_heap_reader_local_state->m_local_relation) {
			m_heap_reader_local_state->GetLocalBuffer(&m_buffer, m_heap_reader_global_state);
		} else {
			m_heap_reader_local_state->GetWorkerBuffer(&m_buffer);
		}
		buffer = m_buffer;
		if (m_buffer == InvalidBuffer) {
			return false;
		}
		m_inited = true;
		m_read_next_page = true;
	} else {
		if (!m_read_next_page) {
			page = BufferGetPage(m_buffer);
		}
	}

	while (buffer != InvalidBuffer) {
		if (m_read_next_page) {
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
			ItemPointerSet(&(m_tuple.t_self), m_block_number, m_current_tuple_index);

			if (!m_page_tuples_all_visible) {
				visible = HeapTupleSatisfiesVisibilityNoHintBits(&m_tuple, m_global_state->m_snapshot, m_buffer);
				/* skip tuples not visible to this snapshot */
				if (!visible)
					continue;
			}

			InsertTupleIntoChunk(output, m_global_state, m_local_state, &m_tuple);
		}

		/* No more items on current page */
		if (!m_page_tuples_left) {
			m_read_next_page = true;
			/* Handle cancel request */
			if (QueryCancelPending) {
				buffer = m_buffer = InvalidBlockNumber;
			} else {
				if (m_heap_reader_local_state->m_local_relation) {
					m_heap_reader_local_state->GetLocalBuffer(&m_buffer, m_heap_reader_global_state);
				} else {
					m_heap_reader_local_state->GetWorkerBuffer(&m_buffer);
				}
				buffer = m_buffer;
			}
		}

		/* We have collected STANDARD_VECTOR_SIZE */
		if (m_local_state->m_output_vector_size == STANDARD_VECTOR_SIZE) {
			output.SetCardinality(m_local_state->m_output_vector_size);
			output.Verify();
			m_local_state->m_output_vector_size = 0;
			return true;
		}
	}

	/* Next assigned block number is InvalidBlockNumber so we check did we write any tuples in output vector */
	if (m_local_state->m_output_vector_size) {
		output.SetCardinality(m_local_state->m_output_vector_size);
		output.Verify();
		m_local_state->m_output_vector_size = 0;
	}

	m_buffer = InvalidBuffer;
	m_tuple.t_data = NULL;
	m_read_next_page = false;

	return false;
}
} // namespace pgduckdb
