#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "pgstat.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/rel.h"
}

#include "quack/quack_process_lock.hpp"
#include "quack/scan/heap_reader.hpp"
#include "quack/quack_types.hpp"

#include <thread>

namespace quack {

//
// HeapReaderGlobalState
//

static const uint32 parallel_seqscan_nchunks = 2048;
static const uint32 worker_buffer_write_size = 2; /* number of */

HeapReaderGlobalState::HeapReaderGlobalState(Relation relation)
    : m_nblocks(RelationGetNumberOfBlocks(relation)),
      m_last_assigned_block_number(InvalidBlockNumber), m_num_readers(0) {

	uint8 plannedWorkers = 2; /* TODO: dynamic ?! */
	uint32 numberOfExpectedBuffers = plannedWorkers * worker_buffer_write_size;

	Size bufferStatusSize = sizeof(uint8) * numberOfExpectedBuffers;
	Size bufferBlockNumberSize = sizeof(BlockNumber) * numberOfExpectedBuffers;
	Size bufferSize = sizeof(Buffer) * numberOfExpectedBuffers;
	Size workerReleaseFlags = sizeof(pg_atomic_flag) * plannedWorkers;

	m_segment = dsm_create(sizeof(QuackWorkerSharedState) + workerReleaseFlags + bufferStatusSize +
	                           bufferBlockNumberSize + bufferSize,
	                       DSM_CREATE_NULL_IF_MAXSEGMENTS);

	m_workers_shared_state = (QuackWorkerSharedState *)dsm_segment_address(m_segment);

	SpinLockInit(&m_workers_shared_state->lock);

	pg_atomic_init_u32(&m_workers_shared_state->n_active_workers, 0);
	pg_atomic_init_u32(&m_workers_shared_state->next_worker_id, 0);
	m_workers_shared_state->planned_workers = plannedWorkers;
	m_workers_shared_state->worker_release_flags =
	    (pg_atomic_flag *)((uint8 *)dsm_segment_address(m_segment) + sizeof(quack::QuackWorkerSharedState));

	m_workers_shared_state->n_blocks = m_nblocks;
	m_workers_shared_state->database = relation->rd_locator.dbOid;
	m_workers_shared_state->relid = relation->rd_rel->oid;
	pg_atomic_test_set_flag(&m_workers_shared_state->scan_next);

	for (int i = 0; i < plannedWorkers; i++) {
		pg_atomic_clear_flag(&m_workers_shared_state->worker_release_flags[i]);
	}

	m_workers_shared_state->worker_buffer_write_size = worker_buffer_write_size;
	m_workers_shared_state->worker_scan_block_size =
	    Max(worker_buffer_write_size, pg_nextpower2_32(Max(m_nblocks / parallel_seqscan_nchunks, 1)));
	m_workers_shared_state->expected_buffer_size = numberOfExpectedBuffers;
	pg_atomic_init_u32(&m_workers_shared_state->real_buffer_size, 0);

	m_workers_shared_state->next_buffer_idx = 0;
	m_workers_shared_state->buffer_status =
	    (uint8 *)(m_workers_shared_state->worker_release_flags + workerReleaseFlags);
	m_workers_shared_state->buffer_block_number =
	    (BlockNumber *)(m_workers_shared_state->buffer_status + bufferStatusSize);
	m_workers_shared_state->buffer = (Buffer *)(m_workers_shared_state->buffer_block_number + bufferBlockNumberSize);

	quack_start_heap_reader_worker(dsm_segment_handle(m_segment));
	m_local_worker_thread = std::thread(quack_heap_reader_worker, dsm_segment_handle(m_segment));

	uint32 expected_n_active_workers = plannedWorkers;
	for (;;) {
		if (pg_atomic_compare_exchange_u32(&m_workers_shared_state->n_active_workers, &expected_n_active_workers,
		                                   expected_n_active_workers)) {
			break;
		}
		expected_n_active_workers = plannedWorkers;
	}
}

HeapReaderGlobalState::~HeapReaderGlobalState() {
	m_local_worker_thread.join();
	dsm_detach(m_segment);
}

Buffer
HeapReaderGlobalState::AssignBuffer(idx_t readerId, BlockNumber *blockNumber) {
	Buffer buffer = InvalidBuffer;
	*blockNumber = InvalidBlockNumber;

	do {

		/* No workers - scan done*/
		if (pg_atomic_unlocked_test_flag(&m_workers_shared_state->scan_next)) {
			return buffer;
		}

		SpinLockAcquire(&m_workers_shared_state->lock);

		uint32 next_buffer_idx =
		    (m_workers_shared_state->next_buffer_idx) % pg_atomic_read_u32(&m_workers_shared_state->real_buffer_size);

		if (m_workers_shared_state->buffer_status[next_buffer_idx] == BUFFER_READ) {
			buffer = m_workers_shared_state->buffer[next_buffer_idx];
			*blockNumber = m_workers_shared_state->buffer_block_number[next_buffer_idx];
			m_workers_shared_state->buffer_status[next_buffer_idx] = BUFFER_ASSIGNED;
			// elog(WARNING, "(%lu) Assigned %d (%d) (%d)", std::hash<std::thread::id> {}(std::this_thread::get_id()),
			//      *blockNumber, next_buffer_idx, pg_atomic_read_u32(&m_workers_shared_state->real_buffer_size));
			m_reader_buffer_idx[readerId] = next_buffer_idx;
			m_workers_shared_state->next_buffer_idx = (++m_workers_shared_state->next_buffer_idx) %
			                                           pg_atomic_read_u32(&m_workers_shared_state->real_buffer_size);
			// elog(WARNING, "(%lu) Next %d", std::hash<std::thread::id> {}(std::this_thread::get_id()),
			//      m_workers_shared_state->next_buffer_idx);
		} else if(m_workers_shared_state->buffer_status[next_buffer_idx] == BUFFER_SKIP) {
			m_workers_shared_state->next_buffer_idx = (++m_workers_shared_state->next_buffer_idx) %
			                                          pg_atomic_read_u32(&m_workers_shared_state->real_buffer_size);
		}

		SpinLockRelease(&m_workers_shared_state->lock);

	} while (buffer == InvalidBuffer);

	return buffer;
}

void
HeapReaderGlobalState::ReleaseBuffer(idx_t readerId) {

	SpinLockAcquire(&m_workers_shared_state->lock);

	uint32 bufferIdx = m_reader_buffer_idx[readerId];
	uint16 workerId = bufferIdx / m_workers_shared_state->worker_buffer_write_size;
	bool allBuffersConsumed = true;

	m_workers_shared_state->buffer_status[bufferIdx] = BUFFER_DONE;

	for (int i = 0; i < m_workers_shared_state->worker_buffer_write_size; i++) {
		allBuffersConsumed &=
		    (m_workers_shared_state->buffer_status[workerId * m_workers_shared_state->worker_buffer_write_size + i] ==
		     BUFFER_DONE);
	}

	// elog(WARNING, "(%lu) HeapReaderGlobalState::ReleaseBuffer %d/%d/%d/%d",
	//      std::hash<std::thread::id> {}(std::this_thread::get_id()), bufferIdx,
	//      m_workers_shared_state->buffer_block_number[bufferIdx], allBuffersConsumed, workerId + 1);

	SpinLockRelease(&m_workers_shared_state->lock);

	if (allBuffersConsumed) {
		pg_atomic_test_set_flag(&m_workers_shared_state->worker_release_flags[workerId]);
	}

	// SpinLockRelease(&m_workers_shared_state->lock);
}

//
// HeapReader
//

HeapReader::HeapReader(Relation relation, duckdb::shared_ptr<HeapReaderGlobalState> heapReaderGlobalState,
                       duckdb::shared_ptr<PostgresScanGlobalState> globalState,
                       duckdb::shared_ptr<PostgresScanLocalState> localState)
    : m_global_state(globalState), m_heap_reader_global_state(heapReaderGlobalState), m_local_state(localState),
      m_relation(relation), m_inited(false), m_read_next_page(true), m_buffer(InvalidBuffer),
      m_current_tuple_index(InvalidOffsetNumber), m_page_tuples_left(0) {
	m_tuple.t_data = NULL;
	m_tuple.t_tableOid = RelationGetRelid(m_relation);
	ItemPointerSetInvalid(&m_tuple.t_self);
	m_global_state->m_lock.lock();
	m_reader_id = m_heap_reader_global_state->m_num_readers++;
	m_heap_reader_global_state->m_reader_buffer_idx.push_back(0);
	m_global_state->m_lock.unlock();
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
	Buffer buffer = InvalidBlockNumber;
	Page page = nullptr;

	if (!m_inited) {
		buffer = m_buffer = m_heap_reader_global_state->AssignBuffer(m_reader_id, &m_block_number);
		if (m_buffer == InvalidBuffer) {
			return false;
		}
		m_inited = true;
		m_read_next_page = true;
	} else {
		buffer = m_buffer;
		if (buffer != InvalidBlockNumber) {
			page = BufferGetPage(m_buffer);
		}
	}

	while (buffer != InvalidBuffer) {
		if (m_read_next_page) {
			CHECK_FOR_INTERRUPTS();
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
				visible = HeapTupleSatisfiesVisibility(&m_tuple, m_global_state->m_snapshot, m_buffer);
				/* skip tuples not visible to this snapshot */
				if (!visible)
					continue;
			}

			pgstat_count_heap_getnext(m_relation);
			InsertTupleIntoChunk(output, m_global_state, m_local_state, &m_tuple);
		}

		/* No more items on current page */
		if (!m_page_tuples_left) {
			m_read_next_page = true;
			/* Handle cancel request */
			if (QueryCancelPending) {
				buffer = m_buffer = InvalidBlockNumber;
			} else {
				m_heap_reader_global_state->ReleaseBuffer(m_reader_id);
				m_buffer = buffer = m_heap_reader_global_state->AssignBuffer(m_reader_id, &m_block_number);
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
	m_tuple.t_data = NULL;
	m_read_next_page = false;

	return false;
}

} // namespace quack