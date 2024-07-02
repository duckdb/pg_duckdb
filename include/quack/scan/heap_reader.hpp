#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "storage/bufmgr.h"
}

#include "quack/scan/postgres_scan.hpp"
#include "quack/scan/heap_reader_worker.hpp"

#include <thread>

namespace quack {

// HeapReaderGlobalState

class HeapReaderGlobalState {
public:
	HeapReaderGlobalState(Relation relation);
	~HeapReaderGlobalState();
	Buffer AssignBuffer(idx_t readerId, BlockNumber *blockNumber);
	void ReleaseBuffer(idx_t readerId);
	BlockNumber m_nblocks;
	BlockNumber m_last_assigned_block_number;
	dsm_segment * m_segment;
	BackgroundWorkerHandle * m_worker_handle; 
	std::thread m_local_worker_thread;
	QuackWorkerSharedState * m_workers_shared_state;
	std::vector<uint16> m_reader_buffer_idx;
	uint16 m_num_readers; /* Number of reader threads*/
};

// HeapReader

class HeapReader {
private:
public:
	HeapReader(Relation relation, duckdb::shared_ptr<HeapReaderGlobalState> heapReaderGlobalState,
	           duckdb::shared_ptr<PostgresScanGlobalState> globalState,
	           duckdb::shared_ptr<PostgresScanLocalState> localState);
	~HeapReader();
	HeapReader(const HeapReader &other) = delete;
	HeapReader &operator=(const HeapReader &other) = delete;
	HeapReader &operator=(HeapReader &&other) = delete;
	HeapReader(HeapReader &&other) = delete;
	bool ReadPageTuples(duckdb::DataChunk &output);
	Buffer
	GetCurrentBuffer() {
		return m_buffer;
	}
private:
	Page PreparePageRead();
private:
	duckdb::shared_ptr<PostgresScanGlobalState> m_global_state;
	duckdb::shared_ptr<HeapReaderGlobalState> m_heap_reader_global_state;
	duckdb::shared_ptr<PostgresScanLocalState> m_local_state;
	Relation m_relation;
	bool m_inited;
	bool m_read_next_page;
	bool m_page_tuples_all_visible;
	Buffer m_buffer;
	BlockNumber m_block_number;
	OffsetNumber m_current_tuple_index;
	int m_page_tuples_left;
	HeapTupleData m_tuple;
	uint16 m_reader_id;
};

} // namespace quack