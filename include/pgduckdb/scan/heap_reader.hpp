#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "storage/bufmgr.h"
}

#include "pgduckdb/scan/heap_reader_worker.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"

namespace pgduckdb {

// HeapReaderGlobalState

class HeapReaderGlobalState {
public:
	HeapReaderGlobalState(Relation relation);
	~HeapReaderGlobalState();
	BlockNumber m_last_assigned_local_block_number;
	Relation m_relation;
	BlockNumber m_nblocks;
	dsm_segment *m_segment;
	PostgresScanWorkerSharedState *m_worker_shared_state;
};

// HeapReaderLocalState

class HeapReaderLocalState {
public:
	HeapReaderLocalState(Relation relation, duckdb::shared_ptr<HeapReaderGlobalState> heap_reader_global_state);
	~HeapReaderLocalState();
	void GetWorkerBuffer(Buffer *buffer);
	void GetLocalBuffer(Buffer * buffer, duckdb::shared_ptr<HeapReaderGlobalState> hrgs);
	bool m_local_relation;
	uint32 m_nblocks;
	Relation m_relation;
	dsm_segment *m_segment;
	PostgresScanThreadWorker *m_thread_worker_shared_state;
	BufferAccessStrategy m_buffer_access_strategy;
};

// HeapReader

class HeapReader {
private:
public:
	HeapReader(Relation rel, duckdb::shared_ptr<HeapReaderGlobalState> heap_reader_global_state,
	           duckdb::shared_ptr<HeapReaderLocalState> heap_reader_local_state,
	           duckdb::shared_ptr<PostgresScanGlobalState> global_state,
	           duckdb::shared_ptr<PostgresScanLocalState> local_state);
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
	duckdb::shared_ptr<HeapReaderLocalState> m_heap_reader_local_state;
	duckdb::shared_ptr<PostgresScanLocalState> m_local_state;
	Relation m_rel;
	bool m_inited;
	bool m_read_next_page;
	bool m_page_tuples_all_visible;
	Buffer m_buffer;
	BlockNumber m_block_number;
	OffsetNumber m_current_tuple_index;
	int m_page_tuples_left;
	HeapTupleData m_tuple;
};

} // namespace pgduckdb
