#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "storage/bufmgr.h"
}

#include "pgduckdb/scan/postgres_scan.hpp"

namespace pgduckdb {

// HeapReaderGlobalState

class HeapReaderGlobalState {
public:
	HeapReaderGlobalState(Relation relation)
	    : m_nblocks(RelationGetNumberOfBlocks(relation)), m_last_assigned_block_number(InvalidBlockNumber) {
	}
	BlockNumber AssignNextBlockNumber(std::mutex &lock);
	BlockNumber m_nblocks;
	BlockNumber m_last_assigned_block_number;
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
	BlockNumber
	GetCurrentBlockNumber() {
		return m_block_number;
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
	BlockNumber m_block_number;
	Buffer m_buffer;
	OffsetNumber m_current_tuple_index;
	int m_page_tuples_left;
	HeapTupleData m_tuple;
};

} // namespace pgduckdb