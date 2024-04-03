#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/tableam.h"
#include "access/heapam.h"
}

#include <mutex>

namespace quack {

class PostgresHeapSeqScanThreadInfo {
public:
	PostgresHeapSeqScanThreadInfo();
	~PostgresHeapSeqScanThreadInfo();
	void EndScan();

public:
	TupleDesc m_tuple_desc;
	bool m_inited;
	bool m_read_next_page;
	bool m_page_tuples_all_visible;
	int m_output_vector_size;
	BlockNumber m_block_number;
	Buffer m_buffer;
	OffsetNumber m_current_tuple_index;
	int m_page_tuples_left;
	HeapTupleData m_tuple;
};

class PostgresHeapSeqScan {
private:
	class ParallelScanState {
	public:
		ParallelScanState() : m_nblocks(InvalidBlockNumber), m_last_assigned_block_number(InvalidBlockNumber) {
		}
		BlockNumber AssignNextBlockNumber();
		std::mutex m_lock;
		BlockNumber m_nblocks;
		BlockNumber m_last_assigned_block_number;
	};

public:
	PostgresHeapSeqScan(RangeTblEntry *table);
	~PostgresHeapSeqScan();
	PostgresHeapSeqScan(const PostgresHeapSeqScan &other) = delete;
	PostgresHeapSeqScan &operator=(const PostgresHeapSeqScan &other) = delete;
	PostgresHeapSeqScan &operator=(PostgresHeapSeqScan &&other) = delete;
	PostgresHeapSeqScan(PostgresHeapSeqScan &&other);

public:
	void InitParallelScanState();
	void
	SetSnapshot(Snapshot snapshot) {
		m_snapshot = snapshot;
	}

public:
	Relation GetRelation();
	TupleDesc GetTupleDesc();
	bool ReadPageTuples(duckdb::DataChunk &output, PostgresHeapSeqScanThreadInfo &threadScanInfo);
	bool IsValid() const;

private:
	Page PreparePageRead(PostgresHeapSeqScanThreadInfo &threadScanInfo);

private:
	Relation m_rel = nullptr;
	Snapshot m_snapshot = nullptr;
	ParallelScanState m_parallel_scan_state;
};

} // namespace quack