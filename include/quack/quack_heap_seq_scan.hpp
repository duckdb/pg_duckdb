#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
}

#include <mutex>
#include <atomic>

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

class PostgresHeapSeqParallelScanState {
private:
	static int const k_max_prefetch_block_number = 32;

public:
	PostgresHeapSeqParallelScanState()
	    : m_nblocks(InvalidBlockNumber), m_last_assigned_block_number(InvalidBlockNumber), m_total_row_count(0),
	      m_last_prefetch_block(0), m_strategy(nullptr) {
	}
	~PostgresHeapSeqParallelScanState() {
		if (m_strategy)
			pfree(m_strategy);
	}
	BlockNumber AssignNextBlockNumber();
	void PrefetchNextRelationPages(Relation rel);
	std::mutex m_lock;
	BlockNumber m_nblocks;
	BlockNumber m_last_assigned_block_number;
	duckdb::map<duckdb::column_t, duckdb::idx_t> m_columns;
	duckdb::map<duckdb::idx_t, duckdb::column_t> m_projections;
	duckdb::TableFilterSet *m_filters = nullptr;
	std::atomic<std::uint32_t> m_total_row_count;
	BlockNumber m_last_prefetch_block;
	BufferAccessStrategy m_strategy;
};

class PostgresHeapSeqScan {
private:
public:
	PostgresHeapSeqScan(RangeTblEntry *table);
	~PostgresHeapSeqScan();
	PostgresHeapSeqScan(const PostgresHeapSeqScan &other) = delete;
	PostgresHeapSeqScan &operator=(const PostgresHeapSeqScan &other) = delete;
	PostgresHeapSeqScan &operator=(PostgresHeapSeqScan &&other) = delete;
	PostgresHeapSeqScan(PostgresHeapSeqScan &&other);

public:
	void InitParallelScanState(const duckdb::vector<duckdb::column_t> &columns,
	                           const duckdb::vector<duckdb::idx_t> &projections, duckdb::TableFilterSet *filters);
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
	PostgresHeapSeqParallelScanState m_parallel_scan_state;
};

} // namespace quack