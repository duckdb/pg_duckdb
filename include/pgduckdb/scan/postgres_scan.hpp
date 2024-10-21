#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "access/relscan.h"
#include "executor/executor.h"
#include "nodes/pathnodes.h"
}

namespace pgduckdb {

class PostgresScanGlobalState {
public:
	PostgresScanGlobalState() : m_snapshot(nullptr), m_count_tuples_only(false), m_total_row_count(0) {
	}
	~PostgresScanGlobalState() {
	}
	void InitGlobalState(duckdb::TableFunctionInitInput &input);
	void InitRelationMissingAttrs(TupleDesc tuple_desc);
	Snapshot m_snapshot;
	TupleDesc m_tuple_desc;
	std::mutex m_lock; // Lock for one replacement scan
	bool m_count_tuples_only;
	duckdb::map<duckdb::idx_t, duckdb::column_t> m_read_columns_ids;
	duckdb::map<duckdb::idx_t, duckdb::column_t> m_output_columns_ids;
	duckdb::TableFilterSet *m_filters = nullptr;
	std::atomic<std::uint32_t> m_total_row_count;
	duckdb::map<int, Datum> m_relation_missing_attrs;
};

class PostgresScanLocalState {
public:
	PostgresScanLocalState() : m_output_vector_size(0), m_exhausted_scan(false) {
	}
	~PostgresScanLocalState() {
	}
	int m_output_vector_size;
	bool m_exhausted_scan;
};

duckdb::unique_ptr<duckdb::TableRef> PostgresReplacementScan(duckdb::ClientContext &context,
                                                             duckdb::ReplacementScanInput &input,
                                                             duckdb::optional_ptr<duckdb::ReplacementScanData> data);

} // namespace pgduckdb
