#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "access/relscan.h"
#include "executor/executor.h"
#include "nodes/pathnodes.h"
}

#include "pgduckdb/utility/allocator.hpp"

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
	/* Postgres column id to duckdb output vector idx */
	std::vector<duckdb::pair<duckdb::column_t, duckdb::idx_t>> m_input_columns;
	std::vector<duckdb::TableFilter *> m_column_filters;
	/* Duckdb output vector idx with information about postgres column id */
	duckdb::vector<duckdb::pair<duckdb::idx_t, duckdb::column_t>> m_output_columns;
	std::atomic<std::uint32_t> m_total_row_count;
	duckdb::map<int, Datum> m_relation_missing_attrs;
};

class PostgresScanLocalState {
public:
	PostgresScanLocalState(const PostgresScanGlobalState *psgs)
	    : m_output_vector_size(0), m_exhausted_scan(false) {
		if (!psgs->m_count_tuples_only) {
			const auto s = psgs->m_input_columns.size();
			values.reserve(s);
			nulls.reserve(s);
		}
	}

	int m_output_vector_size;
	bool m_exhausted_scan;
	std::vector<Datum, DuckDBMallocator<Datum>> values;
	std::vector<bool, DuckDBMallocator<bool>> nulls;
};

duckdb::unique_ptr<duckdb::TableRef> PostgresReplacementScan(duckdb::ClientContext &context,
                                                             duckdb::ReplacementScanInput &input,
                                                             duckdb::optional_ptr<duckdb::ReplacementScanData> data);

} // namespace pgduckdb
