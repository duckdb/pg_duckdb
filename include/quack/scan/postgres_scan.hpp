#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "access/relscan.h"
#include "executor/executor.h"
#include "nodes/pathnodes.h"
}

namespace quack {

class PostgresScanGlobalState {
public:
	PostgresScanGlobalState()
	    : m_snapshot(nullptr), m_count_tuples_only(false), m_total_row_count(0) {
	}
	~PostgresScanGlobalState() {
	}
	void InitGlobalState(duckdb::TableFunctionInitInput &input);
	Snapshot m_snapshot;
	TupleDesc m_tuple_desc;
	std::mutex m_lock;
	bool m_count_tuples_only;
	duckdb::map<duckdb::idx_t, duckdb::idx_t> m_columns;
	duckdb::map<duckdb::idx_t, duckdb::idx_t> m_projections;
	duckdb::TableFilterSet *m_filters = nullptr;
	std::atomic<std::uint32_t> m_total_row_count;
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

struct PostgresReplacementScanData : public duckdb::ReplacementScanData {
public:
	PostgresReplacementScanData(List *rtables, PlannerInfo *queryPlannerInfo, List *neededColumns,
	                            const char *query_string)
	    : m_rtables(rtables), m_query_planner_info(queryPlannerInfo), m_needed_columns(neededColumns),
	      m_query_string(query_string) {
	}
	~PostgresReplacementScanData() override {};

public:
	List *m_rtables;
	PlannerInfo *m_query_planner_info;
	List *m_needed_columns;
	std::string m_query_string;
};

duckdb::unique_ptr<duckdb::TableRef> PostgresReplacementScan(duckdb::ClientContext &context,
                                                             duckdb::ReplacementScanInput &input,
                                                             duckdb::optional_ptr<duckdb::ReplacementScanData> data);

} // namespace quack
