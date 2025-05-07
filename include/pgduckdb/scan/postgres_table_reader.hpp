#pragma once

#include "pgduckdb/pg/declarations.hpp"

#include <vector>

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

class PostgresTableReader {
public:
	PostgresTableReader(const char *table_scan_query, bool count_tuples_only);
	~PostgresTableReader();
	TupleTableSlot *GetNextTuple();
	bool GetNextMinimalTuple(std::vector<uint8_t> &minimal_tuple_buffer);
	void PostgresTableReaderCleanup();
	TupleTableSlot *InitTupleSlot();
	bool
	IsParallelScan() const {
		return nworkers_launched > 0;
	}

private:
	void PostgresTableReaderCleanupUnsafe();
	MinimalTuple GetNextWorkerTuple();
	int ParallelWorkerNumber(Cardinality cardinality);
	const char *ExplainScanPlan(QueryDesc *query_desc);
	bool CanTableScanRunInParallel(Plan *plan);
	bool MarkPlanParallelAware(Plan *plan);

	QueryDesc *table_scan_query_desc;
	PlanState *table_scan_planstate;
	ParallelExecutorInfo *parallel_executor_info;
	void **parallel_worker_readers;
	TupleTableSlot *slot;
	int nworkers_launched;
	int nreaders;
	int next_parallel_reader;
	bool entered_parallel_mode;
	bool cleaned_up;
};

} // namespace pgduckdb
