#pragma once

#include "duckdb.hpp"

#include "pgduckdb/pg/declarations.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

// PostgresTableReader

class PostgresTableReader {
public:
	PostgresTableReader(const char *table_scan_query, bool count_tuples_only);
	~PostgresTableReader();
	TupleTableSlot *GetNextTuple();
	void PostgresTableReaderCleanup();

private:
	MinimalTuple GetNextWorkerTuple();
	int ParallelWorkerNumber(Cardinality cardinality);
	const char *ExplainScanPlan(QueryDesc *query_desc);
	bool CanTableScanRunInParallel(Plan *plan);
	bool MarkPlanParallelAware(Plan *plan);

private:
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
