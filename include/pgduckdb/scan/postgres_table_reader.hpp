#pragma once

#include "duckdb.hpp"

#include "pgduckdb/pg/declarations.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

// PostgresTableReader

class PostgresTableReader {
public:
	PostgresTableReader(const char *table_scan_query);
	~PostgresTableReader();
	TupleTableSlot *GetNextTuple();

private:
	MinimalTuple GetNextWorkerTuple();
	int ParallelWorkerNumber(Cardinality cardinality);

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
};

} // namespace pgduckdb
