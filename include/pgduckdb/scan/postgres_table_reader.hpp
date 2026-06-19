#pragma once

#include "pgduckdb/pg/declarations.hpp"

#include <vector>

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

class PostgresTableReader {
public:
	PostgresTableReader();
	~PostgresTableReader();
	void Init(const char *table_scan_query, bool count_tuples_only);
	void Cleanup();
	bool GetNextMinimalWorkerTuple(std::vector<uint8_t> &minimal_tuple_buffer);
	// In-process variant (no parallel workers): runs the scan node and copies up to `max` tuples into
	// `slots`, each of which takes ownership of its copy (freed on its next store). Returns the number of
	// slots filled; a result < `max` means EOF was reached. Caller must hold GlobalProcessLock.
	int GetNextInProcessTuples(TupleTableSlot **slots, int max);
	// count_tuples_only variant: the aggregate node returns one tuple per call
	// whose first attribute is the partial count. Returns false on EOF, otherwise
	// sets *count_out to that partial count.
	bool GetNextCount(uint64_t *count_out);
	TupleTableSlot *InitTupleSlot();
	int
	NumWorkersLaunched() const {
		return nworkers_launched;
	}

private:
	PostgresTableReader(const PostgresTableReader &) = delete;
	PostgresTableReader &operator=(const PostgresTableReader &) = delete;

	void InitUnsafe(const char *table_scan_query, bool count_tuples_only);
	void InitRunWithParallelScan(PlannedStmt *, bool);
	void CleanupUnsafe();

	TupleTableSlot *GetNextTuple();
	int GetNextInProcessTuplesUnsafe(TupleTableSlot **slots, int max);
	TupleTableSlot *GetNextTupleUnsafe();
	TupleTableSlot *ExecNextTupleUnsafe();
	MinimalTuple GetNextWorkerTuple();
	int ParallelWorkerNumber(Cardinality cardinality);
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
