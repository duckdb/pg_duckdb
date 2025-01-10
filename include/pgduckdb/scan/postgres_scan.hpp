#pragma once

#include "duckdb.hpp"

#include "pgduckdb/pg/declarations.hpp"
#include "pgduckdb/utility/allocator.hpp"

#include "pgduckdb/scan/postgres_table_reader.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

// Global State

struct PostgresScanGlobalState : public duckdb::GlobalTableFunctionState {
	explicit PostgresScanGlobalState(Snapshot, Relation rel, const duckdb::TableFunctionInitInput &input);
	~PostgresScanGlobalState();
	idx_t
	MaxThreads() const override {
		return 1;
	}
	void ConstructTableScanQuery(const duckdb::TableFunctionInitInput &input);

public:
	Snapshot snapshot;
	Relation rel;
	TupleDesc table_tuple_desc;
	bool count_tuples_only;
	duckdb::vector<AttrNumber> output_columns;
	std::atomic<std::uint32_t> total_row_count;
	std::ostringstream scan_query;
	duckdb::shared_ptr<PostgresTableReader> table_reader_global_state;
};

// Local State

struct PostgresScanLocalState : public duckdb::LocalTableFunctionState {
	PostgresScanLocalState(PostgresScanGlobalState *global_state);
	~PostgresScanLocalState() override;

	PostgresScanGlobalState *global_state;

	size_t output_vector_size;
	bool exhausted_scan;
};

// PostgresScanFunctionData

struct PostgresScanFunctionData : public duckdb::TableFunctionData {
	PostgresScanFunctionData(Relation rel, uint64_t cardinality, Snapshot snapshot);
	~PostgresScanFunctionData() override;

	duckdb::vector<duckdb::string> complex_filters;
	Relation rel;
	uint64_t cardinality;
	Snapshot snapshot;
};

// PostgresScanTableFunction

struct PostgresScanTableFunction : public duckdb::TableFunction {
	PostgresScanTableFunction();

	static duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
	PostgresScanInitGlobal(duckdb::ClientContext &context, duckdb::TableFunctionInitInput &input);

	static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
	PostgresScanInitLocal(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
	                      duckdb::GlobalTableFunctionState *gstate);

	static void PostgresScanFunction(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
	                                 duckdb::DataChunk &output);

	static duckdb::unique_ptr<duckdb::NodeStatistics> PostgresScanCardinality(duckdb::ClientContext &context,
	                                                                          const duckdb::FunctionData *data);
	static std::string ToString(const duckdb::FunctionData *bind_data);
};

} // namespace pgduckdb
