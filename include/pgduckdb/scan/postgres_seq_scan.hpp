#pragma once

#include "duckdb.hpp"

#include "pgduckdb/pgduckdb_guc.h"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/scan/heap_reader.hpp"

namespace pgduckdb {

// Global State

struct PostgresSeqScanGlobalState : public duckdb::GlobalTableFunctionState {
	explicit PostgresSeqScanGlobalState(Relation rel, duckdb::TableFunctionInitInput &input);
	~PostgresSeqScanGlobalState();
	idx_t
	MaxThreads() const override;

public:
	duckdb::shared_ptr<PostgresScanGlobalState> m_global_state;
	duckdb::shared_ptr<HeapReaderGlobalState> m_heap_reader_global_state;
	Relation m_rel;
};

// Local State

struct  PostgresSeqScanLocalState : public duckdb::LocalTableFunctionState {
public:
	PostgresSeqScanLocalState(Relation rel, duckdb::shared_ptr<HeapReaderGlobalState> heap_reader_global_state,
	                          duckdb::shared_ptr<PostgresScanGlobalState> global_state);
	~PostgresSeqScanLocalState() override;

public:
	duckdb::shared_ptr<PostgresScanLocalState> m_local_state;
	duckdb::shared_ptr<HeapReaderLocalState> m_heap_reader_local_state;
	duckdb::unique_ptr<HeapReader> m_heap_table_reader;
};

// PostgresSeqScanFunctionData

struct PostgresSeqScanFunctionData : public duckdb::TableFunctionData {
public:
	PostgresSeqScanFunctionData(Relation rel, uint64_t cardinality, Snapshot snapshot);
	~PostgresSeqScanFunctionData() override;

public:
	Relation m_rel;
	uint64_t m_cardinality;
	Snapshot m_snapshot;
};

// PostgresSeqScanFunction

struct PostgresSeqScanFunction : public duckdb::TableFunction {
public:
	PostgresSeqScanFunction();

public:
	static duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
	PostgresSeqScanInitGlobal(duckdb::ClientContext &context, duckdb::TableFunctionInitInput &input);
	static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
	PostgresSeqScanInitLocal(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
	                         duckdb::GlobalTableFunctionState *gstate);
	static void PostgresSeqScanFunc(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
	                                duckdb::DataChunk &output);

	static duckdb::unique_ptr<duckdb::NodeStatistics> PostgresSeqScanCardinality(duckdb::ClientContext &context,
	                                                                             const duckdb::FunctionData *data);
};

} // namespace pgduckdb
