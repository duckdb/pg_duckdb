#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "executor/executor.h"
#include "access/relscan.h"
}

#include "quack/quack.h"
#include "quack/quack_heap_seq_scan.hpp"

// Postgres Relation

namespace quack {

struct PostgresHeapScanLocalState : public duckdb::LocalTableFunctionState {
public:
	PostgresHeapScanLocalState(PostgresHeapSeqScan &relation);
	~PostgresHeapScanLocalState() override;

public:
	PostgresHeapSeqScan &m_rel;
	PostgresHeapSeqScanThreadInfo m_thread_seq_scan_info;
	bool m_exhausted_scan = false;
};

// Global State

struct PostgresHeapScanGlobalState : public duckdb::GlobalTableFunctionState {
	explicit PostgresHeapScanGlobalState(PostgresHeapSeqScan &relation, duckdb::TableFunctionInitInput &input);
	~PostgresHeapScanGlobalState();
	idx_t
	MaxThreads() const override {
		return quack_max_threads_per_query;
	}
};

struct PostgresHeapScanFunctionData : public duckdb::TableFunctionData {
public:
	PostgresHeapScanFunctionData(PostgresHeapSeqScan &&relation, Snapshot Snapshot);
	~PostgresHeapScanFunctionData() override;

public:
	PostgresHeapSeqScan m_relation;
};

struct PostgresHeapScanFunction : public duckdb::TableFunction {
public:
	PostgresHeapScanFunction();

public:
	static duckdb::unique_ptr<duckdb::FunctionData> PostgresHeapBind(duckdb::ClientContext &context,
	                                                                 duckdb::TableFunctionBindInput &input,
	                                                                 duckdb::vector<duckdb::LogicalType> &return_types,
	                                                                 duckdb::vector<duckdb::string> &names);
	static duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
	PostgresHeapInitGlobal(duckdb::ClientContext &context, duckdb::TableFunctionInitInput &input);
	static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
	PostgresHeapInitLocal(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
	                      duckdb::GlobalTableFunctionState *gstate);
	// static idx_t PostgresMaxThreads(ClientContext &context, const FunctionData *bind_data_p);
	// static bool PostgresParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	// LocalTableFunctionState *lstate, GlobalTableFunctionState *gstate); static double PostgresProgress(ClientContext
	// &context, const FunctionData *bind_data_p, const GlobalTableFunctionState *gstate);
	static void PostgresHeapScanFunc(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
	                                 duckdb::DataChunk &output);
	// static unique_ptr<NodeStatistics> PostgresCardinality(ClientContext &context, const FunctionData *bind_data);
	// static idx_t PostgresGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	// LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state); static void
	// PostgresSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const TableFunction
	// &function);
public:
	static void InsertTupleIntoChunk(duckdb::DataChunk &output, TupleDesc tuple, TupleTableSlot *slot, idx_t offset);
};

struct PostgresHeapReplacementScanData : public duckdb::ReplacementScanData {
public:
	PostgresHeapReplacementScanData(Query *parse, const char *query) : m_parse(parse), m_query(query) {
	}
	~PostgresHeapReplacementScanData() override {};

public:
	Query *m_parse;
	std::string m_query;
};

duckdb::unique_ptr<duckdb::TableRef> PostgresHeapReplacementScan(duckdb::ClientContext &context,
                                                                 const duckdb::string &table_name,
                                                                 duckdb::ReplacementScanData *data);

} // namespace quack
