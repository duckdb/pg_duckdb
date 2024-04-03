#pragma once

#include "duckdb.hpp"
#include "postgres.h"
#include "miscadmin.h"

extern "C" {
#include "executor/executor.h"
} // extern "C"

// Postgres Relation

class PostgresRelation {
public:
	PostgresRelation(RangeTblEntry *table);
	~PostgresRelation();
	PostgresRelation(const PostgresRelation &other) = delete;
	PostgresRelation &operator=(const PostgresRelation &other) = delete;
	PostgresRelation &operator=(PostgresRelation &&other) = delete;
	PostgresRelation(PostgresRelation &&other);

public:
	Relation GetRelation();
	bool IsValid() const;

private:
	Relation rel = nullptr;
};

namespace duckdb {

// Local State

struct PostgresScanLocalState : public LocalTableFunctionState {
public:
	PostgresScanLocalState(PostgresRelation &relation, Snapshot snapshot);
	~PostgresScanLocalState() override;

public:
	TableScanDesc scanDesc = nullptr;
	const struct TableAmRoutine *tableam;
	bool exhausted_scan = false;
};

// Global State

struct PostgresScanGlobalState : public GlobalTableFunctionState {
	explicit PostgresScanGlobalState();

	std::mutex lock;
};

// Bind Data

struct PostgresScanFunctionData : public TableFunctionData {
public:
	PostgresScanFunctionData(PostgresRelation &&relation, Snapshot snapshot);
	~PostgresScanFunctionData() override;

public:
	PostgresRelation relation;
	Snapshot snapshot;
};

// ------- Table Function -------

struct PostgresScanFunction : public TableFunction {
public:
	PostgresScanFunction();

public:
	static unique_ptr<FunctionData> PostgresBind(ClientContext &context, TableFunctionBindInput &input,
	                                             vector<LogicalType> &return_types, vector<string> &names);
	static unique_ptr<GlobalTableFunctionState> PostgresInitGlobal(ClientContext &context,
	                                                               TableFunctionInitInput &input);
	static unique_ptr<LocalTableFunctionState>
	PostgresInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate);
	// static idx_t PostgresMaxThreads(ClientContext &context, const FunctionData *bind_data_p);
	// static bool PostgresParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	// LocalTableFunctionState *lstate, GlobalTableFunctionState *gstate); static double PostgresProgress(ClientContext
	// &context, const FunctionData *bind_data_p, const GlobalTableFunctionState *gstate);
	static void PostgresFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
	// static unique_ptr<NodeStatistics> PostgresCardinality(ClientContext &context, const FunctionData *bind_data);
	// static idx_t PostgresGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	// LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state); static void
	// PostgresSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const TableFunction
	// &function);
};

// ------- Replacement Scan -------

struct PostgresReplacementScanData : public ReplacementScanData {
public:
	PostgresReplacementScanData(QueryDesc *desc);
	~PostgresReplacementScanData() override;

public:
	QueryDesc *desc;
};

unique_ptr<TableRef> PostgresReplacementScan(ClientContext &context, const string &table_name,
                                             ReplacementScanData *data);

} // namespace duckdb
