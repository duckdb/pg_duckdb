#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "executor/executor.h"
#include "access/relscan.h"
}

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

namespace quack {

// Local State

struct PostgresScanLocalState : public duckdb::LocalTableFunctionState {
public:
	PostgresScanLocalState(PostgresRelation &relation, Snapshot snapshot);
	~PostgresScanLocalState() override;

public:
	TableScanDesc scanDesc = nullptr;
	const struct TableAmRoutine *tableam;
	bool exhausted_scan = false;
};

// Global State

struct PostgresScanGlobalState : public duckdb::GlobalTableFunctionState {
	explicit PostgresScanGlobalState();
	std::mutex lock;
};

// Bind Data

struct PostgresScanFunctionData : public duckdb::TableFunctionData {
public:
	PostgresScanFunctionData(PostgresRelation &&relation, Snapshot snapshot);
	~PostgresScanFunctionData() override;

public:
	PostgresRelation relation;
	Snapshot snapshot;
};

// ------- Table Function -------

struct PostgresScanFunction : public duckdb::TableFunction {
public:
	PostgresScanFunction();

public:
	static duckdb::unique_ptr<duckdb::FunctionData> PostgresBind(duckdb::ClientContext &context,
	                                                             duckdb::TableFunctionBindInput &input,
	                                                             duckdb::vector<duckdb::LogicalType> &return_types,
	                                                             duckdb::vector<duckdb::string> &names);
	static duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
	PostgresInitGlobal(duckdb::ClientContext &context, duckdb::TableFunctionInitInput &input);
	static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
	PostgresInitLocal(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
	                  duckdb::GlobalTableFunctionState *gstate);
	// static idx_t PostgresMaxThreads(ClientContext &context, const FunctionData *bind_data_p);
	// static bool PostgresParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	// LocalTableFunctionState *lstate, GlobalTableFunctionState *gstate); static double PostgresProgress(ClientContext
	// &context, const FunctionData *bind_data_p, const GlobalTableFunctionState *gstate);
	static void PostgresFunc(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
	                         duckdb::DataChunk &output);
	// static unique_ptr<NodeStatistics> PostgresCardinality(ClientContext &context, const FunctionData *bind_data);
	// static idx_t PostgresGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	// LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state); static void
	// PostgresSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const TableFunction
	// &function);
};

struct PostgresReplacementScanData : public duckdb::ReplacementScanData {
public:
	PostgresReplacementScanData(QueryDesc *desc);
	~PostgresReplacementScanData() override;

public:
	QueryDesc *desc;
};

duckdb::unique_ptr<duckdb::TableRef> PostgresReplacementScan(duckdb::ClientContext &context,
                                                             const duckdb::string &table_name,
                                                             duckdb::ReplacementScanData *data);

} // namespace quack
