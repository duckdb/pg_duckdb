#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/genam.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/scan/postgres_scan.hpp"

namespace pgduckdb {

// Global State

struct PostgresIndexScanGlobalState : public duckdb::GlobalTableFunctionState {
	explicit PostgresIndexScanGlobalState(IndexOptInfo *index, IndexScanState *index_scan_state, bool indexonly,
	                                      Relation relation, duckdb::TableFunctionInitInput &input);
	~PostgresIndexScanGlobalState();
	idx_t
	MaxThreads() const override {
		return 1;
	}

public:
	duckdb::shared_ptr<PostgresScanGlobalState> m_global_state;
	IndexOptInfo *m_index;
	IndexScanState *m_index_scan;
	bool m_indexonly;
	Relation m_relation;
};

// Local State

struct PostgresIndexScanLocalState : public duckdb::LocalTableFunctionState {
public:
	PostgresIndexScanLocalState(IndexScanDesc index_scan_desc, TupleDesc desc, Relation relation);
	~PostgresIndexScanLocalState() override;

public:
	duckdb::shared_ptr<PostgresScanLocalState> m_local_state;
	IndexScanDesc m_index_scan_desc;
	Relation m_relation;
	TupleTableSlot *m_slot;
	TupleTableSlot *m_index_only_slot;
};

// PostgresIndexScanFunctionData

struct PostgresIndexScanFunctionData : public duckdb::TableFunctionData {
public:
	PostgresIndexScanFunctionData(uint64_t cardinality, bool indexonly, Path *path, PlannerInfo *planner_info,
	                              Oid relation_oid, Snapshot Snapshot);
	~PostgresIndexScanFunctionData() override;

public:
	uint64_t m_cardinality;
	bool m_indexonly;
	Path *m_path;
	PlannerInfo *m_planner_info;
	Snapshot m_snapshot;
	Oid m_relation_oid;
};

// PostgresIndexScanFunction

struct PostgresIndexScanFunction : public duckdb::TableFunction {
public:
	PostgresIndexScanFunction();

public:
	static duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
	PostgresIndexScanInitGlobal(duckdb::ClientContext &context, duckdb::TableFunctionInitInput &input);

	static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
	PostgresIndexScanInitLocal(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
	                           duckdb::GlobalTableFunctionState *gstate);

	// static idx_t PostgresMaxThreads(ClientContext &context, const FunctionData *bind_data_p);
	// static bool PostgresParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	// LocalTableFunctionState *lstate, GlobalTableFunctionState *gstate); static double PostgresProgress(ClientContext
	// &context, const FunctionData *bind_data_p, const GlobalTableFunctionState *gstate);

	static void PostgresIndexScanFunc(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
	                                  duckdb::DataChunk &output);

	static duckdb::unique_ptr<duckdb::NodeStatistics> PostgresIndexScanCardinality(duckdb::ClientContext &context,
	                                                                               const duckdb::FunctionData *data);

	// static idx_t PostgresGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	// LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state); static void
	// PostgresSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const TableFunction
	// &function);
};

} // namespace pgduckdb
