
#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
}

#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

/* global variables */
CustomScanMethods duckdb_scan_scan_methods;

/* static variables */
static CustomExecMethods duckdb_scan_exec_methods;

typedef struct DuckdbScanState {
	CustomScanState css; /* must be first field */
	duckdb::Connection *duckdbConnection;
	duckdb::PreparedStatement *preparedStatement;
	bool is_executed;
	bool fetch_next;
	duckdb::unique_ptr<duckdb::QueryResult> queryResult;
	duckdb::idx_t columnCount;
	duckdb::unique_ptr<duckdb::DataChunk> currentDataChunk;
	duckdb::idx_t currentRow;
} DuckdbScanState;

static void
CleanupDuckdbScanState(DuckdbScanState *state) {
	state->queryResult.reset();
	delete state->preparedStatement;
	delete state->duckdbConnection;
}

/* static callbacks */
static Node *Duckdb_CreateCustomScanState(CustomScan *cscan);
static void Duckdb_BeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *Duckdb_ExecCustomScan(CustomScanState *node);
static void Duckdb_EndCustomScan(CustomScanState *node);
static void Duckdb_ReScanCustomScan(CustomScanState *node);
static void Duckdb_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es);

static Node *
Duckdb_CreateCustomScanState(CustomScan *cscan) {
	DuckdbScanState *duckdbScanState = (DuckdbScanState *)newNode(sizeof(DuckdbScanState), T_CustomScanState);
	CustomScanState *customScanState = &duckdbScanState->css;
	duckdbScanState->duckdbConnection = (duckdb::Connection *)linitial(cscan->custom_private);
	duckdbScanState->preparedStatement = (duckdb::PreparedStatement *)lsecond(cscan->custom_private);
	duckdbScanState->is_executed = false;
	duckdbScanState->fetch_next = true;
	customScanState->methods = &duckdb_scan_exec_methods;
	return (Node *)customScanState;
}

void
Duckdb_BeginCustomScan(CustomScanState *cscanstate, EState *estate, int eflags) {
	DuckdbScanState *duckdbScanState = (DuckdbScanState *)cscanstate;
	duckdbScanState->css.ss.ps.ps_ResultTupleDesc = duckdbScanState->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	HOLD_CANCEL_INTERRUPTS();
}

static void
ExecuteQuery(DuckdbScanState *state) {
	auto &prepared = *state->preparedStatement;
	auto &query_result = state->queryResult;
	auto &connection = state->duckdbConnection;

	auto pending = prepared.PendingQuery();
	duckdb::PendingExecutionResult execution_result;
	do {
		execution_result = pending->ExecuteTask();
		if (QueryCancelPending) {
			// Send an interrupt
			connection->Interrupt();
			auto &executor = duckdb::Executor::Get(*connection->context);
			// Wait for all tasks to terminate
			executor.CancelTasks();

			// Delete the scan state
			CleanupDuckdbScanState(state);
			// Process the interrupt on the Postgres side
			ProcessInterrupts();
			elog(ERROR, "Query cancelled");
		}
	} while (!duckdb::PendingQueryResult::IsFinishedOrBlocked(execution_result));
	if (execution_result == duckdb::PendingExecutionResult::EXECUTION_ERROR) {
		elog(ERROR, "Duckdb execute returned an error: %s", pending->GetError().c_str());
	}
	query_result = pending->Execute();
	state->columnCount = query_result->ColumnCount();
	state->is_executed = true;
}

static TupleTableSlot *
Duckdb_ExecCustomScan(CustomScanState *node) {
	DuckdbScanState *duckdbScanState = (DuckdbScanState *)node;
	TupleTableSlot *slot = duckdbScanState->css.ss.ss_ScanTupleSlot;
	MemoryContext oldContext;

	if (!duckdbScanState->is_executed) {
		ExecuteQuery(duckdbScanState);
	}

	if (duckdbScanState->fetch_next) {
		duckdbScanState->currentDataChunk = duckdbScanState->queryResult->Fetch();
		duckdbScanState->currentRow = 0;
		duckdbScanState->fetch_next = false;
		if (!duckdbScanState->currentDataChunk || duckdbScanState->currentDataChunk->size() == 0) {
			MemoryContextReset(duckdbScanState->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
			ExecClearTuple(slot);
			return slot;
		}
	}

	MemoryContextReset(duckdbScanState->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
	ExecClearTuple(slot);

	/* MemoryContext used for allocation */
	oldContext = MemoryContextSwitchTo(duckdbScanState->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	for (idx_t col = 0; col < duckdbScanState->columnCount; col++) {
		// FIXME: we should not use the Value API here, it's complicating the LIST conversion logic
		auto value = duckdbScanState->currentDataChunk->GetValue(col, duckdbScanState->currentRow);
		if (value.IsNull()) {
			slot->tts_isnull[col] = true;
		} else {
			slot->tts_isnull[col] = false;
			pgduckdb::ConvertDuckToPostgresValue(slot, value, col);
		}
	}

	MemoryContextSwitchTo(oldContext);

	duckdbScanState->currentRow++;
	if (duckdbScanState->currentRow >= duckdbScanState->currentDataChunk->size()) {
		delete duckdbScanState->currentDataChunk.release();
		duckdbScanState->fetch_next = true;
	}

	ExecStoreVirtualTuple(slot);
	return slot;
}

void
Duckdb_EndCustomScan(CustomScanState *node) {
	DuckdbScanState *duckdbScanState = (DuckdbScanState *)node;
	CleanupDuckdbScanState(duckdbScanState);
	RESUME_CANCEL_INTERRUPTS();
}

void
Duckdb_ReScanCustomScan(CustomScanState *node) {
}

void
Duckdb_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es) {
	DuckdbScanState *duckdbScanState = (DuckdbScanState *)node;
	auto res = duckdbScanState->preparedStatement->Execute();
	std::string explainOutput = "\n\n";
	auto chunk = res->Fetch();
	if (!chunk || chunk->size() == 0) {
		return;
	}
	/* Is it safe to hardcode this as result of DuckDB explain? */
	auto value = chunk->GetValue(1, 0);
	explainOutput += value.GetValue<duckdb::string>();
	explainOutput += "\n";
	ExplainPropertyText("DuckDB Execution Plan", explainOutput.c_str(), es);
}

extern "C" void
duckdb_init_node() {
	/* setup scan methods */
	memset(&duckdb_scan_scan_methods, 0, sizeof(duckdb_scan_scan_methods));
	duckdb_scan_scan_methods.CustomName = "DuckDBScan";
	duckdb_scan_scan_methods.CreateCustomScanState = Duckdb_CreateCustomScanState;
	RegisterCustomScanMethods(&duckdb_scan_scan_methods);

	/* setup exec methods */
	memset(&duckdb_scan_exec_methods, 0, sizeof(duckdb_scan_exec_methods));
	duckdb_scan_exec_methods.CustomName = "DuckDBScan";

	duckdb_scan_exec_methods.BeginCustomScan = Duckdb_BeginCustomScan;
	duckdb_scan_exec_methods.ExecCustomScan = Duckdb_ExecCustomScan;
	duckdb_scan_exec_methods.EndCustomScan = Duckdb_EndCustomScan;
	duckdb_scan_exec_methods.ReScanCustomScan = Duckdb_ReScanCustomScan;

	duckdb_scan_exec_methods.EstimateDSMCustomScan = NULL;
	duckdb_scan_exec_methods.InitializeDSMCustomScan = NULL;
	duckdb_scan_exec_methods.ReInitializeDSMCustomScan = NULL;
	duckdb_scan_exec_methods.InitializeWorkerCustomScan = NULL;
	duckdb_scan_exec_methods.ShutdownCustomScan = NULL;

	duckdb_scan_exec_methods.ExplainCustomScan = Duckdb_ExplainCustomScan;
}