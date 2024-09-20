
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
	duckdb::Connection *duckdb_connection;
	duckdb::PreparedStatement *prepared_statement;
	bool is_executed;
	bool fetch_next;
	duckdb::unique_ptr<duckdb::QueryResult> query_results;
	duckdb::idx_t column_count;
	duckdb::unique_ptr<duckdb::DataChunk> current_data_chunk;
	duckdb::idx_t current_row;
} DuckdbScanState;

static void
CleanupDuckdbScanState(DuckdbScanState *state) {
	state->query_results.reset();
	delete state->prepared_statement;
	delete state->duckdb_connection;
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
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)newNode(sizeof(DuckdbScanState), T_CustomScanState);
	CustomScanState *custom_scan_state = &duckdb_scan_state->css;
	duckdb_scan_state->duckdb_connection = (duckdb::Connection *)linitial(cscan->custom_private);
	duckdb_scan_state->prepared_statement = (duckdb::PreparedStatement *)lsecond(cscan->custom_private);
	duckdb_scan_state->is_executed = false;
	duckdb_scan_state->fetch_next = true;
	custom_scan_state->methods = &duckdb_scan_exec_methods;
	return (Node *)custom_scan_state;
}

void
Duckdb_BeginCustomScan(CustomScanState *cscanstate, EState *estate, int eflags) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)cscanstate;
	duckdb_scan_state->css.ss.ps.ps_ResultTupleDesc = duckdb_scan_state->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	HOLD_CANCEL_INTERRUPTS();
}

static void
ExecuteQuery(DuckdbScanState *state) {
	auto &prepared = *state->prepared_statement;
	auto &query_results = state->query_results;
	auto &connection = state->duckdb_connection;

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
	} while (!duckdb::PendingQueryResult::IsResultReady(execution_result));
	if (execution_result == duckdb::PendingExecutionResult::EXECUTION_ERROR) {
		CleanupDuckdbScanState(state);
		elog(ERROR, "(PGDuckDB/ExecuteQuery) %s", pending->GetError().c_str());
	}
	query_results = pending->Execute();
	state->column_count = query_results->ColumnCount();
	state->is_executed = true;
}

static TupleTableSlot *
Duckdb_ExecCustomScan(CustomScanState *node) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
	TupleTableSlot *slot = duckdb_scan_state->css.ss.ss_ScanTupleSlot;
	MemoryContext old_context;

	if (!duckdb_scan_state->is_executed) {
		ExecuteQuery(duckdb_scan_state);
	}

	if (duckdb_scan_state->fetch_next) {
		duckdb_scan_state->current_data_chunk = duckdb_scan_state->query_results->Fetch();
		duckdb_scan_state->current_row = 0;
		duckdb_scan_state->fetch_next = false;
		if (!duckdb_scan_state->current_data_chunk || duckdb_scan_state->current_data_chunk->size() == 0) {
			MemoryContextReset(duckdb_scan_state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
			ExecClearTuple(slot);
			return slot;
		}
	}

	MemoryContextReset(duckdb_scan_state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
	ExecClearTuple(slot);

	/* MemoryContext used for allocation */
	old_context = MemoryContextSwitchTo(duckdb_scan_state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	for (idx_t col = 0; col < duckdb_scan_state->column_count; col++) {
		// FIXME: we should not use the Value API here, it's complicating the LIST conversion logic
		auto value = duckdb_scan_state->current_data_chunk->GetValue(col, duckdb_scan_state->current_row);
		if (value.IsNull()) {
			slot->tts_isnull[col] = true;
		} else {
			slot->tts_isnull[col] = false;
			if (!pgduckdb::ConvertDuckToPostgresValue(slot, value, col)) {
				CleanupDuckdbScanState(duckdb_scan_state);
				elog(ERROR, "(PGDuckDB/Duckdb_ExecCustomScan) Value conversion failed");
			}
		}
	}

	MemoryContextSwitchTo(old_context);

	duckdb_scan_state->current_row++;
	if (duckdb_scan_state->current_row >= duckdb_scan_state->current_data_chunk->size()) {
		delete duckdb_scan_state->current_data_chunk.release();
		duckdb_scan_state->fetch_next = true;
	}

	ExecStoreVirtualTuple(slot);
	return slot;
}

void
Duckdb_EndCustomScan(CustomScanState *node) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
	CleanupDuckdbScanState(duckdb_scan_state);
	RESUME_CANCEL_INTERRUPTS();
}

void
Duckdb_ReScanCustomScan(CustomScanState *node) {
}

void
Duckdb_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
	auto res = duckdb_scan_state->prepared_statement->Execute();
	std::string explain_output = "\n\n";
	auto chunk = res->Fetch();
	if (!chunk || chunk->size() == 0) {
		return;
	}
	/* Is it safe to hardcode this as result of DuckDB explain? */
	auto value = chunk->GetValue(1, 0);
	explain_output += value.GetValue<duckdb::string>();
	explain_output += "\n";
	ExplainPropertyText("DuckDB Execution Plan", explain_output.c_str(), es);
}

extern "C" void
DuckdbInitNode() {
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
