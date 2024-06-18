
#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
}

#include "quack/quack_node.hpp"
#include "quack/quack_types.hpp"
#include "quack/quack_error.hpp"

#include <mutex>

/* global variables */
CustomScanMethods quack_scan_scan_methods;

/* static variables */
static CustomExecMethods quack_scan_exec_methods;

typedef struct QuackScanState {
	CustomScanState css; /* must be first field */
	duckdb::Connection *duckdbConnection;
	duckdb::PreparedStatement *preparedStatement;
	std::mutex execution_lock;
	bool is_executed;
	bool fetch_next;
	duckdb::unique_ptr<duckdb::QueryResult> queryResult;
	duckdb::idx_t columnCount;
	duckdb::unique_ptr<duckdb::DataChunk> currentDataChunk;
	duckdb::idx_t currentRow;
} QuackScanState;

/* static callbacks */
static Node *Quack_CreateCustomScanState(CustomScan *cscan);
static void Quack_BeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *Quack_ExecCustomScan(CustomScanState *node);
static void Quack_EndCustomScan(CustomScanState *node);
static void Quack_ReScanCustomScan(CustomScanState *node);
static void Quack_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es);

static Node *
Quack_CreateCustomScanState(CustomScan *cscan) {
	QuackScanState *quackScanState = (QuackScanState *)newNode(sizeof(QuackScanState), T_CustomScanState);
	CustomScanState *customScanState = &quackScanState->css;
	quackScanState->duckdbConnection = (duckdb::Connection *)linitial(cscan->custom_private);
	quackScanState->preparedStatement = (duckdb::PreparedStatement *)lsecond(cscan->custom_private);
	customScanState->methods = &quack_scan_exec_methods;
	quackScanState->is_executed = false;
	quackScanState->fetch_next = true;
	std::allocator<std::mutex> allocator;
	allocator.construct(&quackScanState->execution_lock);
	return (Node *)customScanState;
}

void
Quack_BeginCustomScan(CustomScanState *cscanstate, EState *estate, int eflags) {
	QuackScanState *quackScanState = (QuackScanState *)cscanstate;
	quackScanState->css.ss.ps.ps_ResultTupleDesc = quackScanState->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	HOLD_CANCEL_INTERRUPTS();
}

static TupleTableSlot *
Quack_ExecCustomScan(CustomScanState *node) {
	QuackScanState *quackScanState = (QuackScanState *)node;
	TupleTableSlot *slot = quackScanState->css.ss.ss_ScanTupleSlot;
	MemoryContext oldContext;

	{
		std::lock_guard<std::mutex> l(quackScanState->execution_lock);
		if (!quackScanState->is_executed) {
			quackScanState->queryResult = quackScanState->preparedStatement->Execute();
			quackScanState->columnCount = quackScanState->queryResult->ColumnCount();
			quackScanState->is_executed = true;
		}
	}

	if (quackScanState->queryResult->HasError()) {
		elog_quack(ERROR, "Quack execute returned an error: %s", quackScanState->queryResult->GetError().c_str());
	}

	if (quackScanState->fetch_next) {
		quackScanState->currentDataChunk = quackScanState->queryResult->Fetch();
		quackScanState->currentRow = 0;
		quackScanState->fetch_next = false;
		if (!quackScanState->currentDataChunk || quackScanState->currentDataChunk->size() == 0) {
			MemoryContextReset(quackScanState->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
			ExecClearTuple(slot);
			return slot;
		}
	}

	MemoryContextReset(quackScanState->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
	ExecClearTuple(slot);

	/* MemoryContext used for allocation */
	oldContext = MemoryContextSwitchTo(quackScanState->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	for (idx_t col = 0; col < quackScanState->columnCount; col++) {
		// FIXME: we should not use the Value API here, it's complicating the LIST conversion logic
		auto value = quackScanState->currentDataChunk->GetValue(col, quackScanState->currentRow);
		if (value.IsNull()) {
			slot->tts_isnull[col] = true;
		} else {
			slot->tts_isnull[col] = false;
			quack::ConvertDuckToPostgresValue(slot, value, col);
		}
	}

	MemoryContextSwitchTo(oldContext);

	quackScanState->currentRow++;
	if (quackScanState->currentRow >= quackScanState->currentDataChunk->size()) {
		delete quackScanState->currentDataChunk.release();
		quackScanState->fetch_next = true;
	}

	ExecStoreVirtualTuple(slot);
	return slot;
}

void
Quack_EndCustomScan(CustomScanState *node) {
	QuackScanState *quackScanState = (QuackScanState *)node;
	quackScanState->queryResult.reset();
	delete quackScanState->preparedStatement;
	delete quackScanState->duckdbConnection;
	RESUME_CANCEL_INTERRUPTS();
}

void
Quack_ReScanCustomScan(CustomScanState *node) {
}

void
Quack_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es) {
	QuackScanState *quackScanState = (QuackScanState *)node;
	auto res = quackScanState->preparedStatement->Execute();
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
quack_init_node() {
	/* setup scan methods */
	memset(&quack_scan_scan_methods, 0, sizeof(quack_scan_scan_methods));
	quack_scan_scan_methods.CustomName = "DuckDBScan";
	quack_scan_scan_methods.CreateCustomScanState = Quack_CreateCustomScanState;
	RegisterCustomScanMethods(&quack_scan_scan_methods);

	/* setup exec methods */
	memset(&quack_scan_exec_methods, 0, sizeof(quack_scan_exec_methods));
	quack_scan_exec_methods.CustomName = "DuckDBScan";

	quack_scan_exec_methods.BeginCustomScan = Quack_BeginCustomScan;
	quack_scan_exec_methods.ExecCustomScan = Quack_ExecCustomScan;
	quack_scan_exec_methods.EndCustomScan = Quack_EndCustomScan;
	quack_scan_exec_methods.ReScanCustomScan = Quack_ReScanCustomScan;

	quack_scan_exec_methods.EstimateDSMCustomScan = NULL;
	quack_scan_exec_methods.InitializeDSMCustomScan = NULL;
	quack_scan_exec_methods.ReInitializeDSMCustomScan = NULL;
	quack_scan_exec_methods.InitializeWorkerCustomScan = NULL;
	quack_scan_exec_methods.ShutdownCustomScan = NULL;

	quack_scan_exec_methods.ExplainCustomScan = Quack_ExplainCustomScan;
}