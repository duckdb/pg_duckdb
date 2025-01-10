
#include "duckdb.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/exception.hpp"

#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "tcop/pquery.h"
#include "nodes/params.h"
#include "utils/ruleutils.h"
}

#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

/* global variables */
CustomScanMethods duckdb_scan_scan_methods;

/* static variables */
static CustomExecMethods duckdb_scan_exec_methods;

typedef struct DuckdbScanState {
	CustomScanState css; /* must be first field */
	const Query *query;
	ParamListInfo params;
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
	MemoryContextReset(state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
	ExecClearTuple(state->css.ss.ss_ScanTupleSlot);

	state->query_results.reset();
	state->current_data_chunk.reset();

	if (state->prepared_statement) {
		delete state->prepared_statement;
		state->prepared_statement = nullptr;
	}
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

	duckdb_scan_state->query = (const Query *)linitial(cscan->custom_private);
	custom_scan_state->methods = &duckdb_scan_exec_methods;
	return (Node *)custom_scan_state;
}

void
Duckdb_BeginCustomScan_Cpp(CustomScanState *cscanstate, EState *estate, int /*eflags*/) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)cscanstate;
	duckdb::unique_ptr<duckdb::PreparedStatement> prepared_query = DuckdbPrepare(duckdb_scan_state->query);

	if (prepared_query->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR,
		                        "DuckDB re-planning failed: " + prepared_query->GetError());
	}

	duckdb_scan_state->duckdb_connection = pgduckdb::DuckDBManager::GetConnection();
	duckdb_scan_state->prepared_statement = prepared_query.release();
	duckdb_scan_state->params = estate->es_param_list_info;
	duckdb_scan_state->is_executed = false;
	duckdb_scan_state->fetch_next = true;
	duckdb_scan_state->css.ss.ps.ps_ResultTupleDesc = duckdb_scan_state->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	HOLD_CANCEL_INTERRUPTS();
}

void
Duckdb_BeginCustomScan(CustomScanState *cscanstate, EState *estate, int eflags) {
	InvokeCPPFunc(Duckdb_BeginCustomScan_Cpp, cscanstate, estate, eflags);
}

static void
ExecuteQuery(DuckdbScanState *state) {
	auto &prepared = *state->prepared_statement;
	auto &query_results = state->query_results;
	auto &connection = state->duckdb_connection;
	auto pg_params = state->params;
	const auto num_params = pg_params ? pg_params->numParams : 0;
	duckdb::case_insensitive_map_t<duckdb::BoundParameterData> named_values;

	for (int i = 0; i < num_params; i++) {
		ParamExternData *pg_param;
		ParamExternData tmp_workspace;
		duckdb::Value duckdb_param;

		/* give hook a chance in case parameter is dynamic */
		if (pg_params->paramFetch != NULL) {
			pg_param = pg_params->paramFetch(pg_params, i + 1, false, &tmp_workspace);
		} else {
			pg_param = &pg_params->params[i];
		}

		if (prepared.named_param_map.count(duckdb::to_string(i + 1)) == 0) {
			continue;
		}

		if (pg_param->isnull) {
			duckdb_param = duckdb::Value();
		} else if (OidIsValid(pg_param->ptype)) {
			duckdb_param = pgduckdb::ConvertPostgresParameterToDuckValue(pg_param->value, pg_param->ptype);
		} else {
			std::ostringstream oss;
			oss << "parameter '" << i << "' has an invalid type (" << pg_param->ptype << ") during query execution";
			throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, oss.str().c_str());
		}
		named_values[duckdb::to_string(i + 1)] = duckdb::BoundParameterData(duckdb_param);
	}

	auto pending = prepared.PendingQuery(named_values, true);
	if (pending->HasError()) {
		return pending->ThrowError();
	}

	duckdb::PendingExecutionResult execution_result;
	while (true) {
		execution_result = pending->ExecuteTask();
		if (duckdb::PendingQueryResult::IsResultReady(execution_result)) {
			break;
		}

		if (QueryCancelPending) {
			// Send an interrupt
			connection->Interrupt();
			auto &executor = duckdb::Executor::Get(*connection->context);
			// Wait for all tasks to terminate
			executor.CancelTasks();
			// Delete the scan state
			// Process the interrupt on the Postgres side
			ProcessInterrupts();
			throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, "Query cancelled");
		}
	}

	if (execution_result == duckdb::PendingExecutionResult::EXECUTION_ERROR) {
		return pending->ThrowError();
	}

	query_results = pending->Execute();
	state->column_count = query_results->ColumnCount();
	state->is_executed = true;
}

static TupleTableSlot *
Duckdb_ExecCustomScan_Cpp(CustomScanState *node) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
	TupleTableSlot *slot = duckdb_scan_state->css.ss.ss_ScanTupleSlot;
	MemoryContext old_context;

	bool already_executed = duckdb_scan_state->is_executed;
	if (!already_executed) {
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
				throw duckdb::ConversionException("Value conversion failed");
			}
		}
	}

	MemoryContextSwitchTo(old_context);

	duckdb_scan_state->current_row++;
	if (duckdb_scan_state->current_row >= duckdb_scan_state->current_data_chunk->size()) {
		duckdb_scan_state->current_data_chunk.reset();
		duckdb_scan_state->fetch_next = true;
	}

	ExecStoreVirtualTuple(slot);
	return slot;
}

static TupleTableSlot *
Duckdb_ExecCustomScan(CustomScanState *node) {
	return InvokeCPPFunc(Duckdb_ExecCustomScan_Cpp, node);
}

void
Duckdb_EndCustomScan_Cpp(CustomScanState *node) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
	CleanupDuckdbScanState(duckdb_scan_state);
	RESUME_CANCEL_INTERRUPTS();
}

void
Duckdb_EndCustomScan(CustomScanState *node) {
	InvokeCPPFunc(Duckdb_EndCustomScan_Cpp, node);
}

void
Duckdb_ReScanCustomScan(CustomScanState * /*node*/) {
}

void
Duckdb_ExplainCustomScan_Cpp(CustomScanState *node, ExplainState *es) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
	ExecuteQuery(duckdb_scan_state);

	auto chunk = duckdb_scan_state->query_results->Fetch();
	if (!chunk || chunk->size() == 0) {
		return;
	}

	/* Is it safe to hardcode this as result of DuckDB explain? */
	auto value = chunk->GetValue(1, 0).GetValue<duckdb::string>();

	/* Fully consume the stream */
	do {
		chunk = duckdb_scan_state->query_results->Fetch();
	} while (chunk && chunk->size() > 0);

	std::ostringstream explain_output;
	explain_output << "\n\n" << value << "\n";
	ExplainPropertyText("DuckDB Execution Plan", explain_output.str().c_str(), es);
}

void
Duckdb_ExplainCustomScan(CustomScanState *node, List * /*ancestors*/, ExplainState *es) {
	InvokeCPPFunc(Duckdb_ExplainCustomScan_Cpp, node, es);
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
