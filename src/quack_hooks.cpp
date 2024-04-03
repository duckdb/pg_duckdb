#include "quack/quack.hpp"
#include "quack/quack_scan.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

extern "C" {

#include "postgres.h"

#include "miscadmin.h"

#include "access/tableam.h"
#include "executor/executor.h"
#include "parser/parse_type.h"
#include "tcop/utility.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"
#include "utils/builtins.h"

} // extern "C"

namespace duckdb {
static void QuackExecuteSelect(QueryDesc *query_desc, ScanDirection direction, uint64_t count);
} // namespace duckdb

extern "C" {

static ExecutorRun_hook_type PrevExecutorRunHook = NULL;
static ProcessUtility_hook_type PrevProcessUtilityHook = NULL;

static void
quack_executor_run(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once) {
	if (queryDesc->operation == CMD_SELECT) {
		duckdb::QuackExecuteSelect(queryDesc, direction, count);
		return;
	}

	if (PrevExecutorRunHook) {
		PrevExecutorRunHook(queryDesc, direction, count, execute_once);
	}
}

static void
quack_process_utility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree, ProcessUtilityContext context,
                      ParamListInfo params, struct QueryEnvironment *queryEnv, DestReceiver *dest,
                      QueryCompletion *completionTag) {

	Node *parsetree = pstmt->utilityStmt;

	if (IsA(parsetree, CreateStmt)) {
		CreateStmt *create_stmt = (CreateStmt *)parsetree;
		ListCell *lc;

		if (create_stmt->accessMethod && !memcmp(create_stmt->accessMethod, "quack", 5)) {
			StringInfo create_table_str = makeStringInfo();
			bool first = true;
			appendStringInfo(create_table_str, "CREATE TABLE %s (", create_stmt->relation->relname);
			foreach (lc, create_stmt->tableElts) {
				ColumnDef *def = (ColumnDef *)lfirst(lc);
				Oid pg_oid = LookupTypeNameOid(NULL, def->typeName, true);

				if (first) {
					first = false;
				} else {
					appendStringInfo(create_table_str, ", ");
				}

				appendStringInfo(create_table_str, "%s %s", def->colname, quack_duckdb_type(pg_oid));
			}
			appendStringInfo(create_table_str, ");");

			duckdb::quack_execute_query(create_table_str->data);
		}
	}

	PrevProcessUtilityHook(pstmt, queryString, false, context, params, queryEnv, dest, completionTag);
}

void
quack_init_hooks(void) {
	PrevExecutorRunHook = ExecutorRun_hook ? ExecutorRun_hook : standard_ExecutorRun;
	ExecutorRun_hook = quack_executor_run;

	PrevProcessUtilityHook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = quack_process_utility;
}
}

namespace duckdb {

static void
QuackExecuteSelect(QueryDesc *query_desc, ScanDirection direction, uint64_t count) {
	auto db = quack_open_database(MyDatabaseId, false);
	db->instance->config.replacement_scans.emplace_back(
	    PostgresReplacementScan, make_uniq_base<ReplacementScanData, PostgresReplacementScanData>(query_desc));
	auto connection = quack_open_connection(*db);

	// Add the postgres_scan inserted by the replacement scan
	auto &context = *connection->context;
	PostgresScanFunction scan_fun;
	CreateTableFunctionInfo scan_info(scan_fun);

	auto &catalog = Catalog::GetSystemCatalog(context);
	context.transaction.BeginTransaction();
	catalog.CreateTableFunction(context, &scan_info);
	context.transaction.Commit();

	idx_t column_count;

	CmdType operation;
	DestReceiver *dest;

	TupleTableSlot *slot = NULL;

	// FIXME: try-catch ?
	auto res = connection->Query(query_desc->sourceText);
	if (res->HasError()) {
	}

	operation = query_desc->operation;
	dest = query_desc->dest;

	dest->rStartup(dest, operation, query_desc->tupDesc);

	slot = MakeTupleTableSlot(query_desc->tupDesc, &TTSOpsHeapTuple);
	column_count = res->ColumnCount();

	while (true) {

		auto chunk = res->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		for (idx_t row = 0; row < chunk->size(); row++) {
			ExecClearTuple(slot);

			for (idx_t col = 0; col < column_count; col++) {
				auto value = chunk->GetValue(col, row);
				if (value.IsNull()) {
					slot->tts_isnull[col] = true;
				} else {
					slot->tts_isnull[col] = false;
					quack_translate_value(slot, value, col);
				}
			}
			ExecStoreVirtualTuple(slot);
			dest->receiveSlot(slot, dest);

			for (idx_t i = 0; i < column_count; i++) {
				if (slot->tts_tupleDescriptor->attrs[i].attbyval == false) {
					pfree(DatumGetPointer(slot->tts_values[i]));
				}
			}
		}
	}

	dest->rShutdown(dest);
	return;
}

} // namespace duckdb
