#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "tcop/utility.h"
#include "tcop/pquery.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "optimizer/optimizer.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_guc.h"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_ddl.hpp"
#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/pgduckdb_table_am.hpp"
#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/vendor/pg_explain.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

static planner_hook_type prev_planner_hook = NULL;
static ProcessUtility_hook_type prev_process_utility_hook = NULL;
static ExplainOneQuery_hook_type prev_explain_one_query_hook = NULL;

static bool
IsCatalogTable(List *tables) {
	foreach_node(RangeTblEntry, table, tables) {
		if (table->rtekind == RTE_SUBQUERY) {
			/* Check Subquery rtable list if any table is from PG catalog */
			if (IsCatalogTable(table->subquery->rtable)) {
				return true;
			}
		}

		if (table->relid) {
			auto rel = RelationIdGetRelation(table->relid);
			auto namespace_oid = RelationGetNamespace(rel);
			RelationClose(rel);
			if (namespace_oid == PG_CATALOG_NAMESPACE || namespace_oid == PG_TOAST_NAMESPACE) {
				return true;
			}
		}
	}
	return false;
}

static bool
IsDuckdbTable(Oid relid) {
	if (relid == InvalidOid) {
		return false;
	}

	auto rel = RelationIdGetRelation(relid);
	bool result = pgduckdb::IsDuckdbTableAm(rel->rd_tableam);
	RelationClose(rel);
	return result;
}

static bool
ContainsDuckdbTables(List *rte_list) {
	foreach_node(RangeTblEntry, rte, rte_list) {
		if (IsDuckdbTable(rte->relid)) {
			return true;
		}
	}
	return false;
}

static bool
ContainsDuckdbItems(Node *node, void *context) {
	if (node == NULL)
		return false;

	if (IsA(node, Query)) {
		Query *query = (Query *)node;
		if (ContainsDuckdbTables(query->rtable)) {
			return true;
		}
#if PG_VERSION_NUM >= 160000
		return query_tree_walker(query, ContainsDuckdbItems, context, 0);
#else
		return query_tree_walker(query, (bool (*)())((void *)ContainsDuckdbItems), context, 0);
#endif
	}

	if (IsA(node, FuncExpr)) {
		FuncExpr *func = castNode(FuncExpr, node);
		if (pgduckdb::IsDuckdbOnlyFunction(func->funcid)) {
			return true;
		}
	}

#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, ContainsDuckdbItems, context);
#else
	return expression_tree_walker(node, (bool (*)())((void *)ContainsDuckdbItems), context);
#endif
}

static bool
NeedsDuckdbExecution(Query *query) {
	return ContainsDuckdbItems((Node *)query, NULL);
}

static bool
IsAllowedStatement(Query *query, bool throw_error = false) {
	int elevel = throw_error ? ERROR : DEBUG4;
	/* DuckDB does not support modifying CTEs INSERT/UPDATE/DELETE */
	if (query->hasModifyingCTE) {
		elog(elevel, "DuckDB does not support modifying CTEs");
		return false;
	}

	/* We don't support modifying statements on Postgres tables yet */
	if (query->commandType != CMD_SELECT) {
		RangeTblEntry *resultRte = list_nth_node(RangeTblEntry, query->rtable, query->resultRelation - 1);
		if (!IsDuckdbTable(resultRte->relid)) {
			elog(elevel, "DuckDB does not support modififying Postgres tables");
			return false;
		}
	}

	/*
	 * If there's no rtable, we're only selecting constants. There's no point
	 * in using DuckDB for that.
	 */
	if (!query->rtable) {
		elog(elevel, "DuckDB usage requires at least one table");
		return false;
	}

	/*
	 * If any table is from pg_catalog, we don't want to use DuckDB. This is
	 * because DuckDB has its own pg_catalog tables that contain different data
	 * then Postgres its pg_catalog tables.
	 */
	if (IsCatalogTable(query->rtable)) {
		elog(elevel, "DuckDB does not support querying PG catalog tables");
		return false;
	}

	/*
	 * We don't support multi-statement transactions yet, so don't try to
	 * execute queries in them even if duckdb.force_execution is enabled.
	 */
	if (IsInTransactionBlock(true)) {
		if (throw_error) {
			/*
			 * We don't elog manually here, because PreventInTransactionBlock
			 * provides very detailed errors.
			 */
			PreventInTransactionBlock(true, "DuckDB queries");
		}
		return false;
	}

	/* Anything else is hopefully fine... */
	return true;
}

static PlannedStmt *
DuckdbPlannerHook_Cpp(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params) {
	if (pgduckdb::IsExtensionRegistered()) {
		if (NeedsDuckdbExecution(parse)) {
			IsAllowedStatement(parse, true);

			return DuckdbPlanNode(parse, query_string, cursor_options, bound_params, true);
		} else if (duckdb_force_execution && IsAllowedStatement(parse)) {
			PlannedStmt *duckdbPlan = DuckdbPlanNode(parse, query_string, cursor_options, bound_params, false);
			if (duckdbPlan) {
				return duckdbPlan;
			}
			/* If we can't create a plan, we'll fall back to Postgres */
		}
	}

	if (prev_planner_hook) {
		return prev_planner_hook(parse, query_string, cursor_options, bound_params);
	} else {
		return standard_planner(parse, query_string, cursor_options, bound_params);
	}
}

static PlannedStmt *
DuckdbPlannerHook(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params) {
	return InvokeCPPFunc(DuckdbPlannerHook_Cpp, parse, query_string, cursor_options, bound_params);
}

static void
DuckdbUtilityHook_Cpp(PlannedStmt *pstmt, const char *query_string, bool read_only_tree, ProcessUtilityContext context,
                      ParamListInfo params, struct QueryEnvironment *query_env, DestReceiver *dest,
                      QueryCompletion *qc) {
	Node *parsetree = pstmt->utilityStmt;
	if (pgduckdb::IsExtensionRegistered() && IsA(parsetree, CopyStmt)) {
		auto copy_query = PostgresFunctionGuard(MakeDuckdbCopyQuery, pstmt, query_string, query_env);
		if (copy_query) {
			auto res = pgduckdb::DuckDBQueryOrThrow(copy_query);
			auto chunk = res->Fetch();
			auto processed = chunk->GetValue(0, 0).GetValue<uint64_t>();
			if (qc) {
				SetQueryCompletion(qc, CMDTAG_COPY, processed);
			}
			return;
		}
	}

	if (pgduckdb::IsExtensionRegistered()) {
		DuckdbHandleDDL(parsetree, query_string);
	}

	if (prev_process_utility_hook) {
		(*prev_process_utility_hook)(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
	} else {
		standard_ProcessUtility(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
	}
}

static void
DuckdbUtilityHook(PlannedStmt *pstmt, const char *query_string, bool read_only_tree, ProcessUtilityContext context,
                  ParamListInfo params, struct QueryEnvironment *query_env, DestReceiver *dest, QueryCompletion *qc) {
	InvokeCPPFunc(DuckdbUtilityHook_Cpp, pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
}

extern "C" {
#include "nodes/print.h"
}
void
DuckdbExplainOneQueryHook(Query *query, int cursorOptions, IntoClause *into, ExplainState *es, const char *queryString,
                          ParamListInfo params, QueryEnvironment *queryEnv) {
	/*
	 * It might seem sensible to store this data in the custom_private
	 * field of the CustomScan node, but that's not a trivial change to make.
	 * Storing this in a global variable works fine, as long as we only use
	 * this variable during planning when we're actually executing an explain
	 * QUERY (this can be checked by checking the commandTag of the
	 * ActivePortal). This even works when plans would normally be cached,
	 * because EXPLAIN always execute this hook whenever they are executed.
	 * EXPLAIN queries are also always re-planned (see
	 * standard_ExplainOneQuery).
	 */
	duckdb_explain_analyze = es->analyze;
	prev_explain_one_query_hook(query, cursorOptions, into, es, queryString, params, queryEnv);
}

void
DuckdbInitHooks(void) {
	prev_planner_hook = planner_hook;
	planner_hook = DuckdbPlannerHook;

	prev_process_utility_hook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = DuckdbUtilityHook;

	prev_explain_one_query_hook = ExplainOneQuery_hook ? ExplainOneQuery_hook : standard_ExplainOneQuery;
	ExplainOneQuery_hook = DuckdbExplainOneQueryHook;
}
