#include "duckdb.hpp"

#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/pg/transactions.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"

#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "nodes/primnodes.h"
#include "tcop/utility.h"
#include "tcop/pquery.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_guc.h"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_ddl.hpp"
#include "pgduckdb/pgduckdb_table_am.hpp"
#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/vendor/pg_explain.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_executor_start_hook = NULL;
static ExecutorFinish_hook_type prev_executor_finish_hook = NULL;
static ExplainOneQuery_hook_type prev_explain_one_query_hook = NULL;

static bool
ContainsCatalogTable(List *rtes) {
	foreach_node(RangeTblEntry, rte, rtes) {
		if (rte->rtekind == RTE_SUBQUERY) {
			/* Check Subquery rtable list if any table is from PG catalog */
			if (ContainsCatalogTable(rte->subquery->rtable)) {
				return true;
			}
		}

		if (rte->relid) {
			auto rel = RelationIdGetRelation(rte->relid);
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

	if (IsA(node, Aggref)) {
		Aggref *func = castNode(Aggref, node);
		if (pgduckdb::IsDuckdbOnlyFunction(func->aggfnoid)) {
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

/*
 * We only check ContainsFromClause if duckdb.force_execution is set to true.
 *
 * If there's no FROM clause, we're only selecting constants. From a
 * performance perspective there's not really a point in using DuckDB. If we
 * forward all of such queries to DuckDB anyway, then many queries that are
 * used to inspect postgres will throw a warning or return incorrect results.
 * For example:
 *
 *    SELECT current_setting('work_mem');
 *
 * So even if a user sets duckdb.force_execution = true, we still won't
 * forward such queries to DuckDB. With one exception: If the query
 * requires duckdb e.g. due to a duckdb-only function being used, we'll
 * still executing this in DuckDB.
 */
static bool
ContainsFromClause(Query *query) {
	return query->rtable;
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
		if (pgduckdb::pg::IsInTransactionBlock(true)) {
			if (pgduckdb::pg::DidWalWrites()) {
				elog(elevel, "Writing to DuckDB and Postgres tables in the same transaction block is not supported");
				return false;
			}
		}
	}

	/*
	 * If any table is from pg_catalog, we don't want to use DuckDB. This is
	 * because DuckDB has its own pg_catalog tables that contain different data
	 * then Postgres its pg_catalog tables.
	 */
	if (ContainsCatalogTable(query->rtable)) {
		elog(elevel, "DuckDB does not support querying PG catalog tables");
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
		} else if (duckdb_force_execution && IsAllowedStatement(parse) && ContainsFromClause(parse)) {
			PlannedStmt *duckdbPlan = DuckdbPlanNode(parse, query_string, cursor_options, bound_params, false);
			if (duckdbPlan) {
				return duckdbPlan;
			}
			/* If we can't create a plan, we'll fall back to Postgres */
		}
		if (parse->commandType != CMD_SELECT && pgduckdb::ddb::DidWrites() &&
		    pgduckdb::pg::IsInTransactionBlock(true)) {
			elog(ERROR, "Writing to DuckDB and Postgres tables in the same transaction block is not supported");
		}
	}

	/*
	 * If we're executing a PG query, then if we'll execute a DuckDB
	 * later in the same transaction that means that DuckDB query was
	 * not executed at the top level, but internally by that PG query.
	 * A common case where this happens is a plpgsql function that
	 * executes a DuckDB query.
	 */

	pgduckdb::MarkStatementNotTopLevel();

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

bool
IsDuckdbPlan(PlannedStmt *stmt) {
	Plan *plan = stmt->planTree;
	if (!plan) {
		return false;
	}

	/* If the plan is a Material node, we need to extract the actual plan to
	 * see if it is our CustomScan node. A Matarial containing our CustomScan
	 * node gets created for backward scanning cursors. See our usage of.
	 * materialize_finished_plan. */
	if (IsA(plan, Material)) {
		Material *material = castNode(Material, plan);
		plan = material->plan.lefttree;
		if (!plan) {
			return false;
		}
	}

	if (!IsA(plan, CustomScan)) {
		return false;
	}

	CustomScan *custom_scan = castNode(CustomScan, plan);
	if (custom_scan->methods != &duckdb_scan_scan_methods) {
		return false;
	}

	return true;
}

/*
 * Claim the current command id for obvious DuckDB writes.
 *
 * If we're not in a transaction, this triggers the command to be executed
 * outside of any implicit transaction.
 *
 * This claims the command ID if we're doing a INSERT/UPDATE/DELETE on a DuckDB
 * table. This isn't strictly necessary for safety, as the ExecutorFinishHook
 * would catch it anyway, but this allows us to fail early, i.e. before doing
 * the potentially time-consuming write operation.
 */
static void
DuckdbExecutorStartHook_Cpp(QueryDesc *queryDesc) {
	if (!IsDuckdbPlan(queryDesc->plannedstmt)) {
		/*
		 * If we're executing a PG query, then if we'll execute a DuckDB
		 * later in the same transaction that means that DuckDB query was
		 * not executed at the top level, but internally by that PG query.
		 * A common case where this happens is a plpgsql function that
		 * executes a DuckDB query.
		 */

		pgduckdb::MarkStatementNotTopLevel();
		return;
	}

	pgduckdb::AutocommitSingleStatementQueries();

	if (queryDesc->operation == CMD_SELECT) {
		return;
	}
	pgduckdb::ClaimCurrentCommandId();
}

static void
DuckdbExecutorStartHook(QueryDesc *queryDesc, int eflags) {
	if (!pgduckdb::IsExtensionRegistered()) {
		pgduckdb::MarkStatementNotTopLevel();
		prev_executor_start_hook(queryDesc, eflags);
		return;
	}

	prev_executor_start_hook(queryDesc, eflags);
	InvokeCPPFunc(DuckdbExecutorStartHook_Cpp, queryDesc);
}

/*
 * Claim the current command id for non-obvious DuckDB writes.
 *
 * It's possible that a Postgres SELECT query writes to DuckDB, for example
 * when using one of our UDFs that that internally writes to DuckDB. This
 * function claims the command ID in those cases.
 */
static void
DuckdbExecutorFinishHook_Cpp(QueryDesc *queryDesc) {
	if (!IsDuckdbPlan(queryDesc->plannedstmt)) {
		return;
	}

	if (!pgduckdb::ddb::DidWrites()) {
		return;
	}

	pgduckdb::ClaimCurrentCommandId();
}

static void
DuckdbExecutorFinishHook(QueryDesc *queryDesc) {
	if (!pgduckdb::IsExtensionRegistered()) {
		prev_executor_finish_hook(queryDesc);
		return;
	}

	prev_executor_finish_hook(queryDesc);
	InvokeCPPFunc(DuckdbExecutorFinishHook_Cpp, queryDesc);
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

	prev_executor_start_hook = ExecutorStart_hook ? ExecutorStart_hook : standard_ExecutorStart;
	ExecutorStart_hook = DuckdbExecutorStartHook;

	prev_executor_finish_hook = ExecutorFinish_hook ? ExecutorFinish_hook : standard_ExecutorFinish;
	ExecutorFinish_hook = DuckdbExecutorFinishHook;

	prev_explain_one_query_hook = ExplainOneQuery_hook ? ExplainOneQuery_hook : standard_ExplainOneQuery;
	ExplainOneQuery_hook = DuckdbExplainOneQueryHook;

	DuckdbInitUtilityHook();
}
