#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "tcop/utility.h"
#include "tcop/pquery.h"
#include "utils/rel.h"
#include "optimizer/optimizer.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/vendor/pg_explain.hpp"
#include "pgduckdb/vendor/pg_list.hpp"

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
			if (namespace_oid == PG_CATALOG_NAMESPACE || namespace_oid == PG_TOAST_NAMESPACE) {
				RelationClose(rel);
				return true;
			}
			RelationClose(rel);
		}
	}
	return false;
}

static bool
ContainsDuckdbFunctions(Node *node, void *context) {
	if (node == NULL)
		return false;

	if (IsA(node, Query)) {
		Query *query = (Query *)node;
		return query_tree_walker(query, ContainsDuckdbFunctions, context, 0);
	}

	if (IsA(node, FuncExpr)) {
		FuncExpr *func = castNode(FuncExpr, node);
		if (pgduckdb::IsDuckdbOnlyFunction(func->funcid)) {
			return true;
		}
	}

	return expression_tree_walker(node, ContainsDuckdbFunctions, context);
}

static bool
NeedsDuckdbExecution(Query *query) {
	return query_tree_walker(query, ContainsDuckdbFunctions, NULL, 0);
}

static bool
IsAllowedStatement(Query *query) {
	/* DuckDB does not support modifying CTEs INSERT/UPDATE/DELETE */
	if (query->hasModifyingCTE) {
		return false;
	}

	/* We don't support modifying statements yet */
	if (query->commandType != CMD_SELECT) {
		return false;
	}

	/*
	 * If there's no rtable, we're only selecting constants. There's no point
	 * in using DuckDB for that.
	 */
	if (!query->rtable) {
		return false;
	}

	/*
	 * If any table is from pg_catalog, we don't want to use DuckDB. This is
	 * because DuckDB has its own pg_catalog tables that contain different data
	 * then Postgres its pg_catalog tables.
	 */
	if (IsCatalogTable(query->rtable)) {
		return false;
	}

	/* Anything else is hopefully fine... */
	return true;
}

static PlannedStmt *
DuckdbPlannerHook(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params) {
	if (pgduckdb::IsExtensionRegistered()) {
		if (duckdb_execution && IsAllowedStatement(parse)) {
			PlannedStmt *duckdbPlan = DuckdbPlanNode(parse, cursor_options, bound_params);
			if (duckdbPlan) {
				return duckdbPlan;
			}
		}

		if (NeedsDuckdbExecution(parse)) {
			if (!IsAllowedStatement(parse)) {
				elog(ERROR, "(PGDuckDB/DuckdbPlannerHook) Only SELECT statements involving DuckDB are supported.");
			}
			PlannedStmt *duckdbPlan = DuckdbPlanNode(parse, cursor_options, bound_params);
			if (duckdbPlan) {
				return duckdbPlan;
			}
		}
	}

	if (prev_planner_hook) {
		return prev_planner_hook(parse, query_string, cursor_options, bound_params);
	} else {
		return standard_planner(parse, query_string, cursor_options, bound_params);
	}
}

static void
DuckdbUtilityHook(PlannedStmt *pstmt, const char *query_string, bool read_only_tree, ProcessUtilityContext context,
                  ParamListInfo params, struct QueryEnvironment *query_env, DestReceiver *dest, QueryCompletion *qc) {
	Node *parsetree = pstmt->utilityStmt;
	if (duckdb_execution && pgduckdb::IsExtensionRegistered() && IsA(parsetree, CopyStmt)) {
		uint64 processed;
		if (DuckdbCopy(pstmt, query_string, query_env, &processed)) {
			if (qc) {
				SetQueryCompletion(qc, CMDTAG_COPY, processed);
			}
			return;
		}
	}

	if (prev_process_utility_hook) {
		(*prev_process_utility_hook)(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
	} else {
		standard_ProcessUtility(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
	}
}

extern "C" {
#include "nodes/print.h"
}
void
DuckdbExplainOneQueryHook(Query *query, int cursorOptions, IntoClause *into, ExplainState *es, const char *queryString,
                          ParamListInfo params, QueryEnvironment *queryEnv) {
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
