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
#include "pgduckdb/vendor/pg_list.hpp"

static planner_hook_type prev_planner_hook = NULL;
static ProcessUtility_hook_type prev_process_utility_hook = NULL;

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
IsAllowedStatement() {
	/* For `SELECT ..` ActivePortal doesn't exist */
	if (!ActivePortal)
		return true;
	/* `EXPLAIN ...` should be allowed */
	if (ActivePortal->commandTag == CMDTAG_EXPLAIN)
		return true;
	return false;
}

static PlannedStmt *
DuckdbPlannerHook(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params) {
	if (pgduckdb::IsExtensionRegistered()) {
		if (duckdb_execution && IsAllowedStatement() && parse->rtable && !IsCatalogTable(parse->rtable) &&
		    parse->commandType == CMD_SELECT) {
			PlannedStmt *duckdbPlan = DuckdbPlanNode(parse, query_string, cursor_options, bound_params);
			if (duckdbPlan) {
				return duckdbPlan;
			}
		}

		if (NeedsDuckdbExecution(parse)) {
			if (!IsAllowedStatement()) {
				elog(ERROR, "only SELECT statements involving DuckDB are supported");
			}
			PlannedStmt *duckdbPlan = DuckdbPlanNode(parse, query_string, cursor_options, bound_params);
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

void
DuckdbInitHooks(void) {
	prev_planner_hook = planner_hook;
	planner_hook = DuckdbPlannerHook;

	prev_process_utility_hook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = DuckdbUtilityHook;
}
