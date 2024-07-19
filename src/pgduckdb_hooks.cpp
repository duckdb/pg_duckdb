extern "C" {
#include "postgres.h"

#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "tcop/utility.h"
#include "tcop/pquery.h"
#include "utils/rel.h"
#include "optimizer/optimizer.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_ddl.hpp"
#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/pgduckdb_table_am.hpp"
#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/vendor/pg_list.hpp"

static planner_hook_type prev_planner_hook = NULL;
static ProcessUtility_hook_type prev_process_utility_hook = NULL;

static bool
IsDuckdbExtensionRegistered() {
	return get_extension_oid("pg_duckdb", true) != InvalidOid;
}

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
contains_duckdb_table(List *rte_list) {
	foreach_node(RangeTblEntry, rte, rte_list) {
		if (rte->rtekind == RTE_SUBQUERY) {
			/* Check Subquery rtable list if any table is from PG catalog */
			if (contains_duckdb_table(rte->subquery->rtable)) {
				return true;
			}
		}
		if (rte->relid) {
			auto rel = RelationIdGetRelation(rte->relid);
			if (is_duckdb_table_am(rel->rd_tableam)) {
				RelationClose(rel);
				return true;
			}
			RelationClose(rel);
		}
	}
	return false;
}

static bool
needs_duckdb_execution(Query *parse) {
	if (parse->commandType == CMD_UTILITY) {
		return false;
	}
	return contains_duckdb_table(parse->rtable);
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
	if (IsDuckdbExtensionRegistered()) {
		if (duckdb_execution && IsAllowedStatement() && parse->rtable && !IsCatalogTable(parse->rtable) &&
		    parse->commandType == CMD_SELECT) {
			PlannedStmt *duckdbPlan = DuckdbPlanNode(parse, query_string, cursor_options, bound_params);
			if (duckdbPlan) {
				return duckdbPlan;
			}
		}

		if (needs_duckdb_execution(parse)) {
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
	if (duckdb_execution && IsDuckdbExtensionRegistered() && IsA(parsetree, CopyStmt)) {
		uint64 processed;
		if (DuckdbCopy(pstmt, query_string, query_env, &processed)) {
			if (qc) {
				SetQueryCompletion(qc, CMDTAG_COPY, processed);
			}
			return;
		}
	}

	if (IsDuckdbExtensionRegistered()) {
		duckdb_handle_ddl(parsetree, query_string);
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
