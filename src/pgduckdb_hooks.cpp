extern "C" {
#include "postgres.h"
#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "nodes/nodes.h"
#include "tcop/utility.h"
#include "tcop/pquery.h"
#include "utils/rel.h"
#include "optimizer/optimizer.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/utility/copy.hpp"

static planner_hook_type PrevPlannerHook = NULL;
static ProcessUtility_hook_type PrevProcessUtilityHook = NULL;

static bool
is_duckdb_extension_registered() {
	return get_extension_oid("pg_duckdb", true) != InvalidOid;
}

static bool
is_catalog_table(List *tables) {
	ListCell *lc;
	foreach (lc, tables) {
		RangeTblEntry *table = (RangeTblEntry *)lfirst(lc);
		if (table->rtekind == RTE_SUBQUERY) {
			/* Check Subquery rtable list if any table is from PG catalog */
			if (is_catalog_table(table->subquery->rtable)) {
				return true;
			}
		}
		if (table->relid) {
			auto rel = RelationIdGetRelation(table->relid);
			auto namespaceOid = RelationGetNamespace(rel);
			if (namespaceOid == PG_CATALOG_NAMESPACE || namespaceOid == PG_TOAST_NAMESPACE) {
				RelationClose(rel);
				return true;
			}
			RelationClose(rel);
		}
	}
	return false;
}

static bool
is_allowed_statement() {
	/* For `SELECT ..` ActivePortal doesn't exist */
	if (!ActivePortal)
		return true;
	/* `EXPLAIN ...` should be allowed */
	if (ActivePortal->commandTag == CMDTAG_EXPLAIN)
		return true;
	return false;
}

static PlannedStmt *
duckdb_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams) {
	if (duckdb_execution && is_allowed_statement() && is_duckdb_extension_registered() && parse->rtable &&
	    !is_catalog_table(parse->rtable) && parse->commandType == CMD_SELECT) {
		PlannedStmt *duckdbPlan = duckdb_plan_node(parse, query_string, cursorOptions, boundParams);
		if (duckdbPlan) {
			return duckdbPlan;
		}
	}

	if (PrevPlannerHook) {
		return PrevPlannerHook(parse, query_string, cursorOptions, boundParams);
	} else {
		return standard_planner(parse, query_string, cursorOptions, boundParams);
	}
}

static void
duckdb_utility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree, ProcessUtilityContext context,
               ParamListInfo params, struct QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc) {
	Node *parsetree = pstmt->utilityStmt;
	if (duckdb_execution && is_duckdb_extension_registered() && IsA(parsetree, CopyStmt)) {
		uint64 processed;
		if (duckdb_copy(pstmt, queryString, queryEnv, &processed)) {
			if (qc) {
				SetQueryCompletion(qc, CMDTAG_COPY, processed);
			}
			return;
		}
	}

	if (PrevProcessUtilityHook) {
		(*PrevProcessUtilityHook)(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
	} else {
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
	}
}

void
duckdb_init_hooks(void) {
	PrevPlannerHook = planner_hook;
	planner_hook = duckdb_planner;

	PrevProcessUtilityHook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = duckdb_utility;
}
