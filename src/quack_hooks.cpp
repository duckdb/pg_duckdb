extern "C" {
#include "postgres.h"
#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "utils/rel.h"
}

#include "quack/quack.h"
#include "quack/quack_planner.hpp"

static planner_hook_type PrevPlannerHook = NULL;

static bool
is_quack_extension_registered() {
	return get_extension_oid("quack", true) != InvalidOid;
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
			};
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

static PlannedStmt *
quack_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams) {
	if (quack_execution && is_quack_extension_registered() && !is_catalog_table(parse->rtable) &&
	    parse->commandType == CMD_SELECT) {
		PlannedStmt *quackPlan = quack_plan_node(parse, query_string, cursorOptions, boundParams);
		if (quackPlan) {
			return quackPlan;
		}
	}

	if (PrevPlannerHook) {
		return PrevPlannerHook(parse, query_string, cursorOptions, boundParams);
	} else {
		return standard_planner(parse, query_string, cursorOptions, boundParams);
	}
}

void
quack_init_hooks(void) {
	PrevPlannerHook = planner_hook;
	planner_hook = quack_planner;
}