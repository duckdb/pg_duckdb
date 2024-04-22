extern "C" {
#include "postgres.h"
#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "utils/rel.h"
}

#include "quack/quack.h"
#include "quack/quack_select.h"

static ExecutorRun_hook_type PrevExecutorRunHook = NULL;

static bool
is_quack_extension_registered() {
	return get_extension_oid("quack", true) != InvalidOid;
}

static bool
is_catalog_table(List *tables) {
	ListCell *lc;
	foreach (lc, tables) {
		RangeTblEntry *table = (RangeTblEntry *)lfirst(lc);
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

static void
quack_executor_run(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once) {
	if (is_quack_extension_registered() && !is_catalog_table(queryDesc->plannedstmt->rtable) &&
	    queryDesc->operation == CMD_SELECT) {
		if (quack_execute_select(queryDesc, direction, count)) {
			return;
		}
	}

	elog(DEBUG3, "quack_executor_run: Failing back to PG execution");

	if (PrevExecutorRunHook) {
		PrevExecutorRunHook(queryDesc, direction, count, execute_once);
	}
}

void
quack_init_hooks(void) {
	PrevExecutorRunHook = ExecutorRun_hook ? ExecutorRun_hook : standard_ExecutorRun;
	ExecutorRun_hook = quack_executor_run;
}