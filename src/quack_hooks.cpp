extern "C" {
#include "postgres.h"
#include "commands/extension.h"
}

#include "quack/quack.h"
#include "quack/quack_select.h"

static ExecutorRun_hook_type PrevExecutorRunHook = NULL;

static bool
is_quack_extension_registered() {
	return get_extension_oid("quack", true) != InvalidOid;
}

static void
quack_executor_run(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once) {
	if (is_quack_extension_registered() && queryDesc->operation == CMD_SELECT) {
		if (quack_execute_select(queryDesc, direction, count)) {
			return;
		}
	}

	if (PrevExecutorRunHook) {
		PrevExecutorRunHook(queryDesc, direction, count, execute_once);
	}
}

void
quack_init_hooks(void) {
	PrevExecutorRunHook = ExecutorRun_hook ? ExecutorRun_hook : standard_ExecutorRun;
	ExecutorRun_hook = quack_executor_run;
}