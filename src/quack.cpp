extern "C" {
#include "postgres.h"
#include "utils/guc.h"
}

#include "quack/quack.h"
#include "quack/quack_node.hpp"
#include "quack/quack_utils.hpp"

static void quack_init_guc(void);

bool quack_execution = true;
int quack_max_threads_per_query = 1;

extern "C" {
PG_MODULE_MAGIC;

void
_PG_init(void) {
	quack_init_guc();
	quack_init_hooks();
	quack_init_node();
}

}

/* clang-format off */
static void
quack_init_guc(void) {

    DefineCustomBoolVariable("quack.execution",
                             gettext_noop("Is DuckDB query execution enabled."),
                             NULL,
                             &quack_execution,
                             true,
                             PGC_USERSET,
                             0,
                             NULL, 
                             NULL, 
                             NULL);
    DefineCustomIntVariable("quack.max_threads_per_query",
                            gettext_noop("DuckDB max no. threads per query."),
                            NULL,
                            &quack_max_threads_per_query,
                            1,
                            1,
                            64,
                            PGC_USERSET,
                            0,
                            NULL,
                            NULL,
                            NULL);
}