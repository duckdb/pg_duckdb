#include "postgres.h"

#include "utils/guc.h"

#include "quack/quack.h"

PG_MODULE_MAGIC;

static void quack_init_guc(void);

void
_PG_init(void) {
	quack_init_guc();
	quack_init_hooks();
}

/* clang-format off */
static void
quack_init_guc(void) {

}