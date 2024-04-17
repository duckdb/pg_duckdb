extern "C" {
#include "postgres.h"

#include "utils/guc.h"
}

#include "quack/quack.h"

static void quack_init_guc(void);

extern "C" {
PG_MODULE_MAGIC;

void
_PG_init(void) {
	quack_init_guc();
	quack_init_hooks();
}
}

/* clang-format off */
static void
quack_init_guc(void) {

}