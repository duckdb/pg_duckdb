extern "C" {
#include "postgres.h"
#include "utils/guc.h"
}

#include "quack/quack.h"
#include "quack/quack_node.hpp"
#include "quack/quack_utils.hpp"

static void quack_init_guc(void);

int quack_max_threads_per_query = 1;
char *quack_secret = nullptr;

extern "C" {
PG_MODULE_MAGIC;

void
_PG_init(void) {
	quack_init_guc();
	quack_init_hooks();
	quack_init_node();
}

}

static bool
quack_cloud_secret_check_hooks(char **newval, void **extra, GucSource source) {

	std::vector<std::string> tokens = quack::tokenizeString(*newval, '#');

	if (tokens.size() == 0) {
		return true;
	}

	if (tokens.size() != 4) {
		elog(WARNING, "Incorrect quack.cloud_secret format.");
		return false;
	}

	if (tokens[0].compare("S3")) {
		elog(WARNING, "quack.cloud_secret supports only S3.");
		return false;
	}

	return true;
}

/* clang-format off */
static void
quack_init_guc(void) {

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

    DefineCustomStringVariable("quack.cloud_secret",
                               "Quack (duckdb) cloud secret GUC. Format is TYPE#ID#SECRET#REGION",
                               NULL,
                               &quack_secret,
                               "",
                               PGC_USERSET,
                               0,
                               &quack_cloud_secret_check_hooks,
                               NULL,
                               NULL);
}