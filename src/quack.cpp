#include "quack/quack.hpp"
#include "duckdb/main/connection.hpp"

extern "C" {

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

#include "postgres.h"

#include "miscadmin.h"

#include "access/amapi.h"
#include "commands/vacuum.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

char *quack_data_dir = NULL;

static bool quack_check_data_directory(const char *dataDirectory);

static void
quack_data_directory_assign_hook(const char *newval, void *extra) {
	if (!quack_check_data_directory(newval)) {
		if (mkdir(newval, S_IRWXU | S_IRWXG | S_IRWXO) == -1) {
			int error = errno;
			elog(ERROR, "Creating quack.data_dir failed with reason `%s`\n", strerror(error));
		}
		elog(INFO, "Created %s as `quack.data_dir`", newval);
	};
}

void
_PG_init(void) {
	StringInfo quack_default_data_dir = makeStringInfo();
	appendStringInfo(quack_default_data_dir, "%s/quack/", DataDir);

	elog(INFO, "Initializing quack extension");
	DefineCustomStringVariable("quack.data_dir", gettext_noop("Quack storage data directory."), NULL, &quack_data_dir,
	                           quack_default_data_dir->data, PGC_USERSET, GUC_IS_NAME, NULL,
	                           quack_data_directory_assign_hook, NULL);

	quack_init_tableam();
	quack_init_hooks();
}

bool
quack_check_data_directory(const char *dataDirectory) {
	struct stat info;

	if (lstat(dataDirectory, &info) != 0) {
		if (errno == ENOENT) {
			elog(WARNING, "Directory `%s` doesn't exists.", dataDirectory);
			return false;
		} else if (errno == EACCES) {
			elog(ERROR, "Can't access `%s` directory.", dataDirectory);
		} else {
			elog(ERROR, "Other error when reading `%s`.", dataDirectory);
		}
	}

	if (!S_ISDIR(info.st_mode)) {
		elog(WARNING, "`%s` is not directory.", dataDirectory);
	}

	if (access(dataDirectory, R_OK | W_OK)) {
		elog(ERROR, "Directory `%s` permission problem.", dataDirectory);
	}

	return true;
}
}
