#include "pgduckdb/bgw/utils.hpp"

#include <sys/errno.h>
#include <stdio.h>

extern "C" {
#include "postgres.h"
}

namespace pgduckdb {
const char *
GetStrError() {
	return strerror(errno);
}

int
GetErrno() {
	return errno;
}

void
ELogError(const char *comp, const char *func_name) {
	elog(ERROR, "[%s] - %s - %s", comp, func_name, GetStrError());
}

const char *PGDUCKDB_SOCKET_PATH = "/tmp/pgduckdb.sock";
} // namespace pgduckdb