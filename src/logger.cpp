#include "pgduckdb/logger.hpp"

extern "C" {
#include "postgres.h"
}

#include <utility>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wbuiltin-macro-redefined"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wbuiltin-macro-redefined"

// Redefine them to match the pgduckdb logger so it uses the provided filename and lineno
#define __FILE__ filename
#define __LINE__ lineno

#pragma GCC diagnostic pop
#pragma clang diagnostic pop

namespace pgduckdb {

void pgduckdb_logger(int log_level, const char *filename, int lineno, const char *pattern, ...) {
	va_list args;
	va_start(args, pattern);
	elog(log_level, pattern, args);
	va_end(args);
}

} // namespace pgduckdb
