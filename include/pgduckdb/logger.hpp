#pragma once

namespace pgduckdb {

/* PG Error level codes */
#define DEBUG5		10
#define DEBUG4		11
#define DEBUG3		12
#define DEBUG2		13
#define DEBUG1		14
#define LOG			15
#define INFO		17
#define NOTICE		18
#define WARNING		19
#define PGWARNING	19
#define WARNING_CLIENT_ONLY	20
#define ERROR		21
#define PGERROR		21
#define FATAL		22
#define PANIC		23

void pgduckdb_logger(int log_level, const char *filename, int lineno, const char *pattern, ...);

#define pdlog(level, ...) \
	pgduckdb_logger(level, __FILE__, __LINE__, __VA_ARGS__);

} // namespace pgduckdb
