
extern "C" {
#include "postgres.h"
#include "lib/stringinfo.h"
#include "executor/spi.h"
}

#include "pgduckdb/pg/explain.hpp"

namespace pgduckdb {
const char *
ExplainPGQuery(const char *query_str) {
	StringInfo explain_query_si = makeStringInfo();
	// Must be created before SPI_connect to be in the right context
	StringInfo explain_result_si = makeStringInfo();
	appendStringInfo(explain_query_si, "EXPLAIN (%s)", query_str);

	SPI_connect();
	int ret = SPI_exec(explain_query_si->data, 0);
	if (ret != SPI_OK_UTILITY) {
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
	}

	for (uint64_t proc = 0; proc < SPI_processed; ++proc) {
		HeapTuple tuple = SPI_tuptable->vals[proc];
		char *row = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
		appendStringInfo(explain_result_si, "%s\n", row);
	}

	SPI_finish();
	return explain_result_si->data;
}
} // namespace pgduckdb
