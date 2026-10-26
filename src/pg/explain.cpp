#include "pgduckdb/pg/explain.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"

#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#else
#include "commands/explain.h"
#endif
}

namespace pgduckdb::pg {
void
ExplainPropertyText(const char *qlabel, const char *value, ExplainState *es) {
	PostgresFunctionGuard(::ExplainPropertyText, qlabel, value, es);
}

duckdb::ProfilerPrintFormat
DuckdbExplainFormat(ExplainState *es) {
	if (es->format == EXPLAIN_FORMAT_JSON)
		return duckdb::ProfilerPrintFormat::JSON();

	return duckdb::ProfilerPrintFormat::Default();
}

bool
IsExplainAnalyze(ExplainState *es) {
	return es->analyze;
}
} // namespace pgduckdb::pg
