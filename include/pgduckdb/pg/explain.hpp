#include "pgduckdb/pg/declarations.hpp"
#include "duckdb/main/profiler/profiler_print_format.hpp"

namespace pgduckdb::pg {
void ExplainPropertyText(const char *qlabel, const char *value, ExplainState *es);

duckdb::ProfilerPrintFormat DuckdbExplainFormat(ExplainState *es);

bool IsExplainAnalyze(ExplainState *es);
} // namespace pgduckdb::pg
