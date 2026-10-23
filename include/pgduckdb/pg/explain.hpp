#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb::pg {
void ExplainPropertyText(const char *qlabel, const char *value, ExplainState *es);

bool IsExplainFormatJson(ExplainState *es);

bool IsExplainAnalyze(ExplainState *es);
} // namespace pgduckdb::pg
